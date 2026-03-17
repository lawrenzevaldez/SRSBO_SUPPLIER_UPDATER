"use strict";

const Database = use("Database");
const BranchDatabase = use("App/Services/BranchDatabase");

class SupplierNoRsController {
  constructor() {
    this.CONCURRENCY_LIMIT = 10;
    this.chunkSize = 5000;
  }

  async run() {
    console.log("Starting Parallel Auto Update");
    console.log("================================");

    const branches = await Database.select(
      "branch_name",
      "branch_dbuser",
      "branch_dbpass",
      "branch_database",
      "branch_ip",
    ).table("branch_connection_details");

    const branches_aria = await Database.select(
      "branch_name",
      "branch_dbuser",
      "branch_dbpass",
      "aria_db as branch_database",
      "aria_ip as branch_ip",
    ).table("branch_connection_details");

    await this.processInParallel(
      branches,
      branches_aria,
      this.CONCURRENCY_LIMIT,
    );

    console.log("================================");
    console.log("Auto Update Finished");
  }

  async processInParallel(branches, branches_aria, limit) {
    let index = 0;

    const workers = new Array(limit).fill(null).map(async () => {
      while (index < branches.length) {
        const currentIndex = index++;
        const branch = branches[currentIndex];
        const branch_aria = branches_aria[currentIndex];

        if (!branch || !branch_aria) break;

        await this.processBranch(branch, branch_aria);
      }
    });

    await Promise.all(workers);
  }

  async processBranch(branch, branch_aria) {
    const connectionName = branch.branch_name;
    branch_aria.branch_name = branch_aria.branch_name + "2";

    console.log(`Processing ${connectionName}`);

    try {
      const db = BranchDatabase.connect(branch);
      const aria_db = BranchDatabase.connect(branch_aria);
      const isHO = branch.branch_name === "srsho";

      // ✅ HEALTH CHECK
      await db.raw("SELECT 1");

      await db.transaction(async (trx) => {
        let page = 0;
        let hasMore = true;

        // Get existing suppliers once
        const existingSuppliers = await this.getBranchSupplierMap(trx);

        while (hasMore) {
          const suppliers = await aria_db
            .table("0_suppliers as a")
            .leftJoin("0_grn_batch as b", "a.supplier_id", "b.supplier_id")
            .select("a.supplier_id", "a.supp_name", "a.supp_ref")
            .max("b.delivery_date as last_delivery_date")
            .groupBy("a.supplier_id")
            .havingRaw(
              "last_delivery_date < DATE_SUB(CURDATE(), INTERVAL 3 MONTH)",
            )
            .orderBy("last_delivery_date", "asc")
            .limit(this.chunkSize)
            .offset(page * this.chunkSize);

          if (!suppliers.length) {
            hasMore = false;
            break;
          }

          await this.syncDatabase(trx, suppliers, existingSuppliers);

          page++;
        }

        console.log("Supplier sync completed successfully");
      });

      console.log(`✔ Updated ${branch.branch_name}`);
    } catch (error) {
      console.error(`✘ Failed ${branch.branch_name}`);
      console.error(`Reason: ${error.code || error.message}`);
    } finally {
      await BranchDatabase.close(connectionName);
      await BranchDatabase.close(connectionName + "2");
    }
  }

  computeValues(supplier) {
    return {
      insert: true, // always insert if not exists
      rs_disable: 1,
      bo_disable: 0,
      to_delete: 0,
    };
  }

  async getBranchSupplierMap(trx) {
    const exists = await trx.raw("SHOW TABLES LIKE '0_rms_exluded_supplier'");
    if (!exists[0].length) return new Set();

    const rows = await trx
      .table("0_rms_exluded_supplier")
      .select("supplier_code");
    return new Set(rows.map((r) => r.supplier_code));
  }

  async syncDatabase(trx, suppliers, existingSuppliers) {
    const inserts = [];

    for (const s of suppliers) {
      // Skip if supplier already exists
      if (existingSuppliers.has(s.supp_ref)) continue;

      const values = this.computeValues(s);

      inserts.push({
        supplier_code: s.supp_ref,
        rs_disable: values.rs_disable,
        bo_disable: values.bo_disable,
        to_delete: values.to_delete,
      });

      // Add to set to prevent duplicate inserts in the same batch
      existingSuppliers.add(s.supp_ref);
    }

    if (inserts.length) {
      console.log(`Inserting ${inserts.length} suppliers`);
      await trx.table("0_rms_exluded_supplier").insert(inserts);
    }
  }
}

module.exports = new SupplierNoRsController();
