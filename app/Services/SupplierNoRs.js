"use strict";

const Database = use("Database");
const BranchDatabase = use("App/Services/BranchDatabase");

class SupplierNoRs {
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

  // ✅ UPDATED: supports pause_type (1 = HO, 2 = ALL)
  async getPausedSuppliers() {
    // 🔥 delete expired first
    await Database.table("supplier_exclusion_pause")
      .where("resume_at", "<=", Database.raw("NOW()"))
      .delete();

    const rows = await Database.table("supplier_exclusion_pause").select(
      "supplier_code",
      "pause_type",
    );

    const map = new Map();

    for (const r of rows) {
      map.set(r.supplier_code, r.pause_type);
    }

    return map;
  }

  async getCheckpoint(branchName) {
    const row = await Database.table("bo_supplier_jobs")
      .where({
        job_name: "SupplierNoRS",
        branch_name: branchName,
      })
      .first();

    return row ? row.last_page : 0;
  }

  async updateCheckpoint(branchName, page) {
    const exists = await Database.table("bo_supplier_jobs")
      .where("job_name", "SupplierNoRS")
      .where("branch_name", branchName)
      .first();

    if (exists) {
      await Database.table("bo_supplier_jobs")
        .where("job_name", "SupplierNoRS")
        .where("branch_name", branchName)
        .update({
          last_page: page,
        });
    } else {
      await Database.table("bo_supplier_jobs").insert({
        job_name: "SupplierNoRS",
        branch_name: branchName,
        last_page: page,
      });
    }
  }

  async processInParallel(branches, branches_aria, limit) {
    const queue = branches.map((branch, i) => ({
      branch,
      branch_aria: branches_aria[i],
    }));

    const workers = new Array(limit).fill(null).map(async () => {
      while (queue.length) {
        const item = queue.shift(); // ✅ SAFE (single-threaded)

        if (!item) break;

        await this.processBranch(item.branch, item.branch_aria);
      }
    });

    await Promise.all(workers);
  }

  async processBranch(branch, branch_aria) {
    const connectionName = branch.branch_name;
    branch_aria.branch_name = branch_aria.branch_name + "2";

    console.log(`[SupplierNoRS] Processing ${connectionName}`);

    try {
      const db = BranchDatabase.connect(branch);
      const aria_db = BranchDatabase.connect(branch_aria);
      const isHO = branch.branch_name === "srsho";

      // ✅ HEALTH CHECK
      await db.raw("SELECT 1");

      const pausedMap = await this.getPausedSuppliers();

      // ✅ GET LAST PAGE
      let page = await this.getCheckpoint(connectionName);
      let hasMore = true;

      await db.transaction(async (trx) => {
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

            // ✅ RESET CHECKPOINT WHEN DONE
            await this.updateCheckpoint(connectionName, 0);
            break;
          }

          await this.syncDatabase(
            trx,
            suppliers,
            existingSuppliers,
            pausedMap,
            isHO,
          );

          page++;

          // ✅ SAVE PROGRESS AFTER EACH BATCH
          await this.updateCheckpoint(connectionName, page);
        }
      });

      console.log(`[SupplierNoRS] Updated ${branch.branch_name}`);
    } catch (error) {
      console.error(`[SupplierNoRS] Failed ${branch.branch_name}`);
      console.error(`Reason: ${error.code || error.message}`);
    } finally {
      await BranchDatabase.close(connectionName);
      await BranchDatabase.close(connectionName + "2");
    }
  }

  computeValues(supplier) {
    const code = supplier.vendorcode?.toUpperCase();

    // ❌ Do NOT insert this specific code
    if (code === "ANGATK038") {
      return {
        insert: false,
        rs_disable: 1,
        bo_disable: 0,
        to_delete: 0,
      };
    }

    return {
      insert: true,
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

  async syncDatabase(trx, suppliers, existingSuppliers, pausedMap, isHO) {
    if (!suppliers.length) return;

    const inserts = [];

    for (const s of suppliers) {
      const pauseType = pausedMap.get(code);

      // ❌ APPLY PAUSE LOGIC
      if (pauseType === 2) continue; // pause all
      if (pauseType === 1 && isHO) continue; // pause HO only

      // Skip if already processed in-memory
      if (existingSuppliers.has(s.supp_ref)) continue;

      const values = this.computeValues(s);

      inserts.push([
        s.supp_ref,
        values.rs_disable,
        values.bo_disable,
        values.to_delete,
      ]);

      existingSuppliers.add(s.supp_ref);
    }

    if (!inserts.length) return;

    await trx.raw(
      `
    INSERT INTO 0_rms_exluded_supplier
      (supplier_code, rs_disable, bo_disable, to_delete)
    VALUES ${inserts.map(() => "(?, ?, ?, ?)").join(",")}
    ON DUPLICATE KEY UPDATE
      rs_disable = VALUES(rs_disable),
      bo_disable = VALUES(bo_disable),
      to_delete = VALUES(to_delete)
  `,
      inserts.flat(),
    );
  }
}

module.exports = new SupplierNoRs();
