"use strict";

const Database = use("Database");
const BranchDatabase = use("App/Services/BranchDatabase");

class OneDcSupplierController {
  constructor() {
    this.CONCURRENCY_LIMIT = 10;
    this.chunkSize = 5000;
  }

  async run() {
    console.log("Starting Parallel Auto Update");
    console.log("================================");

    const branches = await Database.table("branch_connection_details");

    await this.processInParallel(branches, this.CONCURRENCY_LIMIT);

    console.log("================================");
    console.log("Auto Update Finished");
  }

  async processInParallel(branches, limit) {
    let index = 0;

    const workers = new Array(limit).fill(null).map(async () => {
      while (index < branches.length) {
        const currentIndex = index++;
        const branch = branches[currentIndex];

        if (!branch) break;

        await this.processBranch(branch);
      }
    });

    await Promise.all(workers);
  }

  async processBranch(branch) {
    const connectionName = branch.branch_name;

    console.log(`Processing ${connectionName}`);

    try {
      const db = BranchDatabase.connect(branch);

      const isHO = branch.branch_name === "srsho";

      // ✅ HEALTH CHECK
      await db.raw("SELECT 1");

      // ✅ TRANSACTION
      await db.transaction(async (trx) => {
        let page = 0;
        let hasMore = true;

        const mainCodes = new Set();

        while (hasMore) {
          const suppliers = await Database.select(
            "vendorcode",
            "inactive",
            "rs_status",
          )
            .from("one_dc_supplier")
            .limit(this.chunkSize)
            .offset(page * this.chunkSize);

          if (!suppliers.length) {
            hasMore = false;
            break;
          }

          await this.syncDatabase(trx, suppliers, mainCodes, isHO);

          page++;
        }

        // Clean removed suppliers
        await this.deleteRemovedSuppliers(trx, mainCodes, isHO);

        console.log("Supplier sync completed successfully");
      });

      console.log(`✔ Updated ${branch.branch_name}`);
    } catch (error) {
      console.error(`✘ Failed ${branch.branch_name}`);
      console.error(`Reason: ${error.code || error.message}`);
      console.log(error);
    } finally {
      await BranchDatabase.close(connectionName);
    }
  }

  computeValues(supplier, isHO) {
    let rs_disable = 0;
    let bo_disable = 0;
    let to_delete = 0;
    let insert = false;

    if (supplier.inactive === 1) {
      // inactive = 1 → always insert (both HO & Branch)
      insert = true;
      rs_disable = 1;
      bo_disable = 0;
      to_delete = 1;
    } else if (supplier.inactive === 0) {
      if (supplier.rs_status === 1) {
        // RS Status 1 → branches only, skip HO
        insert = !isHO; // true for branches, false for HO
        rs_disable = 1;
        bo_disable = 0;
        to_delete = 1;
      } else if (supplier.rs_status === 2) {
        // RS Status 2 → insert in all
        insert = true;
        rs_disable = 1;
        bo_disable = 0;
        to_delete = 1;
      }
    }

    return { insert, rs_disable, bo_disable, to_delete };
  }

  async getBranchSupplierMap(trx) {
    const exists = await trx.raw("SHOW TABLES LIKE '0_rms_exluded_supplier'");
    if (!exists[0].length) return new Set();

    const rows = await trx
      .table("0_rms_exluded_supplier")
      .select("supplier_code");
    return new Set(rows.map((r) => r.supplier_code));
  }

  async syncDatabase(trx, suppliers, mainCodes, isHO = false) {
    const branchSet = await this.getBranchSupplierMap(trx);

    const inserts = [];
    const updates = [];

    for (const s of suppliers) {
      mainCodes.add(s.vendorcode);

      const values = this.computeValues(s, isHO);

      // Skip if not allowed to insert
      if (!values.insert) continue;

      if (!branchSet.has(s.vendorcode)) {
        inserts.push({
          supplier_code: s.vendorcode,
          rs_disable: values.rs_disable,
          bo_disable: values.bo_disable,
          to_delete: values.to_delete,
        });
      } else {
        updates.push({
          supplier_code: s.vendorcode,
          rs_disable: values.rs_disable,
          bo_disable: values.bo_disable,
          to_delete: values.to_delete,
        });
      }
    }

    // Bulk Insert
    if (inserts.length) {
      await trx.table("0_rms_exluded_supplier").insert(inserts);
    }

    // Bulk Update using CASE WHEN
    if (updates.length) {
      const codes = updates.map((u) => `'${u.supplier_code}'`).join(",");

      const rsCase = updates
        .map((u) => `WHEN '${u.supplier_code}' THEN ${u.rs_disable}`)
        .join(" ");

      const boCase = updates
        .map((u) => `WHEN '${u.supplier_code}' THEN ${u.bo_disable}`)
        .join(" ");

      const tdCase = updates
        .map((u) => `WHEN '${u.supplier_code}' THEN ${u.to_delete}`)
        .join(" ");

      await trx.raw(`
        UPDATE \`0_rms_exluded_supplier\`
        SET
          rs_disable = CASE supplier_code ${rsCase} END,
          bo_disable = CASE supplier_code ${boCase} END,
          to_delete = CASE supplier_code ${tdCase} END
        WHERE supplier_code IN (${codes})
      `);
    }
  }

  async deleteRemovedSuppliers(trx, mainCodes, isHO) {
    const exists = await trx.raw("SHOW TABLES LIKE '0_rms_exluded_supplier'");
    if (!exists[0].length) return;

    const rows = await trx
      .table("0_rms_exluded_supplier")
      .select("supplier_code")
      .where("to_delete", 1);

    const toDelete = [];

    for (const r of rows) {
      // 1️⃣ Delete if no longer exists in main server
      if (!mainCodes.has(r.supplier_code)) {
        toDelete.push(r.supplier_code);
        continue;
      }

      // 2️⃣ Additional HO rule
      if (isHO) {
        const supplier = await Database.select("inactive", "rs_status")
          .from("one_dc_supplier")
          .where("vendorcode", r.supplier_code)
          .first();

        if (!supplier) {
          toDelete.push(r.supplier_code);
          continue;
        }

        // inactive = 0 AND rs_status = 1 → NOT allowed in HO
        if (supplier.inactive == 0 && supplier.rs_status == 1) {
          toDelete.push(r.supplier_code);
        }
      }
    }

    const chunkDelete = 5000;

    for (let i = 0; i < toDelete.length; i += chunkDelete) {
      const slice = toDelete.slice(i, i + chunkDelete);

      await trx
        .table("0_rms_exluded_supplier")
        .whereIn("supplier_code", slice)
        .delete();
    }
  }
}

module.exports = new OneDcSupplierController();
