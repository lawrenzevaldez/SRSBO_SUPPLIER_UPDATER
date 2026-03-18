"use strict";

const Database = use("Database");
const BranchDatabase = use("App/Services/BranchDatabase");

class OneDcSupplier {
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

  async getCheckpoint(branchName) {
    const row = await Database.table("bo_supplier_jobs")
      .where({
        job_name: "OneDcSupplier",
        branch_name: branchName,
      })
      .first();

    return row ? row.last_page : 0;
  }

  async updateCheckpoint(branchName, page) {
    const exists = await Database.table("bo_supplier_jobs")
      .where("job_name", "OneDcSupplier")
      .where("branch_name", branchName)
      .first();

    if (exists) {
      await Database.table("bo_supplier_jobs")
        .where("job_name", "OneDcSupplier")
        .where("branch_name", branchName)
        .update({
          last_page: page,
        });
    } else {
      await Database.table("bo_supplier_jobs").insert({
        job_name: "OneDcSupplier",
        branch_name: branchName,
        last_page: page,
      });
    }
  }

  async processInParallel(branches, limit) {
    const queue = branches.map((branch, i) => ({
      branch,
    }));

    const workers = new Array(limit).fill(null).map(async () => {
      while (queue.length) {
        const item = queue.shift(); // ✅ SAFE (single-threaded)

        if (!item) break;

        await this.processBranch(item.branch);
      }
    });

    await Promise.all(workers);
  }

  async processBranch(branch) {
    const connectionName = branch.branch_name;

    console.log(`[OneDcSupplier] Processing ${connectionName}`);

    try {
      const db = BranchDatabase.connect(branch);

      const isHO = branch.branch_name === "srsho";

      // ✅ HEALTH CHECK
      await db.raw("SELECT 1");

      let page = await this.getCheckpoint(connectionName);
      let hasMore = true;

      // ✅ TRANSACTION
      await db.transaction(async (trx) => {
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

            // ✅ RESET CHECKPOINT WHEN DONE
            await this.updateCheckpoint(connectionName, 0);
            break;
          }

          await this.syncDatabase(trx, suppliers, mainCodes, isHO);

          page++;

          // ✅ SAVE PROGRESS AFTER EACH BATCH
          await this.updateCheckpoint(connectionName, page);
        }

        // Clean removed suppliers
        await this.deleteRemovedSuppliers(trx, mainCodes, isHO);
      });

      console.log(`[OneDcSupplier] Updated ${branch.branch_name}`);
    } catch (error) {
      console.error(`[OneDcSupplier] Failed ${branch.branch_name}`);
      // console.error(`Reason: ${error.code || error.message}`);
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

    const code = supplier.vendorcode?.toUpperCase();

    // ❌ HARD EXCLUDE
    if (code === "ANGATK038") {
      return { insert: false, rs_disable: 0, bo_disable: 0, to_delete: 0 };
    }

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

    // ✅ Force to_delete = 0 list
    const FORCE_KEEP = new Set([
      "3MKENT01",
      "AJEGTR001",
      "AKCBM001",
      "AKCCEC001",
      "AKCCMA001",
      "AKCCOP001",
      "AKCCPR001",
      "AKCCTO001",
      "AKCOIL001",
      "ANGAAJ001",
      "ANGAT0019",
      "ANGAT005",
      "ANGAT21",
      "ANGAT37",
      "ANGATBAS",
      "ANGATBG01",
      "ANGATBS1",
      "ANGATCOCO",
      "ANGATCOM1",
      "ANGATCU",
      "ANGATDJ1",
      "ANGATDM022",
      "ANGATE15",
      "ANGATERIC",
      "ANGATGAE",
      "ANGATIAN",
      "ANGATJEFF",
      "ANGATJT",
      "ANGATK001",
      "ANGATK0018",
      "ANGATK0019",
      "ANGATK002",
      "ANGATK0020",
      "ANGATK0021",
      "ANGATK0022",
      "ANGATK0023",
      "ANGATK003",
      "ANGATK004",
      "ANGATK007",
      "ANGATK008",
      "ANGATK009",
      "ANGATK010",
      "ANGATK011",
      "ANGATK012",
      "ANGATK013",
      "ANGATK016",
      "ANGATK017",
      "ANGATK022",
      "ANGATK023",
      "ANGATK024",
      "ANGATK026",
      "ANGATK027",
      "ANGATK029",
      "ANGATK030",
      "ANGATK031",
      "ANGATK032",
      "ANGATK033",
      "ANGATK034",
      "ANGATK035",
      "ANGATK036",
      "ANGATKA014",
      "ANGATKA015",
      "ANGATMEL",
      "ANGATMNK",
      "ANGATPAL1",
      "ANGATPB01",
      "ANGATPB02",
      "ANGATPPP",
      "ANGATR001",
      "ANGATRA001",
      "ANGATRD1",
      "ANGATREN",
      "ANGATRICE",
      "ANGATRICE2",
      "ANGATRON",
      "ANGATRP1",
      "ANGATSC023",
      "ANGATSCG1",
      "ANGATWE1",
      "ANGATYCF01",
      "ANGTKA001",
      "ANGTKA002",
      "ANGTKA003",
      "ANGTKA004",
      "JACMMK001",
      "JIASMEA001",
      "JLVEGG01",
      "MASAEGGS01",
      "RJ3BEGG",
    ]);

    // ✅ Override if in list
    if (code && FORCE_KEEP.has(code)) {
      to_delete = 0;
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
        inserts.flatMap((i) => [
          i.supplier_code,
          i.rs_disable,
          i.bo_disable,
          i.to_delete,
        ]),
      );
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

      await trx.raw(`
        UPDATE \`0_rms_exluded_supplier\`
        SET
          rs_disable = CASE supplier_code ${rsCase} END,
          bo_disable = CASE supplier_code ${boCase} END
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

module.exports = new OneDcSupplier();
