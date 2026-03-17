"use strict";

const Knex = require("knex");

class BranchDatabase {
  constructor() {
    this.connections = {};
  }

  /**
   * Connect to a branch database dynamically
   * @param {Object} branch {branch_name, branch_ip, branch_dbuser, branch_dbpass, branch_database}
   */
  connect(branch) {
    const name = branch.branch_name;

    if (!this.connections[name]) {
      const knex = Knex({
        client: "mysql",
        connection: {
          host: branch.branch_ip,
          user: branch.branch_dbuser,
          password: branch.branch_dbpass,
          database: branch.branch_database,
          connectTimeout: 5000,
        },
        pool: { min: 0, max: 5 },
      });

      // Wrap knex to mimic AdonisJS Database API
      this.connections[name] = {
        knex,
        table: (tableName) => knex(tableName),
        raw: (sql, bindings) => knex.raw(sql, bindings),

        // Transaction helper
        transaction: async (callback) => {
          return await knex.transaction(async (trx) => {
            const trxWrapper = {
              table: (tableName) => trx(tableName),
              raw: (sql, bindings) => trx.raw(sql, bindings),
              commit: trx.commit.bind(trx),
              rollback: trx.rollback.bind(trx),
            };

            return await callback(trxWrapper);
          });
        },
      };
    }

    return this.connections[name];
  }

  /**
   * Close a single branch connection
   */
  async close(branchName) {
    if (this.connections[branchName]) {
      await this.connections[branchName].knex.destroy();
      delete this.connections[branchName];
    }
  }

  /**
   * Close all branch connections
   */
  async closeAll() {
    const keys = Object.keys(this.connections);
    for (const key of keys) {
      await this.connections[key].knex.destroy();
      delete this.connections[key];
    }
  }
}

module.exports = new BranchDatabase();
