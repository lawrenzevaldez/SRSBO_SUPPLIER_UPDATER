"use strict";

/*
|--------------------------------------------------------------------------
| Http server
|--------------------------------------------------------------------------
|
| This file bootstrap Adonisjs to start the HTTP server. You are free to
| customize the process of booting the http server.
|
| """ Loading ace commands """
|     At times you may want to load ace commands when starting the HTTP server.
|     Same can be done by chaining `loadCommands()` method after
|
| """ Preloading files """
|     Also you can preload files by calling `preLoad('path/to/file')` method.
|     Make sure to pass relative path from the project root.
*/

const { Ignitor } = require("@adonisjs/ignitor");
const cron = require("node-cron");

let oneDcSupplierJob = null;
let supplierNoRSJob = null;

new Ignitor(require("@adonisjs/fold"))
  .appRoot(__dirname)
  .fireHttpServer()
  .then(() => {
    console.log("HTTP Server Started");
    console.log("Cron Scheduler Initialized");

    const OneDcSupplier = use("App/Services/OneDcSupplier");
    const SupplierNoRS = use("App/Services/SupplierNoRS");

    /*
      |--------------------------------------------------------------------------
      | CRON 1 — OneDcSupplier
      |--------------------------------------------------------------------------
    */

    cron.schedule("*/5 * * * *", async () => {
      if (oneDcSupplierJob) {
        console.log("[OneDcSupplier] Previous job still running. Skipping.");
        return;
      }

      console.log("================================");
      console.log("[OneDcSupplier] Started:", new Date());
      console.log("================================");

      oneDcSupplierJob = (async () => {
        try {
          await OneDcSupplier.run();
        } catch (error) {
          console.error("[OneDcSupplier] Error:", error);
        }
      })();

      try {
        await oneDcSupplierJob;
      } finally {
        oneDcSupplierJob = null;
      }

      console.log("================================");
      console.log("[OneDcSupplier] Finished");
      console.log("================================");
    });

    /*
      |--------------------------------------------------------------------------
      | CRON 2 — SupplierNoRS
      |--------------------------------------------------------------------------
      */

    cron.schedule("*/15 * * * *", async () => {
      if (supplierNoRSJob) {
        console.log("[SupplierNoRS] Previous job still running. Skipping.");
        return;
      }

      console.log("================================");
      console.log("[SupplierNoRS] Started:", new Date());
      console.log("================================");

      supplierNoRSJob = (async () => {
        try {
          await SupplierNoRS.run();
        } catch (error) {
          console.error("[SupplierNoRS] Error:", error);
        }
      })();

      try {
        await supplierNoRSJob;
      } finally {
        supplierNoRSJob = null;
      }

      console.log("================================");
      console.log("[SupplierNoRS] Finished");
      console.log("================================");
    });
  })
  .catch(console.error);
