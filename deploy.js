#!/usr/bin/env node
"use strict";

// [ADDED] Deployment runner for GitHub Actions.
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const PROJECT_DIR = process.env.PROJECT_DIR || __dirname;
const BRANCH = process.env.BRANCH || "main";
const PM2_TARGET = process.env.PM2_TARGET || "beanance-market";
const APP_PORT = Number(process.env.APP_PORT || 3030);
const LOG_DIR = process.env.DEPLOY_LOG_DIR || "/var/log/beanance-market-deploy";

function log(message) {
  process.stdout.write(`[${new Date().toISOString()}] ${message}\n`);
}

function run(name, command, args) {
  log(`START: ${name}`);
  const result = spawnSync(command, args, {
    cwd: path.resolve(PROJECT_DIR),
    stdio: "inherit",
    env: { ...process.env, PORT: String(APP_PORT) },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${name} exited with code ${result.status}`);
  }
  log(`SUCCESS: ${name}`);
}

async function verifyHealth() {
  const url = `http://127.0.0.1:${APP_PORT}/health`;
  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(3000) });
      if (response.ok) {
        const payload = await response.json();
        if (payload?.success === true && payload?.status === "ok") {
          log(`HEALTH CHECK OK: HTTP ${response.status}`);
          return;
        }
      }
      log(`Health check attempt ${attempt}: HTTP ${response.status}`);
    } catch (error) {
      log(`Health check attempt ${attempt}: ${error.message}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
  throw new Error("Application health check failed after PM2 restart.");
}

async function main() {
  const projectPath = path.resolve(PROJECT_DIR);
  if (!fs.existsSync(path.join(projectPath, ".git"))) {
    throw new Error(`Git repository not found: ${projectPath}`);
  }

  fs.mkdirSync(LOG_DIR, { recursive: true });
  const logFile = path.join(LOG_DIR, `deploy-${new Date().toISOString().replace(/[:.]/g, "-")}.log`);
  const startedAt = Date.now();
  const originalOut = process.stdout.write.bind(process.stdout);
  const originalErr = process.stderr.write.bind(process.stderr);

  const append = (writer, chunk, encoding, callback) => {
    fs.appendFileSync(logFile, Buffer.isBuffer(chunk) ? chunk : String(chunk));
    return writer(chunk, encoding, callback);
  };
  process.stdout.write = (chunk, encoding, callback) => append(originalOut, chunk, encoding, callback);
  process.stderr.write = (chunk, encoding, callback) => append(originalErr, chunk, encoding, callback);

  log(`Deployment started in ${projectPath}`);
  log(`Branch: ${BRANCH}; PM2 target: ${PM2_TARGET}; port: ${APP_PORT}`);
  log(`Deployment log: ${logFile}`);

  run("Git pull", "git", ["pull", "--ff-only", "origin", BRANCH]);
  run("Install production dependencies", "npm", ["install", "--omit=dev", "--no-package-lock"]);
  run("Restart backend with PM2", "pm2", ["restart", PM2_TARGET, "--update-env"]);
  await verifyHealth();

  log(`DEPLOYMENT SUCCESS in ${((Date.now() - startedAt) / 1000).toFixed(1)} seconds`);
}

main().catch((error) => {
  process.stderr.write(`DEPLOYMENT FAILED: ${error.stack || error.message}\n`);
  process.exit(1);
});
