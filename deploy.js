#!/usr/bin/env node
"use strict";

// [MODIFIED] Use ES module imports because package.json declares "type": "module".
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// [MODIFIED] Resolve the project directory without CommonJS __dirname.
const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = process.env.PROJECT_DIR || SCRIPT_DIR;
const BRANCH = process.env.BRANCH || "main";
const PM2_TARGET = process.env.PM2_TARGET || "beanance-market";
const APP_PORT = Number(process.env.APP_PORT || 3030);
const LOG_DIR = process.env.DEPLOY_LOG_DIR || "/var/log/beanance-market-deploy";

function log(message) {
  process.stdout.write(`[${new Date().toISOString()}] ${message}
`);
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

// [ADDED] Add variables from .env.example that are missing in .env.
// Existing production values are preserved and never overwritten.
function syncEnvFromExample() {
  const projectPath = path.resolve(PROJECT_DIR);
  const envPath = path.join(projectPath, ".env");
  const examplePath = path.join(projectPath, ".env.example");

  if (!fs.existsSync(examplePath)) {
    throw new Error(`.env.example not found: ${examplePath}`);
  }

  const exampleContent = fs.readFileSync(examplePath, "utf8");
  if (!fs.existsSync(envPath)) {
    fs.copyFileSync(examplePath, envPath, fs.constants.COPYFILE_EXCL);
    log("Created .env from .env.example because .env did not exist.");
    return;
  }

  const currentContent = fs.readFileSync(envPath, "utf8");
  const existingKeys = new Set(
    currentContent
      .split(/\r?
/)
      .map((line) => line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=/))
      .filter(Boolean)
      .map((match) => match[1])
  );

  const missingLines = exampleContent
    .split(/\r?
/)
    .filter((line) => {
      const match = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=/);
      if (!match || existingKeys.has(match[1])) return false;
      existingKeys.add(match[1]);
      return true;
    });

  if (missingLines.length === 0) {
    log(".env already contains every variable defined in .env.example.");
    return;
  }

  const separator = currentContent.length === 0 || currentContent.endsWith("
") ? "" : "
";
  const addition = `${separator}
# [ADDED] Missing variables synchronized from .env.example during deployment.
${missingLines.join("
")}
`;
  fs.appendFileSync(envPath, addition, "utf8");
  log(`Added ${missingLines.length} missing variable(s) from .env.example to .env. Existing values were preserved.`);
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
  // [ADDED] Keep only the newest deployment logs to prevent unbounded disk usage.
  const oldLogs = fs.readdirSync(LOG_DIR).filter((name) => name.startsWith("deploy-") && name.endsWith(".log")).sort();
  for (const name of oldLogs.slice(0, Math.max(0, oldLogs.length - 9))) fs.rmSync(path.join(LOG_DIR, name), { force: true });
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
  // [ADDED] Synchronize missing environment variables before dependency install and PM2 restart.
  syncEnvFromExample();
  run("Install production dependencies", "npm", ["install", "--omit=dev", "--no-package-lock"]);
  run("Restart backend with PM2", "pm2", ["restart", PM2_TARGET, "--update-env"]);
  await verifyHealth();

  log(`DEPLOYMENT SUCCESS in ${((Date.now() - startedAt) / 1000).toFixed(1)} seconds`);
}

main().catch((error) => {
  process.stderr.write(`DEPLOYMENT FAILED: ${error.stack || error.message}
`);
  process.exit(1);
});
