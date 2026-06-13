/**
 * Write one durable worker_issue from the shell, so non-Node tooling (the
 * auto-updater) can leave a breadcrumb that shows up on the cloud dashboard's
 * Errors → Worker issues tab.
 *
 * Usage:
 *   npx tsx scripts/record-worker-issue.ts <source> <message> [severity]
 *
 * Reads DATABASE_URL from the environment / .env (via dotenv). Exits 0 even on
 * failure — a logging breadcrumb must never break the caller.
 */
import "dotenv/config";
import os from "node:os";
import { recordWorkerIssue, getPool } from "../src/pipelineStore.js";

async function main(): Promise<void> {
  const [, , source, message, severity] = process.argv;
  if (!source || !message) {
    console.error("usage: record-worker-issue.ts <source> <message> [severity]");
    process.exit(0);
  }
  await recordWorkerIssue({
    workerId: process.env.WORKER_ID ?? `${os.hostname()}-autoupdate`,
    source,
    message,
    severity: severity || "info",
    metadata: { via: "auto-update", hostname: os.hostname() },
  });
}

main()
  .catch((err) => console.error("record-worker-issue failed:", err))
  .finally(async () => {
    try {
      await getPool()?.end();
    } catch {
      /* ignore */
    }
    process.exit(0);
  });
