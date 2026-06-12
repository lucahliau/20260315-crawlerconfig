/**
 * Enqueue a processing batch directly into the shared pg-boss queue —
 * useful when you can't (or don't want to) go through the dashboard API.
 *
 *   npx tsx scripts/enqueue-processing.ts nobg [limit]
 *   npx tsx scripts/enqueue-processing.ts embed [limit]
 *   npx tsx scripts/enqueue-processing.ts all
 *   npx tsx scripts/enqueue-processing.ts stats     (no enqueue, just queue state)
 *
 * Whichever worker owns the processing queues (the home M1) claims it
 * within seconds; the batch chain then drains the remaining backlog.
 */
import "dotenv/config";
import { enqueueProcessing, getQueueStats, stopBoss } from "../src/queue.js";

const kind = process.argv[2];
const limitArg = Number(process.argv[3]);

if (!kind || !["nobg", "embed", "all", "stats"].includes(kind)) {
  console.error("Usage: npx tsx scripts/enqueue-processing.ts <nobg|embed|all|stats> [limit]");
  process.exit(1);
}

const kinds: ("nobg" | "embed")[] =
  kind === "all" ? ["nobg", "embed"] : kind === "stats" ? [] : [kind as "nobg" | "embed"];

for (const k of kinds) {
  const fallback = k === "nobg" ? 100 : 2000;
  const limit = Number.isFinite(limitArg) && limitArg > 0 ? Math.floor(limitArg) : fallback;
  const id = await enqueueProcessing({ kind: k, limit });
  console.log(id ? `${k}: enqueued job ${id} (limit ${limit})` : `${k}: enqueue failed (queue disabled?)`);
}

const stats = await getQueueStats();
for (const s of stats.filter((s) => s.name.startsWith("process-"))) {
  console.log(`${s.name}: waiting=${s.waiting} active=${s.active} failed=${s.failed}`);
}
await stopBoss();
