/**
 * Pipeline worker entrypoint — claims `upload-url` jobs from the durable queue
 * and runs them through the existing upload code path.
 *
 * Why a separate process:
 *   The Railway-hosted orchestrator (server.ts) is the place where the dashboard
 *   lives and where job-fan-out happens. Doing the heavy per-URL browser work
 *   inside that same process means a busy crawl can starve the dashboard.
 *
 *   Splitting it out also unlocks the *hybrid* deployment the user wanted:
 *     - Railway runs the orchestrator + queue producer (always-on, cheap),
 *     - Their M4 laptop runs N workers via `npm run worker` against the same
 *       DATABASE_URL, doing all browser + R2 + DB work locally.
 *
 *   The worker is therefore environment-agnostic. Anywhere it can reach
 *   DATABASE_URL and R2, it can drain the queue.
 *
 * Run locally on the M4:
 *   DATABASE_URL=postgres://… \
 *   R2_ACCOUNT_ID=… R2_ACCESS_KEY_ID=… R2_SECRET_ACCESS_KEY=… \
 *   WORKER_ID=lucamac-1 WORKER_CONCURRENCY=4 \
 *   npm run worker
 *
 * Run alongside the server (default Railway deploy):
 *   start it as a Railway worker service pointing at the same DB.
 */
import "dotenv/config";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import crypto from "node:crypto";

import { uploadRetailer } from "./upload.js";
import { safeParseConfig, type Config } from "./schemas/config.js";
import {
  ensurePipelinePersistenceSchema,
  getSetting,
  isAutoPipelineEnabled,
  recordScrapeError,
  recordUploadUrlResult,
  upsertWorkerHeartbeat,
} from "./pipelineStore.js";
import { ErrorCodes, classifyError } from "./errorCodes.js";
import {
  enqueueProcessing,
  getBoss,
  QUEUES,
  type ProcessingJobData,
  type UploadUrlJobData,
} from "./queue.js";
import { runEmbedBatch, runNobgBatch } from "./processing/bridge.js";

const WORKER_ID =
  process.env.WORKER_ID ?? `${os.hostname()}-${process.pid}-${crypto.randomBytes(3).toString("hex")}`;
const WORKER_CONCURRENCY = Math.max(1, parseInt(process.env.WORKER_CONCURRENCY ?? "2", 10));
const HEARTBEAT_INTERVAL_MS = 15_000;
const CONFIGS_DIR = process.env.CONFIGS_DIR ?? path.join(process.cwd(), "configs");

/**
 * Which queues this worker claims. Default = upload-url only, so an existing
 * Railway worker keeps its exact behavior and can never claim processing jobs
 * (it has no rembg/venv). The home M1 runs:
 *   WORKER_QUEUES=upload-url,process-nobg,process-embed
 */
const WORKER_QUEUES = (process.env.WORKER_QUEUES ?? QUEUES.UPLOAD_URL)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const PROCESS_CHAIN_DEPTH_MAX = 50;

let stopping = false;

/** Lazy config cache so the same retailer's config isn't re-read on every job. */
const configCache = new Map<string, { config: Config; loadedAt: number }>();
const CONFIG_CACHE_TTL_MS = 5 * 60 * 1000;

async function loadConfigFor(retailer: string): Promise<Config | null> {
  const cached = configCache.get(retailer);
  if (cached && Date.now() - cached.loadedAt < CONFIG_CACHE_TTL_MS) {
    return cached.config;
  }
  const filename = path.join(CONFIGS_DIR, `${retailer}.json`);
  if (!fs.existsSync(filename)) return null;
  try {
    const raw = fs.readFileSync(filename, "utf-8");
    const parsed = safeParseConfig(JSON.parse(raw));
    if (!parsed.success) {
      await recordScrapeError({
        code: ErrorCodes.EXPLORE_CONFIG_INVALID,
        detail: `Worker could not parse config for ${retailer}: ${parsed.error}`,
        retailer,
        stage: "upload",
        metadata: { workerId: WORKER_ID, file: filename },
      });
      return null;
    }
    configCache.set(retailer, { config: parsed.data, loadedAt: Date.now() });
    return parsed.data;
  } catch (err) {
    await recordScrapeError({
      code: ErrorCodes.STORE_WRITE_FAILED,
      detail: `Worker config load failed: ${err instanceof Error ? err.message : String(err)}`,
      retailer,
      stage: "upload",
      metadata: { workerId: WORKER_ID },
    });
    return null;
  }
}

/**
 * Process one queue job. Returns once the job completes (success or terminal
 * failure). Throwing from here triggers pg-boss's retry policy.
 */
async function handleUploadUrlJob(jobData: UploadUrlJobData, jobId: string): Promise<void> {
  const { retailer, url, crawlSourceCrawledAt, parentJobId } = jobData;
  const config = await loadConfigFor(retailer);
  if (!config) {
    // No config => can't process. Treat as terminal failure so pg-boss DLQs.
    throw new Error(`No config available for retailer "${retailer}"`);
  }

  // Reuse the existing single-process upload code for a single URL. This is
  // intentionally simple; the per-job browser-init cost is acceptable at our
  // target scale (10K/week ÷ N workers). Optimize later by extracting a
  // long-lived per-worker Stagehand session if profiling shows it matters.
  const result = await uploadRetailer(
    config,
    [url],
    (msg) => console.log(`[worker=${WORKER_ID} job=${jobId}] ${msg}`),
    undefined,
    {
      onItemResult: async (itemResult) => {
        await recordUploadUrlResult({
          retailer,
          crawlSourceCrawledAt,
          url: itemResult.url,
          jobId: parentJobId ?? jobId,
          status: itemResult.status,
          externalId: itemResult.externalId,
          itemName: itemResult.itemName,
          imageCount: itemResult.imageCount,
          uploadedToR2: itemResult.uploadedToR2,
          upsertedToDb: itemResult.upsertedToDb,
          error: itemResult.error,
          metadata: { ...(itemResult.metadata ?? {}), workerId: WORKER_ID, queueJobId: jobId },
        });
        if (itemResult.status === "failed") {
          await recordScrapeError({
            code: classifyError(itemResult.error, ErrorCodes.UPLOAD_UNCAUGHT),
            detail: itemResult.error ?? "Unknown upload failure",
            retailer,
            stage: "upload",
            url: itemResult.url,
            jobId: parentJobId ?? jobId,
            metadata: {
              workerId: WORKER_ID,
              queueJobId: jobId,
              itemName: itemResult.itemName,
              imageCount: itemResult.imageCount,
            },
          });
        }
      },
    },
  );

  if (result.failed > 0 && result.uploaded === 0 && result.skipped === 0) {
    // Throw so pg-boss schedules a retry (with the configured backoff).
    throw new Error(`upload-url job failed for ${url}`);
  }
}

/**
 * Process one bounded processing batch (nobg or embed). Chained re-enqueue
 * keeps draining the backlog in expiry-safe slices; the kill switch is
 * honored between batches (and before sweep batches start).
 */
async function handleProcessingJob(data: ProcessingJobData, jobId: string): Promise<void> {
  const log = (line: string) => console.log(`[worker=${WORKER_ID} job=${jobId}] ${line}`);

  if (data.sweep) {
    const autoOn = await isAutoPipelineEnabled();
    const gateOn = await getSetting<boolean>("gate_processing", true);
    if (!autoOn || !gateOn) {
      log(`processing paused (auto_pipeline=${autoOn} gate_processing=${gateOn}) — skipping sweep batch`);
      return;
    }
  }

  let moreRemains = false;
  if (data.kind === "nobg") {
    const result = await runNobgBatch({ limit: data.limit, log });
    log(
      `nobg batch done: processed=${result.processed} failed=${result.failed} remaining≈${result.remaining} hasNobg+=${result.hasNobgUpdated}`,
    );
    moreRemains = result.remaining > 0;
  } else {
    const result = await runEmbedBatch({ limit: data.limit, log });
    moreRemains = result.hitLimit;
  }

  const depth = data.chainDepth ?? 0;
  if (moreRemains && depth < PROCESS_CHAIN_DEPTH_MAX) {
    // The kill switch stops ALL chaining (manual runs included) — a manual
    // first batch always runs, but a runaway chain must be stoppable.
    if (!(await isAutoPipelineEnabled())) {
      log("kill switch off — not chaining next batch");
      return;
    }
    const nextId = await enqueueProcessing({ ...data, chainDepth: depth + 1 });
    log(`backlog remains — chained next ${data.kind} batch (job ${nextId}, depth ${depth + 1})`);
  }
}

/**
 * After upload activity: if the upload queue just drained and the conveyor is
 * on, kick a processing batch. Covers "upload completed" for the queue path,
 * which has no single completion event. Debounced + hour-bucketed singleton.
 */
let lastDrainCheckAt = 0;
async function maybeTriggerProcessingAfterDrain(): Promise<void> {
  if (!WORKER_QUEUES.includes(QUEUES.PROCESS_NOBG)) return; // only the processing-capable worker
  const now = Date.now();
  if (now - lastDrainCheckAt < 60_000) return;
  lastDrainCheckAt = now;
  try {
    const boss = await getBoss();
    if (!boss) return;
    const waiting = await boss.getQueueSize(QUEUES.UPLOAD_URL);
    if (waiting > 0) return;
    if (!(await isAutoPipelineEnabled())) return;
    if (!(await getSetting<boolean>("gate_processing", true))) return;
    const limit = Math.max(1, parseInt(process.env.PROCESS_NOBG_DEFAULT_LIMIT ?? "100", 10));
    const hourBucket = new Date().toISOString().slice(0, 13);
    const id = await enqueueProcessing(
      { kind: "nobg", limit, sweep: true },
      { singletonKey: `nobg:drain:${hourBucket}` },
    );
    if (id) console.log(`[worker=${WORKER_ID}] upload queue drained — enqueued nobg batch ${id}`);
  } catch (err) {
    console.error(`[worker=${WORKER_ID}] drain-check failed:`, err);
  }
}

async function heartbeatLoop(): Promise<void> {
  while (!stopping) {
    try {
      await upsertWorkerHeartbeat({
        workerId: WORKER_ID,
        hostname: os.hostname(),
        pid: process.pid,
        concurrency: WORKER_CONCURRENCY,
        metadata: {
          platform: process.platform,
          arch: process.arch,
          nodeVersion: process.version,
          queues: WORKER_QUEUES,
        },
      });
    } catch (err) {
      console.error(`[worker=${WORKER_ID}] heartbeat failed:`, err);
    }
    await new Promise((r) => setTimeout(r, HEARTBEAT_INTERVAL_MS));
  }
}

async function main(): Promise<void> {
  console.log(`[worker=${WORKER_ID}] starting (concurrency=${WORKER_CONCURRENCY})`);
  await ensurePipelinePersistenceSchema();
  const boss = await getBoss();
  if (!boss) {
    console.error(`[worker=${WORKER_ID}] DATABASE_URL not set — refusing to start.`);
    process.exit(1);
  }

  // Heartbeat in the background.
  void heartbeatLoop();

  // pg-boss v10 work() options:
  //   batchSize: claim up to N jobs per poll. We then run them concurrently
  //              inside the handler with Promise.all to get our concurrency.
  //   pollingIntervalSeconds: how often to look for new jobs when idle.
  if (WORKER_QUEUES.includes(QUEUES.UPLOAD_URL)) {
    await boss.work<UploadUrlJobData>(
      QUEUES.UPLOAD_URL,
      { batchSize: WORKER_CONCURRENCY, pollingIntervalSeconds: 2 },
      async (jobs) => {
        // v10 always passes an array; guard for older shapes just in case.
        const list = Array.isArray(jobs) ? jobs : [jobs];
        await Promise.all(
          list.map(async (j) => {
            const data = j.data;
            console.log(`[worker=${WORKER_ID}] claimed job ${j.id} (${data.retailer} ${data.url})`);
            await handleUploadUrlJob(data, j.id);
          }),
        );
        void maybeTriggerProcessingAfterDrain();
      },
    );
  }

  // Processing queues: batchSize is ALWAYS 1 — a bridge job is internally
  // parallel (rembg chains / MPS batches); two at once would thrash the M1.
  for (const queue of [QUEUES.PROCESS_NOBG, QUEUES.PROCESS_EMBED] as const) {
    if (!WORKER_QUEUES.includes(queue)) continue;
    await boss.work<ProcessingJobData>(
      queue,
      { batchSize: 1, pollingIntervalSeconds: 5 },
      async (jobs) => {
        const list = Array.isArray(jobs) ? jobs : [jobs];
        for (const j of list) {
          console.log(`[worker=${WORKER_ID}] claimed ${queue} job ${j.id} (limit=${j.data.limit})`);
          await handleProcessingJob(j.data, j.id);
        }
      },
    );
  }

  console.log(`[worker=${WORKER_ID}] ready, waiting for jobs (queues: ${WORKER_QUEUES.join(", ")}).`);
}

async function shutdown(signal: string): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.log(`[worker=${WORKER_ID}] received ${signal}, shutting down…`);
  const boss = await getBoss();
  if (boss) {
    try {
      await boss.stop({ graceful: true, wait: true });
    } catch (err) {
      console.error(`[worker=${WORKER_ID}] boss.stop error:`, err);
    }
  }
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

main().catch(async (err) => {
  console.error(`[worker=${WORKER_ID}] fatal error:`, err);
  await recordScrapeError({
    code: ErrorCodes.UNKNOWN,
    detail: `Worker fatal error: ${err instanceof Error ? err.message : String(err)}`,
    stage: "upload",
    metadata: { workerId: WORKER_ID, hostname: os.hostname() },
  }).catch(() => {});
  process.exit(1);
});
