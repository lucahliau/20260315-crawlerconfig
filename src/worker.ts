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
  getRetailerPipelineState,
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
  getQueueStats,
  purgeQueuedUploadsForRetailer,
  QUEUES,
  type ProcessingJobData,
  type UploadUrlJobData,
} from "./queue.js";
import { runForDomain, domainKey } from "./domainGate.js";
import { getProcessingBacklog, runEmbedBatch, runNobgBatch } from "./processing/bridge.js";
import {
  getHeartbeatTelemetry,
  recordActivity,
  recordIssue,
  startCaffeinate,
  startWorkerStatusServer,
  statusJobFinished,
  statusJobStarted,
} from "./workerStatus.js";

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

/**
 * Retailers whose queued uploads we've already purged this process lifetime
 * after finding no valid config — so a batch of doomed jobs triggers exactly
 * one purge + one issue, not one per URL.
 */
const purgedNoConfigRetailers = new Set<string>();

async function loadConfigFor(retailer: string): Promise<Config | null> {
  const cached = configCache.get(retailer);
  if (cached && Date.now() - cached.loadedAt < CONFIG_CACHE_TTL_MS) {
    return cached.config;
  }
  const filename = path.join(CONFIGS_DIR, `${retailer}.json`);
  try {
    // Local file first; else the config persisted by the explore job in
    // retailer_pipeline_state — a home worker's checkout may not have the
    // config files for retailers explored on Railway after the last pull.
    let rawConfig: unknown = null;
    if (fs.existsSync(filename)) {
      rawConfig = JSON.parse(fs.readFileSync(filename, "utf-8"));
    } else {
      const state = await getRetailerPipelineState(retailer);
      rawConfig = state?.exploreState?.config ?? null;
      if (!rawConfig) return null;
      console.log(`[worker=${WORKER_ID}] config for ${retailer} loaded from Postgres (no local file).`);
    }
    const parsed = safeParseConfig(rawConfig);
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
    // Missing/invalid config is a RETAILER-level fault, not a per-URL transient:
    // retrying this URL — or churning through the other queued URLs — can never
    // succeed. Record it once, purge the retailer's remaining pending uploads so
    // they don't each burn their retry budget, then COMPLETE this job (no throw)
    // so pg-boss doesn't retry-storm it. The fix is to (re)explore the retailer.
    if (!purgedNoConfigRetailers.has(retailer)) {
      purgedNoConfigRetailers.add(retailer);
      await recordScrapeError({
        code: ErrorCodes.EXPLORE_CONFIG_INVALID,
        detail: `No valid config for retailer "${retailer}" — queued uploads purged. Re-explore to fix.`,
        retailer,
        stage: "upload",
        url,
        jobId: parentJobId ?? jobId,
        metadata: { workerId: WORKER_ID, queueJobId: jobId },
      });
      let purged = 0;
      try {
        purged = await purgeQueuedUploadsForRetailer(retailer);
      } catch (err) {
        console.error(`[worker=${WORKER_ID}] purge for ${retailer} failed:`, err);
      }
      recordIssue(
        "upload",
        `${retailer}: no valid config — purged ${purged} queued upload(s); re-explore needed.`,
      );
      console.warn(`[worker=${WORKER_ID}] ${retailer} has no valid config — purged ${purged} queued upload(s).`);
    }
    return; // complete the job; do not retry
  }

  // Capture the per-item error so the batch-level failure throw carries a real
  // cause into the issue feed instead of an opaque "upload-url job failed".
  let lastItemError: string | undefined;

  // Reuse the existing single-process upload code for a single URL. This is
  // intentionally simple; the per-job browser-init cost is acceptable at our
  // target scale (10K/week ÷ N workers). Optimize later by extracting a
  // long-lived per-worker Stagehand session if profiling shows it matters.
  //
  // Serialize per domain (concurrency 1 + politeness gap) so the queue's
  // batch-claim + Promise.all fan-out never hits one site with concurrent,
  // undelayed fetches — that self-inflicted burst is what rate-limits us.
  const domain = domainKey(url, retailer);
  const result = await runForDomain(domain, () =>
    uploadRetailer(
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
            lastItemError = itemResult.error ?? "Unknown upload failure";
            await recordScrapeError({
              code: classifyError(itemResult.error, ErrorCodes.UPLOAD_UNCAUGHT),
              detail: lastItemError,
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
    ),
  );

  if (result.failed > 0 && result.uploaded === 0 && result.skipped === 0) {
    // Throw so pg-boss schedules a retry (with the configured backoff). Include
    // the real per-item cause so the issue feed is diagnosable at a glance.
    throw new Error(`upload-url job failed for ${url}: ${lastItemError ?? "unknown error"}`);
  }
}

/**
 * Process one bounded processing batch (nobg or embed). Chained re-enqueue
 * keeps draining the backlog in expiry-safe slices; the kill switch is
 * honored between batches (and before sweep batches start).
 */
async function handleProcessingJob(data: ProcessingJobData, jobId: string): Promise<void> {
  const log = (line: string) => {
    console.log(`[worker=${WORKER_ID} job=${jobId}] ${line}`);
    recordActivity(line);
  };
  statusJobStarted(jobId, data.kind, `limit ${data.limit}`);

  if (data.sweep) {
    const autoOn = await isAutoPipelineEnabled();
    const gateOn = await getSetting<boolean>("gate_processing", true);
    if (!autoOn || !gateOn) {
      log(`processing paused (auto_pipeline=${autoOn} gate_processing=${gateOn}) — skipping sweep batch`);
      return;
    }
  }

  let moreRemains = false;
  try {
    if (data.kind === "nobg") {
      const result = await runNobgBatch({ limit: data.limit, log });
      log(
        `nobg batch done: processed=${result.processed} failed=${result.failed} remaining≈${result.remaining} hasNobg+=${result.hasNobgUpdated}`,
      );
      if (result.failed > 0) {
        recordIssue("nobg", `${result.failed} image(s) failed in batch ${jobId} (see activity log)`);
      }
      statusJobFinished(jobId, data.kind, `processed ${result.processed}, failed ${result.failed}`);
      moreRemains = result.remaining > 0;
    } else {
      const result = await runEmbedBatch({ limit: data.limit, log });
      statusJobFinished(jobId, data.kind, `batch complete (limit ${data.limit})`);
      moreRemains = result.hitLimit;
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    statusJobFinished(jobId, data.kind, `FAILED: ${message}`);
    recordIssue(data.kind, `job ${jobId} failed: ${message} (pg-boss will retry)`);
    throw err;
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

/**
 * Self-feeding backlog loop: whenever there is processing backlog but the
 * queue is completely empty (nothing waiting, nothing running), bootstrap a
 * sweep batch — the batch chain then carries it until the backlog is gone.
 * Without this, an idle worker would sit on a backlog until the nightly
 * cron or a manual tap. Respects the Autopilot kill switch + processing
 * gate (checked here AND at claim time, since these are sweep batches).
 */
const IDLE_SWEEP_INTERVAL_MS = Math.max(
  60_000,
  parseInt(process.env.IDLE_SWEEP_INTERVAL_MS ?? "600000", 10),
);

async function idleBacklogLoop(): Promise<void> {
  // Let boss/heartbeat settle before the first check.
  await new Promise((r) => setTimeout(r, 60_000));
  while (!stopping) {
    try {
      const autoOn = await isAutoPipelineEnabled();
      const gateOn = await getSetting<boolean>("gate_processing", true);
      if (autoOn && gateOn) {
        const [stats, backlog] = await Promise.all([getQueueStats(), getProcessingBacklog()]);
        const hourBucket = new Date().toISOString().slice(0, 13);
        const targets = [
          {
            queue: QUEUES.PROCESS_NOBG,
            kind: "nobg" as const,
            needs: backlog.needsNobg,
            limit: Math.max(1, parseInt(process.env.PROCESS_NOBG_DEFAULT_LIMIT ?? "100", 10)),
          },
          {
            queue: QUEUES.PROCESS_EMBED,
            kind: "embed" as const,
            needs: backlog.needsEmbed,
            limit: Math.max(1, parseInt(process.env.PROCESS_EMBED_DEFAULT_LIMIT ?? "2000", 10)),
          },
        ];
        for (const t of targets) {
          if (!WORKER_QUEUES.includes(t.queue) || t.needs <= 0) continue;
          const stat = stats.find((s) => s.name === t.queue);
          if (!stat || stat.waiting > 0 || stat.active > 0) continue;
          const id = await enqueueProcessing(
            { kind: t.kind, limit: t.limit, sweep: true },
            { singletonKey: `${t.kind}:idle:${hourBucket}` },
          );
          if (id) {
            console.log(
              `[worker=${WORKER_ID}] idle + backlog (${t.needs} ${t.kind}) — bootstrapped batch ${id}`,
            );
            recordActivity(`idle backlog: started ${t.kind} (${t.needs} remaining)`);
          }
        }
      }
    } catch (err) {
      console.error(`[worker=${WORKER_ID}] idle backlog check failed:`, err);
    }
    await new Promise((r) => setTimeout(r, IDLE_SWEEP_INTERVAL_MS));
  }
}

async function heartbeatLoop(): Promise<void> {
  while (!stopping) {
    try {
      // Machine telemetry rides the heartbeat so the CLOUD dashboard can show
      // the home server's memory/disk/thermal/power from anywhere.
      const telemetry = await getHeartbeatTelemetry().catch(() => null);
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
          ...(telemetry ? { telemetry } : {}),
        },
      });
    } catch (err) {
      console.error(`[worker=${WORKER_ID}] heartbeat failed:`, err);
      recordIssue("heartbeat", err instanceof Error ? err.message : String(err));
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

  // Self-feeding backlog drain (processing-capable workers only).
  if (WORKER_QUEUES.includes(QUEUES.PROCESS_NOBG) || WORKER_QUEUES.includes(QUEUES.PROCESS_EMBED)) {
    void idleBacklogLoop();
  }

  // Local status dashboard (http://localhost:4577) + keep-awake assertion.
  startWorkerStatusServer({
    workerId: WORKER_ID,
    queues: WORKER_QUEUES,
    concurrency: WORKER_CONCURRENCY,
  });
  startCaffeinate();

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
            recordActivity(`upload ${data.retailer}: ${data.url}`);
            statusJobStarted(j.id, "upload", data.retailer);
            try {
              await handleUploadUrlJob(data, j.id);
              statusJobFinished(j.id, "upload", `${data.retailer} ok`);
            } catch (err) {
              const message = err instanceof Error ? err.message : String(err);
              statusJobFinished(j.id, "upload", `FAILED: ${message}`);
              recordIssue("upload", `${data.retailer} ${data.url}: ${message}`);
              throw err;
            }
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
          recordActivity(`claimed ${queue} job ${j.id} (limit ${j.data.limit})`);
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
