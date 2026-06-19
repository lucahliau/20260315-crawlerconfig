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
  getPool,
  getRetailerPipelineState,
  getSetting,
  isAutoPipelineEnabled,
  recordScrapeError,
  recordUploadUrlResult,
  recordWorkerIssue,
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
import {
  getProcessingBacklog,
  runEmbedBatch,
  runNobgBatch,
  runPersonScanBatch,
} from "./processing/bridge.js";
import { reconcileHasNobg } from "./processing/reconcile.js";
import {
  getHeartbeatTelemetry,
  getMachineTelemetry,
  type MachineTelemetry,
  recordActivity,
  recordIssue,
  setIssueSink,
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
 *   WORKER_QUEUES=upload-url,process-nobg,process-embed,process-person
 */
const WORKER_QUEUES = (process.env.WORKER_QUEUES ?? QUEUES.UPLOAD_URL)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const PROCESS_CHAIN_DEPTH_MAX = 50;

// ---------------------------------------------------------------------------
// Capacity governor (background removal)
// ---------------------------------------------------------------------------
// The home box is the 8GB fanless M1 Air. nobg work is network/GPU-bound, so
// the CPU sits ~60% idle even at "full" load — but 8GB unified RAM (each rembg
// chain ≈ 0.6–0.8GB on top of an embed batch's CLIP model) and the fanless
// thermal envelope ARE the real limits. So instead of a fixed PARALLEL_CHAINS,
// we choose it per-batch from LIVE telemetry: push to the ceiling when RAM is
// free, cool, and on AC; back off when RAM is tight, the SoC is throttling, or
// we're on battery. Set PROCESS_NOBG_PARALLEL to pin a fixed value (bypasses
// the governor); PROCESS_NOBG_PARALLEL_MAX caps the adaptive ceiling.
const NOBG_PARALLEL_MAX = Math.max(2, parseInt(process.env.PROCESS_NOBG_PARALLEL_MAX ?? "7", 10));
const NOBG_PARALLEL_FIXED = process.env.PROCESS_NOBG_PARALLEL
  ? Math.max(1, parseInt(process.env.PROCESS_NOBG_PARALLEL, 10))
  : null;

function governNobgParallel(t: MachineTelemetry | null): { chains: number; reason: string } {
  if (NOBG_PARALLEL_FIXED) return { chains: NOBG_PARALLEL_FIXED, reason: `pinned via env=${NOBG_PARALLEL_FIXED}` };
  if (!t) return { chains: 5, reason: "no telemetry → 5" };
  // Memory bands (available MB, reclaimable-aware). Anchored so the common
  // embed-concurrent case (~3–3.5GB free) keeps today's 5 chains — never a
  // regression — and only climbs when embed is idle and RAM is genuinely free.
  let chains: number;
  if (t.freeMemMb < 1500) chains = 2;
  else if (t.freeMemMb < 2500) chains = 3;
  else if (t.freeMemMb < 3500) chains = 5;
  else if (t.freeMemMb < 4500) chains = 6;
  else chains = NOBG_PARALLEL_MAX;
  const why = [`mem ${t.freeMemMb}MB→${chains}`];
  // Thermal (pmset CPU_Speed_Limit < 100 ⇒ throttling on the fanless chassis).
  if (t.cpuSpeedLimitPct != null && t.cpuSpeedLimitPct < 60) {
    chains = Math.min(chains, 2);
    why.push(`throttle ${t.cpuSpeedLimitPct}%`);
  } else if (t.cpuSpeedLimitPct != null && t.cpuSpeedLimitPct < 85) {
    chains = Math.min(chains, 4);
    why.push(`warm ${t.cpuSpeedLimitPct}%`);
  }
  // Power: don't hammer the battery; this box is meant to live on AC.
  if (t.onACPower === false) {
    chains = Math.min(chains, 3);
    why.push("on battery");
  }
  chains = Math.max(2, Math.min(NOBG_PARALLEL_MAX, chains));
  return { chains, reason: why.join(", ") };
}

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

// Per-image processing failures are deduped over this window so the poison-image
// loop (a few permanently-broken images re-tried every sweep) records each
// broken image once, not on every batch. Codes are string literals on purpose:
// errorCodes.ts is outside the M1 auto-update allowlist, so adding enum members
// there would block this file from auto-deploying.
const PROCESS_FAILURE_DEDUPE_HOURS = 20;

/** R2 keys look like `products/<retailer>/<id>/<file>` — pull the retailer. */
function retailerFromR2Key(key: string): string | null {
  const parts = key.split("/");
  return parts[0] === "products" && parts[1] ? parts[1] : null;
}

/**
 * Record this batch's per-image nobg failures into scrape_errors so they're
 * diagnosable from any machine (the per-image reason otherwise only reaches the
 * M1's local activity log). Deduped against the last ~20h so a permanently
 * broken image doesn't spam the feed every sweep. Returns the count of NEW
 * (not-recently-seen) failures.
 */
async function recordNobgFailures(
  failures: { key: string; reason: string }[],
  jobId: string,
): Promise<number> {
  if (failures.length === 0) return 0;
  let fresh = failures;
  const pg = getPool();
  if (pg) {
    try {
      const { rows } = await pg.query(
        `SELECT DISTINCT url FROM scrape_errors
          WHERE code = 'NOBG_IMAGE_FAILED' AND url = ANY($1::text[])
            AND occurred_at > NOW() - ($2 || ' hours')::interval`,
        [failures.map((f) => f.key), String(PROCESS_FAILURE_DEDUPE_HOURS)],
      );
      const known = new Set(rows.map((r) => r.url as string));
      fresh = failures.filter((f) => !known.has(f.key));
    } catch {
      // Dedupe is best-effort — if the probe fails, record everything.
    }
  }
  for (const f of fresh.slice(0, 25)) {
    await recordScrapeError({
      code: "NOBG_IMAGE_FAILED",
      detail: f.reason,
      retailer: retailerFromR2Key(f.key),
      stage: "process",
      url: f.key,
      jobId,
      metadata: { workerId: WORKER_ID },
    });
  }
  return fresh.length;
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
      const telem = await getMachineTelemetry().catch(() => null);
      const { chains, reason } = governNobgParallel(telem);
      log(
        `nobg capacity governor → ${chains} parallel chains (${reason}; cpu ${telem?.cpuUsagePct ?? "?"}% used, ${telem?.freeMemMb ?? "?"}MB free, thermal ${telem?.cpuSpeedLimitPct ?? "?"}%)`,
      );
      const result = await runNobgBatch({ limit: data.limit, log, parallel: chains });
      log(
        `nobg batch done: processed=${result.processed} failed=${result.failed} remaining≈${result.remaining} hasNobg+=${result.hasNobgUpdated}`,
      );
      if (result.failures.length > 0) {
        const fresh = await recordNobgFailures(result.failures, jobId);
        // Only surface a NEW issue when there are genuinely new failures — a
        // repeating poison image is already on the Errors tab and shouldn't
        // re-alert every sweep.
        if (fresh > 0) {
          recordIssue(
            "nobg",
            `${fresh} new image failure(s) in batch ${jobId} — see Errors tab (scrape_errors, code NOBG_IMAGE_FAILED)`,
          );
        }
      }
      statusJobFinished(jobId, data.kind, `processed ${result.processed}, failed ${result.failed}`);
      // Chain while we're still making progress. `remaining` is the tool's
      // "Found N − limit" estimate (0 once fewer than `limit` candidates are
      // left). If that estimate failed to parse (−1) but the batch DID process
      // images, chain once more and let the next batch discover the true floor —
      // a batch that processes 0 (only quarantined/none left) stops the chain.
      moreRemains = result.processed > 0 && result.remaining !== 0;
    } else if (data.kind === "person") {
      const result = await runPersonScanBatch({ limit: data.limit, log });
      log(`person-scan batch done: stripped=${result.stripped} hidden=${result.hidden}`);
      statusJobFinished(jobId, data.kind, `stripped ${result.stripped}, hidden ${result.hidden}`);
      moreRemains = result.hitLimit;
    } else {
      const result = await runEmbedBatch({ limit: data.limit, log });
      statusJobFinished(jobId, data.kind, `batch complete (limit ${data.limit})`);
      moreRemains = result.hitLimit;
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    statusJobFinished(jobId, data.kind, `FAILED: ${message}`);
    // Mirror the batch-level crash to scrape_errors (with the tool's output tail
    // baked into `message`) so embed/nobg subprocess crashes are diagnosable off
    // the M1, not just in the local issue ring.
    await recordScrapeError({
      code:
        data.kind === "embed"
          ? "EMBED_WORKER_CRASH"
          : data.kind === "person"
            ? "PERSON_SCAN_CRASH"
            : "NOBG_BATCH_CRASH",
      detail: message,
      stage: "process",
      jobId,
      metadata: { workerId: WORKER_ID, kind: data.kind, limit: data.limit },
    }).catch(() => {});
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
    // Also kick a people-photo scan so freshly-uploaded items from a new brand
    // are vetted promptly (the "never added" guard), not only on the 2×/day cron.
    if (WORKER_QUEUES.includes(QUEUES.PROCESS_PERSON)) {
      const personLimit = Math.max(1, parseInt(process.env.PROCESS_PERSON_DEFAULT_LIMIT ?? "500", 10));
      const pid = await enqueueProcessing(
        { kind: "person", limit: personLimit, sweep: true },
        { singletonKey: `person:drain:${hourBucket}` },
      );
      if (pid) console.log(`[worker=${WORKER_ID}] upload queue drained — enqueued person scan ${pid}`);
    }
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
// Re-feed cadence for the self-healing backlog loop. Was 10min — too slack:
// after an embed/upload burst frees a slot, nobg should resume within a couple
// minutes, not sit idle for ten. The enqueue is singleton-guarded so a faster
// tick can't stack duplicate batches.
const IDLE_SWEEP_INTERVAL_MS = Math.max(
  30_000,
  parseInt(process.env.IDLE_SWEEP_INTERVAL_MS ?? "120000", 10),
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
          {
            queue: QUEUES.PROCESS_PERSON,
            kind: "person" as const,
            needs: backlog.needsPerson,
            limit: Math.max(1, parseInt(process.env.PROCESS_PERSON_DEFAULT_LIMIT ?? "500", 10)),
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

// ---------------------------------------------------------------------------
// hasNobg ⇄ R2 reconciliation loop — the safety net populate-has-nobg never had
// ---------------------------------------------------------------------------
// The live per-image flip (setHasNobgForKeys) can't see items already done in R2
// but stale in the DB (re-crawls, swallowed writes, the migration default) — the
// tool skips them as "done" so they're never processed → never flipped →
// permanent phantom backlog. This loop heals it from R2 truth automatically: a
// cheap FORWARD pass (only hasNobg=false rows) every RECONCILE_INTERVAL_MS, and a
// FULL bidirectional pass (also demotes rows whose -nobg.png 404s) every
// RECONCILE_FULL_EVERY ticks. HEAD-only, so it's light enough to run often.
const RECONCILE_INTERVAL_MS = Math.max(
  10 * 60_000,
  parseInt(process.env.RECONCILE_INTERVAL_MS ?? String(2 * 60 * 60_000), 10), // default 2h
);
const RECONCILE_FULL_EVERY = Math.max(1, parseInt(process.env.RECONCILE_FULL_EVERY ?? "6", 10)); // full ~every 12h
let reconcileTick = 0;

async function reconcileLoop(): Promise<void> {
  await new Promise((r) => setTimeout(r, 3 * 60_000)); // let boss/heartbeat settle first
  while (!stopping) {
    try {
      // Respect the kill switch + processing gate, like the sweeps do.
      if ((await isAutoPipelineEnabled()) && (await getSetting<boolean>("gate_processing", true))) {
        const mode = reconcileTick % RECONCILE_FULL_EVERY === 0 ? "full" : "forward"; // tick 0 = full
        reconcileTick++;
        await reconcileHasNobg({
          mode,
          log: (line) => {
            console.log(`[worker=${WORKER_ID}] ${line}`);
            recordActivity(line);
          },
        });
      }
    } catch (err) {
      console.error(`[worker=${WORKER_ID}] reconcile loop failed:`, err);
    }
    await new Promise((r) => setTimeout(r, RECONCILE_INTERVAL_MS));
  }
}

// Surface "running on battery" at most every 30 min — on battery the Mac
// sleeps on lid-close (worker suspends → multi-hour idle gaps) and the capacity
// governor throttles, so this is usually the real reason throughput stalls.
let lastBatteryWarnAt = 0;

async function heartbeatLoop(): Promise<void> {
  while (!stopping) {
    try {
      // Machine telemetry rides the heartbeat so the CLOUD dashboard can show
      // the home server's memory/disk/thermal/power from anywhere.
      const telemetry = await getHeartbeatTelemetry().catch(() => null);
      const power = telemetry as { onACPower?: boolean; batteryPct?: number } | null;
      if (power && power.onACPower === false && Date.now() - lastBatteryWarnAt > 30 * 60_000) {
        lastBatteryWarnAt = Date.now();
        recordIssue(
          "power",
          `Home server is on BATTERY (${power.batteryPct ?? "?"}%), not AC. Plug it in: on battery the Mac sleeps on lid-close (worker suspends) and processing is throttled — the main cause of multi-hour idle gaps in nobg/embed.`,
        );
      }
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

  // Mirror every local issue into the shared DB (worker_issues) so they're
  // visible on the cloud dashboard, not just on this Mac's localhost:4577.
  // Fire-and-forget; recordWorkerIssue swallows its own DB errors.
  setIssueSink(({ workerId, source, message }) => {
    void recordWorkerIssue({ workerId: workerId || WORKER_ID, source, message });
  });

  await ensurePipelinePersistenceSchema();
  const boss = await getBoss();
  if (!boss) {
    console.error(`[worker=${WORKER_ID}] DATABASE_URL not set — refusing to start.`);
    process.exit(1);
  }

  // Heartbeat in the background.
  void heartbeatLoop();

  // Self-feeding backlog drain + hasNobg⇄R2 reconciliation (processing-capable
  // workers only — they have the R2 access these need).
  if (
    WORKER_QUEUES.includes(QUEUES.PROCESS_NOBG) ||
    WORKER_QUEUES.includes(QUEUES.PROCESS_EMBED) ||
    WORKER_QUEUES.includes(QUEUES.PROCESS_PERSON)
  ) {
    void idleBacklogLoop();
    void reconcileLoop();
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
  for (const queue of [QUEUES.PROCESS_NOBG, QUEUES.PROCESS_EMBED, QUEUES.PROCESS_PERSON] as const) {
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
