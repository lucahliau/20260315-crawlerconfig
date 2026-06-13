import "dotenv/config";
import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { exploreRetailer, type ExploreFailureError } from "./explore.js";
import { crawlProductUrls, type CrawlResult } from "./crawl.js";
import { uploadRetailer, type UploadResult } from "./upload.js";
import {
  discoverBrands,
  normalizeDiscoverCategory,
  normalizeUrl,
  syncConfigsToMasterList,
  bootstrapBrandsFromJson,
  listBrands,
  isPipelineEligible,
  type DiscoveredBrand,
  type DiscoverBrandsOptions,
} from "./discoverBrands.js";
import { bootstrapLeadsFromJson } from "./brandSources.js";
import { writeJsonAtomic } from "./jsonFs.js";
import { retailerSlugFromUrl } from "./retailerSlug.js";
import { recordDiscoverUsage, recordExploreUsage, getCostMetrics } from "./usageLedger.js";
import { classifyExploreFailure, type ExploreFailureInfo } from "./stagehandConfig.js";
import {
  appendPipelineEvent,
  createPipelineJob,
  ensurePipelinePersistenceSchema,
  getPipelineJob,
  getPool,
  getRetailerPipelineState,
  getScrapeErrorCounts,
  getSetting,
  getSuccessfulUploadUrls,
  isAutoPipelineEnabled,
  listRecentScrapeErrors,
  listRecentWorkerIssues,
  listRetailerPipelineStates,
  listPipelineEvents,
  listWorkerHeartbeats,
  markRunningJobsInterrupted,
  recordScrapeError,
  recordUploadUrlResult,
  savePipelineJob,
  upsertRetailerPipelineState,
  type PipelineJobRecord,
} from "./pipelineStore.js";
import { ErrorCodes, classifyError, type ErrorCode } from "./errorCodes.js";
import {
  enqueueUploadUrls,
  getBoss,
  getQueueStats,
  listDeadLetter,
  QUEUES,
  retryDeadLetterJob,
  stopBoss,
  type PipelineSweepJobData,
} from "./queue.js";
import { safeParseConfig } from "./schemas/config.js";
import { createBrandsRouter } from "./routes/brands.js";
import { createOpsRouter } from "./routes/ops.js";
import { createAnalyticsRouter } from "./routes/analytics.js";
import { installDashboardAuth } from "./dashboardAuth.js";
import { configureAutoChain, maybeChain } from "./autoChain.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json());
// Everything below — legacy UI, /app dashboard, /swipe, all /api routes — is
// behind the dashboard password when DASHBOARD_PASSWORD is set (Railway).
installDashboardAuth(app);
app.use(createOpsRouter());
app.use(createAnalyticsRouter());
app.use(
  createBrandsRouter({
    // Approve → config, hands-free, when it's free: every approved brand gets
    // an explore over the $0 rungs only (Shopify → WooCommerce → sitemap) via
    // the normal explore-job machinery, so state persists like any explore.
    // Sites needing the AI rungs fail as `needs_paid_explore` and land in the
    // Configure retry queue for a manual (paid) Identify.
    onBrandApproved: (url) => {
      void (async () => {
        try {
          const clean = cleanUrl(url);
          if (!clean) return;
          const slug = retailerSlugFromUrl(clean);
          const config = await loadStoredConfig(slug);
          const state = await getRetailerPipelineState(slug);
          const status = getExploreStatus(state?.exploreState, !!config, hasConfigError(config));
          if (shouldSkipExistingExplore(status)) return;
          const { jobId } = await createExploreJob([clean], 1, undefined, { freeRungsOnly: true });
          console.log(`[auto-explore] ${slug}: free-rungs explore job ${jobId} started.`);
        } catch (err) {
          console.error("[auto-explore] Failed:", err);
        }
      })();
    },
  }),
);

const PORT = parseInt(process.env.PORT ?? "3456", 10);
const CONFIGS_DIR = process.env.CONFIGS_DIR ?? path.join(process.cwd(), "configs");
const PRODUCT_URLS_DIR = process.env.PRODUCT_URLS_DIR ?? path.join(process.cwd(), "product-urls");
const UPLOAD_STATUS_DIR = process.env.UPLOAD_STATUS_DIR ?? path.join(process.cwd(), "upload-status");
const DISCOVERED_BRANDS_PATH =
  process.env.DISCOVERED_BRANDS_PATH ?? path.join(process.cwd(), "discovered-brands.json");
const JOB_LOGS_DIR = process.env.JOB_LOGS_DIR ?? path.join(process.cwd(), "logs");
const JOB_LOG_MAX_LINES = parseInt(process.env.JOB_LOG_MAX_LINES ?? "5000", 10);

/** SSE comment pings so proxies and browsers do not treat the stream as idle (0 = disabled). */
const SSE_HEARTBEAT_MS = parseInt(process.env.SSE_HEARTBEAT_MS ?? "20000", 10);

// Delay between processing consecutive URLs (helps with rate limits)
const INTER_URL_DELAY_MS = parseInt(process.env.INTER_URL_DELAY_MS ?? "5000", 10);
const MIN_EXPLORE_PARALLELISM = 1;
const MAX_EXPLORE_PARALLELISM = 10;
const EXPLORE_RETRY_ATTEMPTS = parseInt(process.env.EXPLORE_RETRY_ATTEMPTS ?? "2", 10);
const EXPLORE_RETRY_DELAY_MS = parseInt(process.env.EXPLORE_RETRY_DELAY_MS ?? "30000", 10);

// ---------------------------------------------------------------------------
// Job tracking and live progress
// ---------------------------------------------------------------------------

type Job = PipelineJobRecord;
type RecommendationValue = "recommended" | "usable" | "not recommended" | "unknown";
type ExploreUiStatus = "idle" | "running" | "completed" | "queued_retry" | "needs_retry" | "failed";

interface ExploreAttemptSuccess {
  ok: true;
  config: Record<string, unknown>;
  metrics: Awaited<ReturnType<typeof exploreRetailer>>["metrics"];
  estimatedUsd: number;
  attemptCount: number;
  retailer: string;
  recommendation: RecommendationValue;
}

interface ExploreAttemptFailure {
  ok: false;
  error: Error;
  failureInfo: ExploreFailureInfo;
  attemptCount: number;
  retailer: string;
  status: Extract<ExploreUiStatus, "needs_retry" | "failed">;
}

const jobs = new Map<string, Job>();
const sseClients = new Map<string, Set<express.Response>>();
const sseHeartbeatIntervals = new Map<express.Response, ReturnType<typeof setInterval>>();

function registerSseHeartbeat(res: express.Response) {
  if (SSE_HEARTBEAT_MS <= 0) return;
  const id = setInterval(() => {
    try {
      res.write(`: ping\n\n`);
    } catch {
      clearSseHeartbeat(res);
    }
  }, SSE_HEARTBEAT_MS);
  sseHeartbeatIntervals.set(res, id);
}

function clearSseHeartbeat(res: express.Response) {
  const id = sseHeartbeatIntervals.get(res);
  if (id !== undefined) {
    clearInterval(id);
    sseHeartbeatIntervals.delete(res);
  }
}

function closeAllSseClients(jobId: string) {
  const clients = sseClients.get(jobId);
  if (!clients) return;
  for (const res of clients) {
    clearSseHeartbeat(res);
    res.end();
  }
  sseClients.delete(jobId);
}

function appendJobNdjson(jobId: string, payload: Record<string, unknown>) {
  try {
    fs.mkdirSync(JOB_LOGS_DIR, { recursive: true });
    fs.appendFileSync(
      path.join(JOB_LOGS_DIR, `${jobId}.ndjson`),
      JSON.stringify({ t: new Date().toISOString(), ...payload }) + "\n",
      "utf-8",
    );
  } catch {
    // ignore disk errors
  }
}

function persistJob(job: Job) {
  void savePipelineJob(job).catch((err) => {
    console.error(`[job=${job.id}] Failed to persist job snapshot:`, err);
  });
}

function buildUploadStatusPayload(
  retailer: string,
  crawlSourceCrawledAt: string,
  result: UploadResult,
  extra?: Record<string, unknown>,
): Record<string, unknown> {
  return {
    retailer,
    uploadedAt: result.uploadedAt,
    uploaded: result.uploaded,
    skipped: result.skipped,
    failed: result.failed,
    total: result.total,
    crawlSourceCrawledAt,
    ...(extra ?? {}),
  };
}

function writeUploadStatusSnapshot(retailer: string, payload: Record<string, unknown>) {
  fs.mkdirSync(UPLOAD_STATUS_DIR, { recursive: true });
  writeJsonAtomic(path.join(UPLOAD_STATUS_DIR, `${retailer}.json`), payload);
}

/** @param err Optional error — stack is appended to the log line and NDJSON record */
function pushLog(jobId: string, msg: string, err?: Error) {
  let full = msg;
  if (err) {
    full += err.stack ? `\n${err.stack}` : `\n${String(err)}`;
  }
  const job = jobs.get(jobId);
  if (job) {
    job.logs.push(full);
    if (job.logs.length > JOB_LOG_MAX_LINES) {
      job.logs.splice(0, job.logs.length - JOB_LOG_MAX_LINES);
    }
    persistJob(job);
  }
  appendJobNdjson(jobId, { type: "log", msg: full });
  void appendPipelineEvent(jobId, { type: "log", msg: full });

  const clients = sseClients.get(jobId);
  if (clients) {
    const data = `data: ${JSON.stringify({ type: "log", msg: full })}\n\n`;
    for (const res of clients) res.write(data);
  }
}

/**
 * Record a structured error into scrape_errors AND surface it in the live job
 * log/SSE feed so the dashboard sees it without polling a separate endpoint.
 * Never throws — error reporting must not break the calling job.
 */
function logScrapeError(
  jobId: string,
  stage: "explore" | "crawl" | "upload" | "discover" | "e2e",
  err: unknown,
  context: { retailer?: string | null; url?: string | null; code?: ErrorCode; attempt?: number; metadata?: Record<string, unknown> } = {},
) {
  const code = context.code ?? classifyError(err, ErrorCodes.UNKNOWN);
  const detail = err instanceof Error ? err.message : String(err ?? "");
  // Persist (fire-and-forget; pipelineStore swallows its own errors).
  void recordScrapeError({
    code,
    detail,
    retailer: context.retailer ?? null,
    stage,
    url: context.url ?? null,
    attempt: context.attempt ?? 1,
    jobId,
    metadata: context.metadata,
  });
  // Mirror to the live log so users see it in the SSE stream too.
  const labelParts = [`[${code}]`];
  if (context.retailer) labelParts.push(context.retailer);
  if (context.url) labelParts.push(context.url);
  const label = labelParts.join(" ");
  const e = err instanceof Error ? err : new Error(detail);
  pushLog(jobId, `${label} ${detail}`, e);
  pushEvent(jobId, {
    type: "scrape-error",
    code,
    stage,
    retailer: context.retailer ?? null,
    url: context.url ?? null,
    detail,
  });
}

function pushEvent(jobId: string, event: Record<string, unknown>) {
  const job = jobs.get(jobId);
  if (job && event.type === "e2e-progress") {
    job.e2eProgress = event;
    persistJob(job);
  }
  if (job && event.type === "progress" && typeof event.current === "number") {
    job.current = Math.max(0, Number(event.current) - 1);
    job.currentUrl = typeof event.url === "string" ? event.url : job.currentUrl;
    persistJob(job);
  }
  void appendPipelineEvent(jobId, event);
  const clients = sseClients.get(jobId);
  if (clients) {
    const data = `data: ${JSON.stringify(event)}\n\n`;
    for (const res of clients) res.write(data);
  }
}

/** Full model text from brand discovery — stored for SSE replay + collapsible UI. */
function pushDiscoverModelResponse(jobId: string, body: string) {
  const job = jobs.get(jobId);
  if (job) {
    job.discoverModelResponse = body;
    persistJob(job);
  }
  appendJobNdjson(jobId, { type: "model-response", phase: "discover", body });
  void appendPipelineEvent(jobId, { type: "model-response", phase: "discover", body });
  const payload = { type: "model-response" as const, phase: "discover" as const, body };
  const clients = sseClients.get(jobId);
  if (clients) {
    const data = `data: ${JSON.stringify(payload)}\n\n`;
    for (const res of clients) res.write(data);
  }
}

function createJobRecord(args: {
  id: string;
  kind: Job["kind"];
  urls?: string[];
  retailer?: string | null;
  meta?: Record<string, unknown>;
}): Job {
  return {
    id: args.id,
    kind: args.kind,
    retailer: args.retailer ?? null,
    urls: args.urls ?? [],
    current: 0,
    status: "running",
    logs: [],
    results: [],
    meta: args.meta ?? {},
  };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampExploreParallelism(raw: unknown): number {
  const parsed =
    typeof raw === "number" ? raw : typeof raw === "string" ? parseInt(raw, 10) : Number.NaN;
  if (!Number.isFinite(parsed)) return MIN_EXPLORE_PARALLELISM;
  return Math.max(MIN_EXPLORE_PARALLELISM, Math.min(MAX_EXPLORE_PARALLELISM, Math.trunc(parsed)));
}

function clampExploreRetryAttempts(raw: unknown): number {
  const parsed =
    typeof raw === "number" ? raw : typeof raw === "string" ? parseInt(raw, 10) : Number.NaN;
  if (!Number.isFinite(parsed)) return Math.max(1, EXPLORE_RETRY_ATTEMPTS);
  return Math.max(1, Math.min(5, Math.trunc(parsed)));
}

function normalizeRecommendationValue(raw: unknown): RecommendationValue {
  if (raw === "recommended" || raw === "usable" || raw === "not recommended") return raw;
  return "unknown";
}

function getExploreFailureInfo(err: unknown): ExploreFailureInfo {
  const tagged = err as ExploreFailureError | undefined;
  if (tagged?.failureInfo) return tagged.failureInfo;
  return classifyExploreFailure(err);
}

function hasConfigError(config: Record<string, unknown> | null | undefined): boolean {
  if (!config || typeof config !== "object") return false;
  const error = config.error;
  if (error && typeof error === "object" && typeof (error as { message?: unknown }).message === "string") {
    return true;
  }
  // Schema-invalid configs (e.g. legacy stubs with `discovery.method: null`)
  // must surface as a failed explore so they end up in the Configure retry
  // queue instead of silently appearing as "ready to crawl".
  return !safeParseConfig(config).success;
}

function isConfigValid(config: Record<string, unknown> | null | undefined): boolean {
  if (!config || typeof config !== "object") return false;
  return safeParseConfig(config).success;
}

function getExploreStatus(
  state: Record<string, unknown> | undefined,
  hasConfig: boolean,
  configHasError = false,
): ExploreUiStatus {
  const status = state?.status;
  // A schema-invalid config always wins over a stored "completed" status —
  // those rows came from the legacy stub-writer and must surface as failed
  // so they end up in the Configure retry queue.
  if (configHasError && status !== "running") return "failed";
  if (
    status === "running" ||
    status === "completed" ||
    status === "queued_retry" ||
    status === "needs_retry" ||
    status === "failed"
  ) {
    return status;
  }
  return hasConfig ? "completed" : "idle";
}

function shouldSkipExistingExplore(status: ExploreUiStatus): boolean {
  return status === "completed" || status === "running";
}

function getEffectiveRecommendation(
  recommendation: RecommendationValue,
  exploreStatus: ExploreUiStatus,
): RecommendationValue {
  if (exploreStatus !== "completed") return "not recommended";
  return recommendation;
}

async function createExploreJob(
  urls: string[],
  parallelism?: number | string,
  skippedUrls?: string[],
  opts?: { freeRungsOnly?: boolean },
): Promise<{ jobId: string; parallelism: number }> {
  const id = crypto.randomUUID();
  const normalizedParallelism = clampExploreParallelism(parallelism);
  const job = createJobRecord({
    id,
    kind: "explore",
    urls,
    meta: { parallelism: normalizedParallelism, ...(opts?.freeRungsOnly ? { freeRungsOnly: true } : {}) },
  });
  jobs.set(id, job);
  await createPipelineJob(job);

  processJob(job, skippedUrls && skippedUrls.length > 0 ? skippedUrls : undefined).catch((err) => {
    job.status = "error";
    job.error = (err as Error).message;
    persistJob(job);
    console.error(`[processJob job=${job.id}] Error:`, err);
  });

  return {
    jobId: id,
    parallelism: normalizedParallelism,
  };
}

async function exploreRetailerWithRecovery(
  job: Job,
  args: { url: string; retailer: string; displayName: string },
): Promise<ExploreAttemptSuccess | ExploreAttemptFailure> {
  const maxAttempts = clampExploreRetryAttempts(job.meta?.exploreRetryAttempts);
  const startedAt = new Date().toISOString();
  let lastError: Error | null = null;
  let lastFailureInfo: ExploreFailureInfo | null = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    await upsertRetailerPipelineState({
      retailer: args.retailer,
      baseUrl: args.url,
      displayName: args.displayName,
      latestJobId: job.id,
      exploreState: {
        status: "running",
        startedAt,
        attempt,
        maxAttempts,
        retryable: false,
        nextRetryAt: null,
        failedAt: null,
        error: null,
        failureCode: null,
        failureReason: null,
      },
    });

    if (attempt > 1) {
      pushLog(
        job.id,
        `Retrying explore for ${args.displayName} (${attempt}/${maxAttempts}) after a retryable failure.\n`,
      );
    }

    try {
      const { config, metrics, estimatedUsd } = await exploreRetailer(args.url, (msg) => pushLog(job.id, msg), {
        freeRungsOnly: job.meta?.freeRungsOnly === true,
      });
      const retailer = (config.retailer as string) ?? args.retailer;
      const dq = config.dataQuality as Record<string, unknown> | undefined;
      const recommendation = normalizeRecommendationValue(dq?.overallRecommendation);

      await upsertRetailerPipelineState({
        retailer,
        baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : args.url,
        displayName:
          typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : args.displayName,
        latestJobId: job.id,
        exploreState: {
          status: "completed",
          startedAt,
          completedAt: new Date().toISOString(),
          recommendation,
          estimatedUsd,
          configPath: path.join(CONFIGS_DIR, `${retailer}.json`),
          config,
          attempt,
          maxAttempts,
          retryable: false,
          nextRetryAt: null,
          failedAt: null,
          error: null,
          failureCode: null,
          failureReason: null,
        },
      });

      return {
        ok: true,
        config,
        metrics,
        estimatedUsd,
        attemptCount: attempt,
        retailer,
        recommendation,
      };
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      lastFailureInfo = getExploreFailureInfo(lastError);
      const retryable = lastFailureInfo.retryable;
      const hasRetryRemaining = retryable && attempt < maxAttempts;
      const status: ExploreAttemptFailure["status"] = retryable && !hasRetryRemaining ? "needs_retry" : "failed";

      pushLog(
        job.id,
        `${retryable ? "Retryable" : "Terminal"} explore failure for ${args.displayName}: ${lastError.message}\n`,
        lastError,
      );
      logScrapeError(job.id, "explore", lastError, {
        retailer: args.retailer,
        url: args.url,
        code: classifyError(lastError, ErrorCodes.EXPLORE_UNCAUGHT),
        attempt,
        metadata: {
          retryable,
          failureCode: lastFailureInfo?.code,
          failureReason: lastFailureInfo?.reason,
        },
      });

      await upsertRetailerPipelineState({
        retailer: args.retailer,
        baseUrl: args.url,
        displayName: args.displayName,
        latestJobId: job.id,
        exploreState: {
          status: hasRetryRemaining ? "queued_retry" : status,
          startedAt,
          failedAt: new Date().toISOString(),
          attempt,
          maxAttempts,
          retryable,
          error: lastError.message,
          failureCode: lastFailureInfo.code,
          failureReason: lastFailureInfo.reason,
          nextRetryAt: hasRetryRemaining ? new Date(Date.now() + EXPLORE_RETRY_DELAY_MS).toISOString() : null,
        },
      });

      if (!hasRetryRemaining) {
        return {
          ok: false,
          error: lastError,
          failureInfo: lastFailureInfo,
          attemptCount: attempt,
          retailer: args.retailer,
          status,
        };
      }

      pushLog(
        job.id,
        `Queued retry for ${args.displayName} in ${Math.round(EXPLORE_RETRY_DELAY_MS / 1000)}s (${lastFailureInfo.code}).\n`,
      );
      await sleep(EXPLORE_RETRY_DELAY_MS);
    }
  }

  return {
    ok: false,
    error: lastError ?? new Error(`Explore failed for ${args.displayName}.`),
    failureInfo: lastFailureInfo ?? {
      retryable: false,
      code: "unknown",
      reason: "The explore run failed without a classified error.",
    },
    attemptCount: maxAttempts,
    retailer: args.retailer,
    status: lastFailureInfo?.retryable ? "needs_retry" : "failed",
  };
}

async function processExploreUrl(job: Job, url: string) {
  const fallbackRetailer = retailerSlugFromUrl(url);
  const displayName = fallbackRetailer;
  const outcome = await exploreRetailerWithRecovery(job, {
    url,
    retailer: fallbackRetailer,
    displayName,
  });

  if (outcome.ok) {
    const { config, metrics, estimatedUsd } = outcome;
    recordExploreUsage({
      retailer: outcome.retailer,
      estimatedUsd,
      promptTokens: metrics?.totalPromptTokens ?? 0,
      completionTokens: metrics?.totalCompletionTokens ?? 0,
      inferenceTimeMs: metrics?.totalInferenceTimeMs ?? 0,
    });
    job.results.push(config);
    persistJob(job);
    // Conveyor: a good config rolls straight into a crawl (gated + capped).
    void maybeChain("explore-completed", {
      retailer: outcome.retailer,
      recommendation: outcome.recommendation,
      log: (msg) => pushLog(job.id, `${msg}\n`),
    });
    return;
  }

  console.error(`[processJob job=${job.id}] Error processing ${url}:`, outcome.error);
}

async function processJob(job: Job, skippedUrls?: string[]) {
  const parallelism = clampExploreParallelism(job.meta?.parallelism);
  if (skippedUrls && skippedUrls.length > 0) {
    pushLog(
      job.id,
      `Skipped ${skippedUrls.length} URL(s) with existing configs:\n${skippedUrls.join("\n")}\n`,
    );
  }
  if (parallelism > 1) {
    pushLog(job.id, `Running up to ${parallelism} explore tasks in parallel.\n`);
  }

  let completedCount = 0;
  let activeCount = 0;

  for (let batchStart = 0; batchStart < job.urls.length; batchStart += parallelism) {
    if (batchStart > 0 && INTER_URL_DELAY_MS > 0) {
      pushLog(job.id, `\nWaiting ${INTER_URL_DELAY_MS / 1000}s before next URL batch...\n`);
      await sleep(INTER_URL_DELAY_MS);
    }

    const batch = job.urls.slice(batchStart, batchStart + parallelism);
    await Promise.all(
      batch.map(async (url, offset) => {
        const index = batchStart + offset;
        activeCount += 1;
        job.currentUrl = url;
        persistJob(job);
        pushLog(job.id, `\n========== Processing ${index + 1}/${job.urls.length}: ${url} ==========\n`);
        await processExploreUrl(job, url);
        activeCount = Math.max(0, activeCount - 1);
        completedCount += 1;
        job.current = completedCount;
        job.currentUrl = url;
        persistJob(job);
        pushEvent(job.id, {
          type: "progress",
          current: completedCount,
          total: job.urls.length,
          url,
          activeCount,
          parallelism,
        });
      }),
    );
  }

  job.status = "done";
  persistJob(job);
  pushEvent(job.id, { type: "done" });

  closeAllSseClients(job.id);
}

// ---------------------------------------------------------------------------
// API endpoints
// ---------------------------------------------------------------------------

function cleanUrl(raw: string): string | null {
  let s = raw.trim();
  if (!s) return null;
  // Remove wrapping quotes or angle brackets
  s = s.replace(/^["'<]+|["'>]+$/g, "");
  // Fix doubled protocols
  s = s.replace(/^(https?:\/\/)+/i, "https://");
  // Handle protocol-relative URLs
  if (s.startsWith("//")) {
    s = "https:" + s;
  }
  // Add protocol if missing
  if (!/^https?:\/\//i.test(s)) {
    s = "https://" + s;
  }
  try {
    const u = new URL(s);
    // Ensure trailing slash on bare domains
    if (!u.pathname || u.pathname === "/") {
      u.pathname = "/";
    }
    return u.toString();
  } catch {
    return null;
  }
}

app.post("/api/explore", async (req, res) => {
  const { urls, skipExisting, parallelism } = req.body as {
    urls?: string[];
    skipExisting?: boolean;
    parallelism?: number | string;
  };
  if (!urls || !Array.isArray(urls) || urls.length === 0) {
    res.status(400).json({ error: "Provide a non-empty urls array." });
    return;
  }

  const cleanedUrls: string[] = [];
  for (const raw of urls) {
    const url = cleanUrl(String(raw));
    if (url) {
      try {
        new URL(url);
        cleanedUrls.push(url);
      } catch (e) {
        console.error("[api/explore] Invalid URL skipped:", raw, e);
      }
    }
  }
  if (cleanedUrls.length === 0) {
    res.status(400).json({ error: "No valid URLs provided." });
    return;
  }

  const shouldSkipExisting = skipExisting !== false;
  fs.mkdirSync(CONFIGS_DIR, { recursive: true });
  const skippedUrls: string[] = [];
  const toExplore: string[] = [];
  for (const url of cleanedUrls) {
    if (shouldSkipExisting) {
      const slug = retailerSlugFromUrl(url);
      if (fs.existsSync(path.join(CONFIGS_DIR, `${slug}.json`))) {
        const config = await loadStoredConfig(slug);
        const state = await getRetailerPipelineState(slug);
        const status = getExploreStatus(state?.exploreState, true, hasConfigError(config));
        if (shouldSkipExistingExplore(status)) {
          skippedUrls.push(url);
          continue;
        }
      }
    }
    toExplore.push(url);
  }

  if (toExplore.length === 0) {
    res.status(400).json({
      error: "All URLs already have completed configs or are currently running. Clear skip to force re-explore.",
      skippedUrls,
    });
    return;
  }

  const jobInfo = await createExploreJob(toExplore, parallelism, skippedUrls);

  res.json({
    jobId: jobInfo.jobId,
    skippedUrls,
    skippedCount: skippedUrls.length,
    parallelism: jobInfo.parallelism,
  });
});

app.post("/api/explore/retry", async (req, res) => {
  const { retailers, parallelism } = req.body as {
    retailers?: string[];
    parallelism?: number | string;
  };
  if (!retailers || !Array.isArray(retailers) || retailers.length === 0) {
    res.status(400).json({ error: "Provide a non-empty retailers array." });
    return;
  }

  const uniqueRetailers = [...new Set(retailers.map((retailer) => String(retailer || "").trim()).filter(Boolean))];
  const toExplore: string[] = [];
  const acceptedRetailers: string[] = [];
  const skippedRetailers: Array<{ retailer: string; reason: string }> = [];

  for (const retailer of uniqueRetailers) {
    const config = await loadStoredConfig(retailer);
    const state = await getRetailerPipelineState(retailer);
    const status = getExploreStatus(state?.exploreState, !!config, hasConfigError(config));
    if (status === "completed") {
      skippedRetailers.push({ retailer, reason: "Explore is already completed." });
      continue;
    }
    if (status === "running") {
      skippedRetailers.push({ retailer, reason: "Explore is already running." });
      continue;
    }

    const rawBaseUrl =
      typeof config?.baseUrl === "string"
        ? config.baseUrl
        : typeof state?.baseUrl === "string"
          ? state.baseUrl
          : "";
    const cleanBaseUrl = rawBaseUrl ? cleanUrl(rawBaseUrl) : null;
    if (!cleanBaseUrl) {
      skippedRetailers.push({ retailer, reason: "No base URL is available to retry." });
      continue;
    }

    acceptedRetailers.push(retailer);
    toExplore.push(cleanBaseUrl);
  }

  if (toExplore.length === 0) {
    res.status(400).json({
      error: "No failed or non-completed retailers could be retried.",
      skippedRetailers,
    });
    return;
  }

  const jobInfo = await createExploreJob(toExplore, parallelism);
  res.json({
    jobId: jobInfo.jobId,
    parallelism: jobInfo.parallelism,
    retailers: acceptedRetailers,
    submittedCount: acceptedRetailers.length,
    skippedRetailers,
    skippedCount: skippedRetailers.length,
  });
});

app.post("/api/discover-brands", async (req, res) => {
  const id = crypto.randomUUID();
  const cat = normalizeDiscoverCategory(req.body?.category);
  const discoverOptions: DiscoverBrandsOptions | undefined =
    cat !== undefined ? { category: cat } : undefined;
  const job = createJobRecord({
    id,
    kind: "discover",
    urls: [],
    meta: discoverOptions ? { category: discoverOptions.category } : {},
  });
  jobs.set(id, job);
  await createPipelineJob(job);

  (async () => {
    try {
      const result = await discoverBrands(
        (msg) => {
          pushLog(job.id, msg);
          pushEvent(job.id, { type: "progress", message: msg });
        },
        (text) => pushDiscoverModelResponse(job.id, text),
        discoverOptions,
      );
      recordDiscoverUsage({
        estimatedUsd: result.estimatedUsd,
        inputTokens: result.usage.input_tokens,
        outputTokens: result.usage.output_tokens,
        webSearchRequests: result.usage.web_search_requests,
      });
      await Promise.all(
        result.brands.map((brand) =>
          upsertRetailerPipelineState({
            retailer: retailerSlugFromUrl(brand.url),
            baseUrl: brand.url,
            displayName: brand.name,
            latestJobId: job.id,
            discoveryState: {
              status: "completed",
              discoveredAt: new Date().toISOString(),
              source: "discover-brands",
            },
          }),
        ),
      );
      job.discoveredBrands = result.brands;
      job.status = "done";
      persistJob(job);
      pushEvent(job.id, { type: "brands", brands: result.brands });
      pushEvent(job.id, { type: "done", brands: result.brands });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[discover-brands job=${job.id}] Error:`, err);
      logScrapeError(job.id, "discover", e, {
        code: classifyError(e, ErrorCodes.DISCOVER_UNCAUGHT),
      });
      job.status = "error";
      job.error = e.message;
      persistJob(job);
      pushEvent(job.id, { type: "done", error: e.message });
    }

    closeAllSseClients(job.id);
  })();

  res.json({ jobId: id });
});

// ---------------------------------------------------------------------------
// End-to-end orchestrator
// ---------------------------------------------------------------------------

const E2E_CONCURRENCY = parseInt(process.env.E2E_CONCURRENCY ?? "4", 10);

async function runWithConcurrencyLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += limit) {
    const chunk = items.slice(i, i + limit);
    const chunkResults = await Promise.all(chunk.map(fn));
    results.push(...chunkResults);
  }
  return results;
}

interface E2EProgress {
  type: "e2e-progress";
  step: 1 | 2 | 3 | 4;
  stepLabel: string;
  brandsDiscovered: number;
  configsTotal: number;
  configsSuccessful: number;
  configsRecommended: number;
  productUrlsTotal: number;
  productUrlsCrawled: number;
  uploadedTotal: number;
  uploadedSuccess: number;
  retailersUploadComplete?: number;
  retailersUploadTotal?: number;
  currentBrand?: string;
  percentComplete: number;
  etaSeconds?: number;
  startedAt: string;
}

function extractRetailerFromUrl(url: string): string {
  try {
    const hostname = new URL(url).hostname;
    return hostname.replace(/^www\./, "").split(".")[0];
  } catch {
    return "unknown";
  }
}

function readJsonFile<T>(filePath: string): T | null {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, "utf-8")) as T;
  } catch {
    return null;
  }
}

function summarizeConfigValidationIssues(
  issues: Array<{ path: (string | number)[]; message: string }>,
): string {
  return issues
    .map((issue) => `${issue.path.length ? issue.path.join(".") : "config"}: ${issue.message}`)
    .join("; ");
}

async function loadStoredConfig(retailer: string): Promise<Record<string, unknown> | null> {
  const filePath = path.join(CONFIGS_DIR, `${retailer}.json`);
  const fromFile = readJsonFile<Record<string, unknown>>(filePath);
  let config: Record<string, unknown> | null = null;
  if (fromFile) {
    config = fromFile;
  } else {
    const state = await getRetailerPipelineState(retailer);
    const stored = state?.exploreState?.config;
    config = stored && typeof stored === "object" ? (stored as Record<string, unknown>) : null;
  }
  if (!config) return null;
  // Validation parity with the worker (worker.ts loadConfigFor runs
  // safeParseConfig). An invalid/stub config (e.g. legacy `discovery.method:
  // null`) must NOT pass the crawl/upload-enqueue gates — otherwise it slips
  // through and floods the worker with jobs that can never process. Reject it
  // here so the retailer lands in the Configure retry queue instead.
  if (hasConfigError(config)) {
    console.warn(`[config] stored config for "${retailer}" failed validation — treating as missing.`);
    return null;
  }
  return config;
}

async function loadStoredCrawlResult(retailer: string): Promise<CrawlResult | null> {
  const filePath = path.join(PRODUCT_URLS_DIR, `${retailer}.json`);
  const fromFile = readJsonFile<CrawlResult>(filePath);
  if (fromFile) return fromFile;
  const state = await getRetailerPipelineState(retailer);
  const crawlState = state?.crawlState;
  if (!crawlState) return null;
  const urls = Array.isArray(crawlState.urls) ? (crawlState.urls as string[]) : [];
  const totalUrls =
    typeof crawlState.totalUrls === "number" ? crawlState.totalUrls : urls.length;
  const crawledAt =
    typeof crawlState.crawledAt === "string" ? crawlState.crawledAt : new Date().toISOString();
  const method = typeof crawlState.method === "string" ? crawlState.method : "unknown";
  return {
    retailer,
    crawledAt,
    method,
    totalUrls,
    urls,
    completed: crawlState.status === "completed",
    error: typeof crawlState.error === "string" ? crawlState.error : undefined,
    resumedFromCheckpoint: !!crawlState.resumedFromCheckpoint,
  };
}

async function loadStoredUploadStatus(retailer: string): Promise<Record<string, unknown> | null> {
  const filePath = path.join(UPLOAD_STATUS_DIR, `${retailer}.json`);
  const fromFile = readJsonFile<Record<string, unknown>>(filePath);
  if (fromFile) return fromFile;
  const state = await getRetailerPipelineState(retailer);
  const uploadState = state?.uploadState;
  return uploadState && typeof uploadState === "object"
    ? ({
        retailer,
        ...uploadState,
      } as Record<string, unknown>)
    : null;
}

async function runE2EOrchestrator(jobId: string, discoverOptions?: DiscoverBrandsOptions) {
  const startedAt = new Date().toISOString();
  const job = jobs.get(jobId);
  if (!job) return;

  const pushProgress = (overrides: Partial<E2EProgress>) => {
    const event: E2EProgress = {
      type: "e2e-progress",
      step: 1,
      stepLabel: "Discovering brands",
      brandsDiscovered: 0,
      configsTotal: 0,
      configsSuccessful: 0,
      configsRecommended: 0,
      productUrlsTotal: 0,
      productUrlsCrawled: 0,
      uploadedTotal: 0,
      uploadedSuccess: 0,
      percentComplete: 0,
      startedAt,
      ...overrides,
    };
    pushEvent(jobId, event as unknown as Record<string, unknown>);
  };

  try {
    // Step 1: Discover brands
    pushProgress({ step: 1, stepLabel: "Discovering brands", percentComplete: 5 });
    pushLog(jobId, "\n========== Step 1: Discovering brands (Gemini + Google Search) ==========\n");

    const discoverResult = await discoverBrands(
      (msg) => pushLog(jobId, msg),
      (text) => pushDiscoverModelResponse(jobId, text),
      discoverOptions,
    );
    recordDiscoverUsage({
      estimatedUsd: discoverResult.estimatedUsd,
      inputTokens: discoverResult.usage.input_tokens,
      outputTokens: discoverResult.usage.output_tokens,
      webSearchRequests: discoverResult.usage.web_search_requests,
      e2eJobId: jobId,
    });
    const brands = discoverResult.brands;
    if (!brands || brands.length === 0) {
      throw new Error("No brands discovered.");
    }
    await Promise.all(
      brands.map((brand) =>
        upsertRetailerPipelineState({
          retailer: retailerSlugFromUrl(brand.url),
          baseUrl: brand.url,
          displayName: brand.name,
          latestJobId: jobId,
          discoveryState: {
            status: "completed",
            discoveredAt: new Date().toISOString(),
            source: "e2e",
          },
        }),
      ),
    );
    job.discoveredBrands = brands;
    persistJob(job);

    pushProgress({
      step: 1,
      stepLabel: "Discovering brands",
      brandsDiscovered: brands.length,
      percentComplete: 15,
    });
    pushLog(jobId, `Discovered ${brands.length} brands.\n`);

    // Step 2: Config exploration (sequential)
    pushProgress({
      step: 2,
      stepLabel: "Generating configs",
      brandsDiscovered: brands.length,
      configsTotal: brands.length,
      percentComplete: 20,
    });
    pushLog(jobId, "\n========== Step 2: Generating configs (sequential) ==========\n");

    const recommendedConfigs: { retailer: string; config: Record<string, unknown> }[] = [];
    let configsSuccessful = 0;

    for (let i = 0; i < brands.length; i++) {
      const brand = brands[i];
      const url = brand.url;
      const fallbackRetailer = retailerSlugFromUrl(url);
      pushProgress({
        step: 2,
        stepLabel: "Generating configs",
        brandsDiscovered: brands.length,
        configsTotal: brands.length,
        configsSuccessful,
        configsRecommended: recommendedConfigs.length,
        currentBrand: brand.name,
        percentComplete: 20 + Math.floor((i / brands.length) * 35),
      });

      // Resume / cache the explore stage: if this brand already has a completed
      // config (from a prior run that crashed or was redeployed mid-pipeline),
      // reuse it instead of re-spending Gemini + Stagehand credits.
      const existingConfig = await loadStoredConfig(fallbackRetailer);
      if (existingConfig) {
        const existingState = await getRetailerPipelineState(fallbackRetailer);
        const existingStatus = getExploreStatus(
          existingState?.exploreState,
          true,
          hasConfigError(existingConfig),
        );
        if (existingStatus === "completed") {
          configsSuccessful++;
          const rec = normalizeRecommendationValue(
            (existingConfig.dataQuality as Record<string, unknown> | undefined)?.overallRecommendation,
          );
          const retailer =
            typeof existingConfig.retailer === "string" ? existingConfig.retailer : fallbackRetailer;
          if (rec === "recommended") {
            recommendedConfigs.push({ retailer, config: existingConfig });
          }
          pushLog(jobId, `  Config for ${brand.name}: ${rec} (resumed from existing config — no AI spend)\n`);
          continue;
        }
      }

      const outcome = await exploreRetailerWithRecovery(job, {
        url,
        retailer: fallbackRetailer,
        displayName: brand.name,
      });

      if (outcome.ok) {
        const { config, metrics, estimatedUsd } = outcome;
        recordExploreUsage({
          retailer: outcome.retailer,
          estimatedUsd,
          promptTokens: metrics?.totalPromptTokens ?? 0,
          completionTokens: metrics?.totalCompletionTokens ?? 0,
          inferenceTimeMs: metrics?.totalInferenceTimeMs ?? 0,
          e2eJobId: jobId,
        });
        configsSuccessful++;
        const retailer = outcome.retailer;
        const dq = config.dataQuality as Record<string, unknown> | undefined;
        const rec = normalizeRecommendationValue(dq?.overallRecommendation);
        await upsertRetailerPipelineState({
          retailer,
          baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : url,
          displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : brand.name,
          latestJobId: jobId,
          exploreState: {
            status: "completed",
            completedAt: new Date().toISOString(),
            recommendation: rec,
            estimatedUsd,
            configPath: path.join(CONFIGS_DIR, `${retailer}.json`),
            config,
            attempt: outcome.attemptCount,
          },
        });
        if (rec === "recommended") {
          recommendedConfigs.push({ retailer, config });
        }
        pushLog(jobId, `  Config for ${brand.name}: ${rec}\n`);
      }

      if (i < brands.length - 1 && INTER_URL_DELAY_MS > 0) {
        pushLog(jobId, `Waiting ${INTER_URL_DELAY_MS / 1000}s before next URL...\n`);
        await new Promise((r) => setTimeout(r, INTER_URL_DELAY_MS));
      }
    }

    pushProgress({
      step: 2,
      stepLabel: "Generating configs",
      brandsDiscovered: brands.length,
      configsTotal: brands.length,
      configsSuccessful,
      configsRecommended: recommendedConfigs.length,
      percentComplete: 55,
    });
    pushLog(jobId, `Config step done: ${configsSuccessful}/${brands.length} successful, ${recommendedConfigs.length} recommended.\n`);

    if (recommendedConfigs.length === 0) {
      pushLog(jobId, "No recommended configs — skipping crawl and upload.\n");
      job.status = "done";
      persistJob(job);
      pushProgress({
        step: 4,
        stepLabel: "Complete",
        brandsDiscovered: brands.length,
        configsTotal: brands.length,
        configsSuccessful,
        configsRecommended: 0,
        percentComplete: 100,
      });
      pushEvent(jobId, { type: "done" });
      closeAllSseClients(jobId);
      return;
    }

    // Step 3: Crawl product URLs (parallel)
    pushProgress({
      step: 3,
      stepLabel: "Crawling product URLs",
      brandsDiscovered: brands.length,
      configsTotal: brands.length,
      configsSuccessful,
      configsRecommended: recommendedConfigs.length,
      percentComplete: 60,
    });
    pushLog(jobId, "\n========== Step 3: Crawling product URLs (parallel) ==========\n");

    const crawlResults: { retailer: string; config: Record<string, unknown>; urls: string[] }[] = [];
    let productUrlsTotal = 0;

    await runWithConcurrencyLimit(recommendedConfigs, E2E_CONCURRENCY, async ({ retailer, config }) => {
      try {
        const parsedConfig = safeParseConfig(config);
        if (!parsedConfig.success) {
          const details = summarizeConfigValidationIssues(parsedConfig.error.issues);
          pushLog(jobId, `Skipping crawl for ${retailer}: stored config is invalid (${details}).\n`);
          await upsertRetailerPipelineState({
            retailer,
            baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : undefined,
            displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : retailer,
            latestJobId: jobId,
            crawlState: {
              status: "failed",
              failedAt: new Date().toISOString(),
              error: `Stored config is invalid: ${details}`,
            },
          });
          return null;
        }
        const crawlConfig = parsedConfig.data;
        const existingState = await getRetailerPipelineState(retailer);
        const initialUrls = Array.isArray(existingState?.crawlState?.urls)
          ? (existingState?.crawlState?.urls as string[])
          : [];
        await upsertRetailerPipelineState({
          retailer,
          baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : undefined,
          displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : retailer,
          latestJobId: jobId,
          crawlState: {
            status: "running",
            startedAt: new Date().toISOString(),
            method: crawlConfig.discovery.method,
            resumedFromCheckpoint: initialUrls.length > 0,
          },
        });
        const result = await crawlProductUrls(
          crawlConfig,
          (msg) => pushLog(jobId, msg),
          undefined,
          {
            initialUrls,
            onCheckpoint: async (partial) => {
              await upsertRetailerPipelineState({
                retailer,
                latestJobId: jobId,
                crawlState: {
                  status: "running",
                  lastCheckpointAt: partial.crawledAt,
                  totalUrls: partial.totalUrls,
                  method: partial.method,
                  urls: partial.urls,
                  resumable: true,
                  resumedFromCheckpoint: partial.resumedFromCheckpoint,
                },
              });
            },
            onComplete: async (finalResult) => {
              await upsertRetailerPipelineState({
                retailer,
                latestJobId: jobId,
                crawlState: {
                  status: "completed",
                  completedAt: finalResult.crawledAt,
                  totalUrls: finalResult.totalUrls,
                  method: finalResult.method,
                  crawledAt: finalResult.crawledAt,
                  urls: finalResult.urls,
                  resumedFromCheckpoint: finalResult.resumedFromCheckpoint,
                },
              });
            },
            onError: async (partial, error) => {
              logScrapeError(jobId, "crawl", error, {
                retailer,
                code: classifyError(error, ErrorCodes.CRAWL_UNCAUGHT),
                metadata: { method: partial.method, totalUrls: partial.totalUrls, recoverable: true },
              });
              await upsertRetailerPipelineState({
                retailer,
                latestJobId: jobId,
                crawlState: {
                  status: "failed",
                  failedAt: new Date().toISOString(),
                  totalUrls: partial.totalUrls,
                  method: partial.method,
                  crawledAt: partial.crawledAt,
                  urls: partial.urls,
                  error: error.message,
                  errorCode: classifyError(error, ErrorCodes.CRAWL_UNCAUGHT),
                  resumable: true,
                  resumedFromCheckpoint: partial.resumedFromCheckpoint,
                },
              });
            },
          },
        );
        fs.mkdirSync(PRODUCT_URLS_DIR, { recursive: true });
        writeJsonAtomic(path.join(PRODUCT_URLS_DIR, `${retailer}.json`), result);
        productUrlsTotal += result.totalUrls;
        crawlResults.push({ retailer, config, urls: result.urls });
        if (!result.completed) {
          pushLog(jobId, `Crawl recovered partial URLs for ${retailer}: ${result.error ?? "unknown error"}\n`);
        }
        pushEvent(jobId, {
          type: "e2e-progress",
          step: 3,
          stepLabel: "Crawling product URLs",
          brandsDiscovered: brands.length,
          configsTotal: brands.length,
          configsSuccessful,
          configsRecommended: recommendedConfigs.length,
          productUrlsTotal,
          productUrlsCrawled: crawlResults.reduce((s, r) => s + r.urls.length, 0),
          uploadedTotal: 0,
          uploadedSuccess: 0,
          percentComplete: 60 + Math.floor((crawlResults.length / recommendedConfigs.length) * 20),
          startedAt,
        });
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        logScrapeError(jobId, "crawl", e, {
          retailer,
          code: classifyError(e, ErrorCodes.CRAWL_UNCAUGHT),
        });
        await upsertRetailerPipelineState({
          retailer,
          latestJobId: jobId,
          crawlState: {
            status: "failed",
            failedAt: new Date().toISOString(),
            error: e.message,
            errorCode: classifyError(e, ErrorCodes.CRAWL_UNCAUGHT),
            resumable: true,
          },
        });
        return null;
      }
    });

    const totalCrawled = crawlResults.reduce((s, r) => s + r.urls.length, 0);
    pushProgress({
      step: 3,
      stepLabel: "Crawling product URLs",
      brandsDiscovered: brands.length,
      configsTotal: brands.length,
      configsSuccessful,
      configsRecommended: recommendedConfigs.length,
      productUrlsTotal,
      productUrlsCrawled: totalCrawled,
      uploadedTotal: 0,
      uploadedSuccess: 0,
      percentComplete: 80,
    });
    pushLog(jobId, `Crawl done: ${totalCrawled} product URLs across ${crawlResults.length} brands.\n`);

    // Step 4: Upload (parallel)
    pushProgress({
      step: 4,
      stepLabel: "Extracting & uploading",
      brandsDiscovered: brands.length,
      configsTotal: brands.length,
      configsSuccessful,
      configsRecommended: recommendedConfigs.length,
      productUrlsTotal,
      productUrlsCrawled: totalCrawled,
      uploadedTotal: crawlResults.reduce((s, r) => s + r.urls.length, 0),
      uploadedSuccess: 0,
      percentComplete: 82,
    });
    pushLog(jobId, "\n========== Step 4: Extracting & uploading (parallel) ==========\n");

    const uploadState = { success: 0, retailersDone: 0 };
    const toUpload = crawlResults.filter((r) => r.urls.length > 0);
    const totalUrlsToUpload = toUpload.reduce((s, r) => s + r.urls.length, 0);
    const nUpload = toUpload.length;

    await runWithConcurrencyLimit(toUpload, E2E_CONCURRENCY, async ({ retailer, config, urls }) => {
      try {
        const crawlResult = await loadStoredCrawlResult(retailer);
        const crawlSourceCrawledAt = crawlResult?.crawledAt ?? new Date().toISOString();
        const resumeUploadedUrls = await getSuccessfulUploadUrls(retailer, crawlSourceCrawledAt);
        await upsertRetailerPipelineState({
          retailer,
          baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : undefined,
          displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : retailer,
          latestJobId: jobId,
          uploadState: {
            status: "running",
            startedAt: new Date().toISOString(),
            total: urls.length,
            uploaded: 0,
            skipped: resumeUploadedUrls.size,
            failed: 0,
            crawlSourceCrawledAt,
          },
        });
        const result = await uploadRetailer(
          config as Parameters<typeof uploadRetailer>[0],
          urls,
          (msg) => pushLog(jobId, msg),
          async (progress) => {
            await upsertRetailerPipelineState({
              retailer,
              latestJobId: jobId,
              uploadState: {
                status: "running",
                updatedAt: new Date().toISOString(),
                uploaded: progress.uploaded,
                skipped: progress.skipped,
                failed: progress.failed,
                total: progress.total,
                currentUrl: progress.currentUrl,
                crawlSourceCrawledAt,
              },
            });
          },
          {
            resumeUploadedUrls,
            onItemResult: async (itemResult) => {
              await recordUploadUrlResult({
                retailer,
                crawlSourceCrawledAt,
                url: itemResult.url,
                jobId,
                status: itemResult.status,
                externalId: itemResult.externalId,
                itemName: itemResult.itemName,
                imageCount: itemResult.imageCount,
                uploadedToR2: itemResult.uploadedToR2,
                upsertedToDb: itemResult.upsertedToDb,
                error: itemResult.error,
                metadata: itemResult.metadata,
              });
              if (itemResult.status === "failed" && itemResult.error) {
                logScrapeError(jobId, "upload", itemResult.error, {
                  retailer,
                  url: itemResult.url,
                  code: classifyError(itemResult.error, ErrorCodes.UPLOAD_UNCAUGHT),
                  metadata: { itemName: itemResult.itemName, imageCount: itemResult.imageCount },
                });
              }
            },
          },
        );
        const uploadStatusPayload = buildUploadStatusPayload(retailer, crawlSourceCrawledAt, result, {
          jobId,
        });
        writeUploadStatusSnapshot(retailer, uploadStatusPayload);
        await upsertRetailerPipelineState({
          retailer,
          latestJobId: jobId,
          uploadState: {
            status: "completed",
            completedAt: result.uploadedAt,
            uploadedAt: result.uploadedAt,
            uploaded: result.uploaded,
            skipped: result.skipped,
            failed: result.failed,
            total: result.total,
            crawlSourceCrawledAt,
          },
        });
        uploadState.success += result.uploaded;
        uploadState.retailersDone += 1;
        pushEvent(jobId, {
          type: "e2e-progress",
          step: 4,
          stepLabel: "Extracting & uploading",
          brandsDiscovered: brands.length,
          configsTotal: brands.length,
          configsSuccessful,
          configsRecommended: recommendedConfigs.length,
          productUrlsTotal,
          productUrlsCrawled: totalCrawled,
          uploadedTotal: totalUrlsToUpload,
          uploadedSuccess: uploadState.success,
          retailersUploadComplete: uploadState.retailersDone,
          retailersUploadTotal: nUpload,
          percentComplete:
            totalUrlsToUpload > 0
              ? 82 + Math.floor((uploadState.success / totalUrlsToUpload) * 18)
              : 95,
          startedAt,
        });
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        uploadState.retailersDone += 1;
        logScrapeError(jobId, "upload", e, {
          retailer,
          code: classifyError(e, ErrorCodes.UPLOAD_UNCAUGHT),
        });
        await upsertRetailerPipelineState({
          retailer,
          latestJobId: jobId,
          uploadState: {
            status: "failed",
            failedAt: new Date().toISOString(),
            error: e.message,
            errorCode: classifyError(e, ErrorCodes.UPLOAD_UNCAUGHT),
          },
        });
        pushEvent(jobId, {
          type: "e2e-progress",
          step: 4,
          stepLabel: "Extracting & uploading",
          brandsDiscovered: brands.length,
          configsTotal: brands.length,
          configsSuccessful,
          configsRecommended: recommendedConfigs.length,
          productUrlsTotal,
          productUrlsCrawled: totalCrawled,
          uploadedTotal: totalUrlsToUpload,
          uploadedSuccess: uploadState.success,
          retailersUploadComplete: uploadState.retailersDone,
          retailersUploadTotal: nUpload,
          percentComplete:
            totalUrlsToUpload > 0
              ? 82 + Math.floor((uploadState.success / totalUrlsToUpload) * 18)
              : 95,
          startedAt,
        });
        return null;
      }
    });

    job.status = "done";
    persistJob(job);
    pushProgress({
      step: 4,
      stepLabel: "Complete",
      brandsDiscovered: brands.length,
      configsTotal: brands.length,
      configsSuccessful,
      configsRecommended: recommendedConfigs.length,
      productUrlsTotal,
      productUrlsCrawled: totalCrawled,
      uploadedTotal: totalUrlsToUpload,
      uploadedSuccess: uploadState.success,
      percentComplete: 100,
    });
    pushEvent(jobId, { type: "done" });
  } catch (err) {
    const e = err instanceof Error ? err : new Error(String(err));
    console.error(`[run-e2e job=${jobId}] Error:`, err);
    job.status = "error";
    job.error = e.message;
    persistJob(job);
    logScrapeError(jobId, "e2e", e, {
      code: classifyError(e, ErrorCodes.UNKNOWN),
    });
    pushEvent(jobId, { type: "done", error: e.message });
  }

  closeAllSseClients(jobId);
}

app.post("/api/run-e2e", async (req, res) => {
  const id = crypto.randomUUID();
  const cat = normalizeDiscoverCategory(req.body?.category);
  const discoverOptions: DiscoverBrandsOptions | undefined =
    cat !== undefined ? { category: cat } : undefined;
  const job = createJobRecord({
    id,
    kind: "e2e",
    urls: [],
    meta: discoverOptions ? { category: discoverOptions.category } : {},
  });
  jobs.set(id, job);
  await createPipelineJob(job);

  runE2EOrchestrator(id, discoverOptions).catch((err) => {
    console.error(`[run-e2e job=${id}] Unhandled error:`, err);
    const job = jobs.get(id);
    if (job) {
      job.status = "error";
      job.error = (err as Error).message;
      persistJob(job);
      pushEvent(id, { type: "done", error: (err as Error).message });
    }
    closeAllSseClients(id);
  });

  res.json({ jobId: id });
});

app.get("/api/job/:jobId", async (req, res) => {
  const job = jobs.get(req.params.jobId) ?? await getPipelineJob(req.params.jobId);
  if (!job) {
    res.status(404).json({ error: "Job not found." });
    return;
  }
  res.json({ exists: true, status: job.status });
});

app.get("/api/progress/:jobId", async (req, res) => {
  const jobId = req.params.jobId;
  const job = jobs.get(jobId) ?? await getPipelineJob(jobId);
  if (!job) {
    res.status(404).json({ error: "Job not found." });
    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });

  const replayedEvents = await listPipelineEvents(job.id);
  if (replayedEvents.length > 0) {
    for (const event of replayedEvents) {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    }
  } else {
    for (const msg of job.logs) {
      res.write(`data: ${JSON.stringify({ type: "log", msg })}\n\n`);
    }
    if (job.e2eProgress) {
      res.write(`data: ${JSON.stringify(job.e2eProgress)}\n\n`);
    }
    if (job.discoverModelResponse) {
      res.write(
        `data: ${JSON.stringify({
          type: "model-response",
          phase: "discover",
          body: job.discoverModelResponse,
        })}\n\n`,
      );
    }
  }

  if (job.status !== "running" || !jobs.has(job.id)) {
    const doneEvent: Record<string, unknown> = { type: "done" };
    if (job.discoveredBrands) doneEvent.brands = job.discoveredBrands;
    if (job.error) doneEvent.error = job.error;
    res.write(`data: ${JSON.stringify(doneEvent)}\n\n`);
    res.end();
    return;
  }

  if (!sseClients.has(job.id)) sseClients.set(job.id, new Set());
  sseClients.get(job.id)!.add(res);

  registerSseHeartbeat(res);
  req.on("close", () => {
    clearSseHeartbeat(res);
    sseClients.get(job.id)?.delete(res);
    console.log(`[sse] progress connection closed job=${job.id}`);
  });
});

app.get("/api/cost-metrics", (_req, res) => {
  try {
    res.json(getCostMetrics());
  } catch (err) {
    console.error("[api/cost-metrics] Error:", err);
    res.status(500).json({ error: "Failed to load cost metrics." });
  }
});

/**
 * Unified errors feed — every pipeline failure across explore / crawl / upload /
 * discover lands here. Used by the dashboard's Errors panel.
 *
 * Query params: ?limit=200&since=60&retailer=foo&stage=upload&code=UPLOAD_FETCH_FAILED
 */
app.get("/api/errors", async (req, res) => {
  try {
    const limitParam = Number.parseInt(String(req.query.limit ?? "200"), 10);
    const sinceParam = Number.parseInt(String(req.query.since ?? "1440"), 10);
    const errors = await listRecentScrapeErrors({
      limit: Number.isFinite(limitParam) ? limitParam : 200,
      sinceMinutes: Number.isFinite(sinceParam) ? sinceParam : 1440,
      retailer: typeof req.query.retailer === "string" ? req.query.retailer : undefined,
      stage: typeof req.query.stage === "string" ? req.query.stage : undefined,
      code: typeof req.query.code === "string" ? req.query.code : undefined,
    });
    const counts = await getScrapeErrorCounts({
      sinceMinutes: Number.isFinite(sinceParam) ? sinceParam : 1440,
    });
    res.json({ errors, counts });
  } catch (err) {
    console.error("[api/errors] Error:", err);
    res.status(500).json({
      error: err instanceof Error ? err.message : "Failed to load errors.",
      // Surface the failure of the errors endpoint itself so the dashboard can
      // show "errors API is down" rather than silently rendering an empty list.
      code: "ERRORS_API_FAILED",
    });
  }
});

// Operational issues mirrored from the home worker (heartbeat/batch/config/
// auto-update). Durable copy of the M1's localhost:4577 feed, so it can be
// diagnosed from any machine.
app.get("/api/worker-issues", async (req, res) => {
  try {
    const limitParam = Number.parseInt(String(req.query.limit ?? "200"), 10);
    const sinceParam = Number.parseInt(String(req.query.since ?? "1440"), 10);
    const issues = await listRecentWorkerIssues({
      limit: Number.isFinite(limitParam) ? limitParam : 200,
      sinceMinutes: Number.isFinite(sinceParam) ? sinceParam : 1440,
      source: typeof req.query.source === "string" ? req.query.source : undefined,
      workerId: typeof req.query.workerId === "string" ? req.query.workerId : undefined,
    });
    res.json({ issues });
  } catch (err) {
    console.error("[api/worker-issues] Error:", err);
    res.status(500).json({
      error: err instanceof Error ? err.message : "Failed to load worker issues.",
      code: "WORKER_ISSUES_API_FAILED",
    });
  }
});

// ---------------------------------------------------------------------------
// Durable queue + workers — exposes pg-boss state to the dashboard so an
// operator can see queue depth, dead-lettered jobs, and which workers are
// online (Railway vs. the M4 laptop, in the hybrid topology).
// ---------------------------------------------------------------------------

app.get("/api/queue/stats", async (_req, res) => {
  try {
    const [queues, workers] = await Promise.all([
      getQueueStats(),
      listWorkerHeartbeats({ staleAfterSeconds: 120 }),
    ]);
    res.json({ queues, workers });
  } catch (err) {
    console.error("[api/queue/stats] Error:", err);
    res.status(500).json({
      error: err instanceof Error ? err.message : "Failed to load queue stats.",
      code: "QUEUE_STATS_FAILED",
    });
  }
});

app.get("/api/queue/dead-letter", async (req, res) => {
  try {
    const limitParam = Number.parseInt(String(req.query.limit ?? "50"), 10);
    const rows = await listDeadLetter({
      limit: Number.isFinite(limitParam) ? limitParam : 50,
      queue: typeof req.query.queue === "string" ? req.query.queue : undefined,
    });
    res.json({ rows });
  } catch (err) {
    console.error("[api/queue/dead-letter] Error:", err);
    res.status(500).json({
      error: err instanceof Error ? err.message : "Failed to load DLQ.",
      code: "QUEUE_DLQ_FAILED",
    });
  }
});

app.post("/api/queue/retry/:jobId", async (req, res) => {
  try {
    const newId = await retryDeadLetterJob(req.params.jobId);
    if (!newId) return res.status(404).json({ error: "Job not found" });
    res.json({ ok: true, newJobId: newId });
  } catch (err) {
    console.error("[api/queue/retry] Error:", err);
    res.status(500).json({ error: err instanceof Error ? err.message : "Retry failed." });
  }
});

/**
 * Fan a retailer's product URLs onto the durable queue for processing by any
 * online worker. This is the queue-driven equivalent of POST /api/upload/:retailer
 * — but instead of running the upload inside this process, it just enqueues.
 */
/**
 * Fan a retailer's crawled URLs into the durable upload queue. Reads the
 * local product-urls file when present, else falls back to the crawl
 * checkpoint persisted in retailer_pipeline_state (survives redeploys).
 * Callable from the route and the auto-chain (crawl → upload).
 */
async function enqueueUploadForRetailer(
  retailer: string,
  limitArg?: number,
): Promise<
  | {
      retailer: string;
      totalUrls: number;
      targeted: number;
      alreadyUploaded: number;
      enqueued: number;
      duplicates: number;
    }
  | { error: string; status?: number }
> {
  const config = await loadStoredConfig(retailer);
  if (!config) {
    return { error: `No config for retailer "${retailer}"`, status: 404 };
  }

  let urls: string[] = [];
  let crawledAt: string | undefined;
  const productUrlsPath = path.join(PRODUCT_URLS_DIR, `${retailer}.json`);
  if (fs.existsSync(productUrlsPath)) {
    const urlsData = JSON.parse(fs.readFileSync(productUrlsPath, "utf-8")) as {
      urls?: string[];
      crawledAt?: string;
    };
    urls = Array.isArray(urlsData.urls) ? urlsData.urls : [];
    crawledAt = urlsData.crawledAt;
  }
  if (urls.length === 0) {
    const state = await getRetailerPipelineState(retailer);
    const crawlState = state?.crawlState as
      | { urls?: unknown; crawledAt?: unknown; completedAt?: unknown }
      | undefined;
    if (Array.isArray(crawlState?.urls)) {
      urls = crawlState.urls.filter((u): u is string => typeof u === "string");
      crawledAt =
        (typeof crawlState.crawledAt === "string" ? crawlState.crawledAt : undefined) ??
        (typeof crawlState.completedAt === "string" ? crawlState.completedAt : undefined);
    }
  }
  if (urls.length === 0) {
    return { error: `No crawled URLs for retailer "${retailer}"`, status: 404 };
  }
  const effectiveCrawledAt = crawledAt ?? new Date().toISOString();

  const limit = Math.max(
    1,
    Math.min(Number.isFinite(limitArg) && limitArg! > 0 ? Math.floor(limitArg!) : urls.length, urls.length),
  );
  const targetUrls = urls.slice(0, limit);

  // Skip URLs already uploaded for this crawl checkpoint.
  const alreadyDone = await getSuccessfulUploadUrls(retailer, effectiveCrawledAt);
  const toEnqueue = targetUrls.filter((u) => !alreadyDone.has(u));

  const queueJobs = toEnqueue.map((url) => {
    let domain = retailer;
    try {
      domain = new URL(url).hostname;
    } catch {
      /* fall back to retailer name */
    }
    return {
      retailer,
      url,
      crawlSourceCrawledAt: effectiveCrawledAt,
      domain,
    };
  });

  const { accepted, duplicates } = await enqueueUploadUrls(queueJobs);
  return {
    retailer,
    totalUrls: urls.length,
    targeted: targetUrls.length,
    alreadyUploaded: alreadyDone.size,
    enqueued: accepted,
    duplicates,
  };
}

app.post("/api/upload/:retailer/enqueue", async (req, res) => {
  try {
    const rawLimit = Number(req.body?.limit);
    const result = await enqueueUploadForRetailer(
      req.params.retailer,
      Number.isFinite(rawLimit) ? rawLimit : undefined,
    );
    if ("error" in result) {
      return res.status(result.status ?? 500).json({ error: result.error });
    }
    res.json(result);
  } catch (err) {
    console.error("[api/upload/enqueue] Error:", err);
    res.status(500).json({
      error: err instanceof Error ? err.message : "Enqueue failed.",
      code: "QUEUE_ENQUEUE_FAILED",
    });
  }
});

app.get("/api/discovered-brands", (_req, res) => {
  try {
    if (!fs.existsSync(DISCOVERED_BRANDS_PATH)) {
      return res.json({ brands: [], urls: [] });
    }
    const raw = fs.readFileSync(DISCOVERED_BRANDS_PATH, "utf-8");
    const data = JSON.parse(raw) as { brands?: unknown[]; urls?: unknown[]; history?: unknown[] };
    const historyRaw = data.history;
    const history = Array.isArray(historyRaw)
      ? historyRaw.filter(
          (h): h is { at: string; count: number } =>
            !!h &&
            typeof h === "object" &&
            typeof (h as { at?: string }).at === "string" &&
            typeof (h as { count?: number }).count === "number" &&
            Number.isFinite((h as { count: number }).count),
        )
      : [];
    res.json({
      brands: Array.isArray(data.brands) ? data.brands : [],
      urls: Array.isArray(data.urls) ? data.urls : [],
      history,
    });
  } catch (err) {
    console.error("[api/discovered-brands] Error:", err);
    res.status(500).json({ error: "Failed to load discovered brands." });
  }
});

app.get("/api/retailers-overview", async (_req, res) => {
  try {
    fs.mkdirSync(CONFIGS_DIR, { recursive: true });
    const files = fs.readdirSync(CONFIGS_DIR).filter((f) => f.endsWith(".json"));
    const stateRows = await listRetailerPipelineStates();
    const stateRetailers = new Set(
      stateRows
        .filter((row) => row.exploreState?.config)
        .map((row) => row.retailer),
    );
    const allRetailers = new Set<string>([
      ...files.map((filename) => filename.replace(/\.json$/, "")),
      ...stateRetailers,
    ]);

    const configBaseUrls = new Set<string>();
    const retailers = [];
    for (const retailer of [...allRetailers].sort()) {
      const filename = `${retailer}.json`;
      const config = await loadStoredConfig(retailer);
      if (!config) continue;
      const baseUrlRaw = typeof config.baseUrl === "string" ? config.baseUrl.trim() : "";
      const normBase = normalizeUrl(baseUrlRaw);
      if (normBase) configBaseUrls.add(normBase);

      const dq = config.dataQuality as { overallRecommendation?: string } | undefined;
      const state = stateRows.find((row) => row.retailer === retailer);
      const storedRecommendation = normalizeRecommendationValue(
        dq?.overallRecommendation ?? state?.exploreState?.recommendation,
      );
      const exploreStatus = getExploreStatus(state?.exploreState, true, hasConfigError(config));
      const recommendation = getEffectiveRecommendation(storedRecommendation, exploreStatus);
      const exploreError =
        typeof state?.exploreState?.error === "string" ? state.exploreState.error : null;
      const retryAt =
        typeof state?.exploreState?.nextRetryAt === "string" ? state.exploreState.nextRetryAt : null;
      const failureCode =
        typeof state?.exploreState?.failureCode === "string" ? state.exploreState.failureCode : null;
      const failureReason =
        typeof state?.exploreState?.failureReason === "string" ? state.exploreState.failureReason : null;
      const exploreAttempt =
        typeof state?.exploreState?.attempt === "number" ? state.exploreState.attempt : null;
      const exploreMaxAttempts =
        typeof state?.exploreState?.maxAttempts === "number" ? state.exploreState.maxAttempts : null;

      const crawlResult = await loadStoredCrawlResult(retailer);
      const crawl = crawlResult
        ? {
            totalUrls: crawlResult.totalUrls,
            crawledAt: crawlResult.crawledAt,
            method: crawlResult.method,
          }
        : null;

      const upload = await loadStoredUploadStatus(retailer);
      const uploadMatchesCurrentCrawl = !!(
        crawl &&
        upload &&
        typeof upload.crawlSourceCrawledAt === "string" &&
        upload.crawlSourceCrawledAt === crawl.crawledAt
      );

      // Live stage state from retailer_pipeline_state — lets the dashboard show
      // in-flight crawls/uploads (and their failures) without polling job SSE.
      const crawlState = state?.crawlState as Record<string, unknown> | undefined;
      const uploadState = state?.uploadState as Record<string, unknown> | undefined;
      const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : null);
      const str = (v: unknown) => (typeof v === "string" && v.length > 0 ? v : null);
      const crawlLive = str(crawlState?.status)
        ? {
            status: str(crawlState?.status)!,
            totalUrls: num(crawlState?.totalUrls),
            lastCheckpointAt: str(crawlState?.lastCheckpointAt),
            error: str(crawlState?.error),
          }
        : null;
      const uploadLive = str(uploadState?.status)
        ? {
            status: str(uploadState?.status)!,
            uploaded: num(uploadState?.uploaded),
            skipped: num(uploadState?.skipped),
            failed: num(uploadState?.failed),
            total: num(uploadState?.total),
            error: str(uploadState?.error),
          }
        : null;

      retailers.push({
        retailer,
        filename,
        config,
        configValid: isConfigValid(config),
        configMethod:
          (config.discovery as { method?: unknown } | undefined)?.method &&
          typeof (config.discovery as { method?: unknown }).method === "string"
            ? ((config.discovery as { method: string }).method)
            : null,
        recommendation,
        storedRecommendation,
        exploreStatus,
        exploreError,
        retryAt,
        exploreFailureCode: failureCode,
        exploreFailureReason: failureReason,
        exploreAttempt,
        exploreMaxAttempts,
        crawl,
        upload,
        uploadMatchesCurrentCrawl,
        crawlLive,
        uploadLive,
        latestJobId: str(state?.latestJobId),
      });
    }

    const identifiedWithoutConfig: { name: string; url: string }[] = [];
    const backlogSeen = new Set<string>();

    // Brand store is Postgres-first (JSON only as offline fallback) — the old
    // direct read of discovered-brands.json went stale on Railway's ephemeral FS.
    // Only pipeline-eligible brands (approved or legacy) belong in the explore backlog.
    let discoveredBrandsList: DiscoveredBrand[] = [];
    try {
      discoveredBrandsList = (await listBrands()).filter(isPipelineEligible);
    } catch (err) {
      console.error("[api/retailers-overview] Failed to list brands for backlog:", err);
    }

    for (const b of discoveredBrandsList) {
      if (!b || typeof b.url !== "string") continue;
      const n = normalizeUrl(b.url);
      if (!n || backlogSeen.has(n)) continue;
      if (configBaseUrls.has(n)) continue;
      backlogSeen.add(n);
      const name = typeof b.name === "string" && b.name.trim().length > 0 ? b.name.trim() : n;
      identifiedWithoutConfig.push({ name, url: n });
    }
    for (const row of stateRows) {
      const n = typeof row.baseUrl === "string" ? normalizeUrl(row.baseUrl) : null;
      if (!n || backlogSeen.has(n)) continue;
      if (configBaseUrls.has(n)) continue;
      backlogSeen.add(n);
      identifiedWithoutConfig.push({
        name: row.displayName?.trim() || n,
        url: n,
      });
    }
    identifiedWithoutConfig.sort((a, b) =>
      a.name.localeCompare(b.name, undefined, { sensitivity: "base" }),
    );

    res.json({ retailers, identifiedWithoutConfig });
  } catch (err) {
    console.error("[api/retailers-overview] Error:", err);
    res.status(500).json({ error: "Failed to load retailers overview." });
  }
});

app.get("/api/configs", async (_req, res) => {
  try {
    fs.mkdirSync(CONFIGS_DIR, { recursive: true });
    const files = fs.readdirSync(CONFIGS_DIR).filter((f) => f.endsWith(".json"));
    const stateRows = await listRetailerPipelineStates();
    const allRetailers = new Set<string>([
      ...files.map((filename) => filename.replace(/\.json$/, "")),
      ...stateRows.filter((row) => row.exploreState?.config).map((row) => row.retailer),
    ]);

    const configs = await Promise.all(
      [...allRetailers].sort().map(async (retailer) => ({
        retailer,
        filename: `${retailer}.json`,
        config: await loadStoredConfig(retailer),
      })),
    );

    res.json(configs);
  } catch (err) {
    console.error("[api/configs] Error:", err);
    res.status(500).json({ error: "Failed to load configs." });
  }
});

app.get("/api/configs/:retailer", async (req, res) => {
  const config = await loadStoredConfig(req.params.retailer);
  if (!config) {
    res.status(404).json({ error: "Config not found." });
    return;
  }
  res.json(config);
});

app.patch("/api/configs/:retailer/recommendation", async (req, res) => {
  const retailer = req.params.retailer;
  const recommendation = normalizeRecommendationValue(req.body?.recommendation);
  if (recommendation === "unknown") {
    res.status(400).json({ error: "Recommendation must be recommended, usable, or not recommended." });
    return;
  }

  const config = await loadStoredConfig(retailer);
  if (!config) {
    res.status(404).json({ error: "Config not found." });
    return;
  }

  const currentDataQuality =
    config.dataQuality && typeof config.dataQuality === "object"
      ? (config.dataQuality as Record<string, unknown>)
      : {};

  const nextConfig: Record<string, unknown> = {
    ...config,
    dataQuality: {
      ...currentDataQuality,
      overallRecommendation: recommendation,
    },
  };

  writeJsonAtomic(path.join(CONFIGS_DIR, `${retailer}.json`), nextConfig);
  const state = await getRetailerPipelineState(retailer);
  await upsertRetailerPipelineState({
    retailer,
    baseUrl: typeof nextConfig.baseUrl === "string" ? nextConfig.baseUrl : state?.baseUrl,
    displayName:
      typeof nextConfig.retailerDisplayName === "string" ? nextConfig.retailerDisplayName : state?.displayName,
    latestJobId: state?.latestJobId,
    exploreState: {
      recommendation,
      config: nextConfig,
    },
  });

  res.json({
    retailer,
    recommendation,
    config: nextConfig,
  });
});

// ---------------------------------------------------------------------------
// Crawl endpoints — use a generated config to discover all product URLs
// ---------------------------------------------------------------------------

/**
 * Start a crawl job for a retailer with a stored config. Callable from the
 * route, the auto-chain (explore → crawl), and the weekly re-crawl sweep.
 */
async function startCrawlJob(
  retailer: string,
  opts?: { auto?: boolean },
): Promise<{ jobId?: string; error?: string; status?: number; details?: unknown }> {
  const config = await loadStoredConfig(retailer);
  if (!config) {
    return { error: "Config not found.", status: 404 };
  }

  const parsedConfig = safeParseConfig(config);
  if (!parsedConfig.success) {
    return {
      error: "Stored config is invalid.",
      status: 400,
      details: parsedConfig.error.flatten(),
    };
  }
  const crawlConfig = parsedConfig.data;

  const id = crypto.randomUUID();
  const job = createJobRecord({
    id,
    kind: "crawl",
    retailer,
    urls: [crawlConfig.baseUrl],
    meta: opts?.auto ? { auto: true } : {},
  });
  jobs.set(id, job);
  await createPipelineJob(job);

  (async () => {
    try {
      const existingState = await getRetailerPipelineState(retailer);
      const initialUrls = Array.isArray(existingState?.crawlState?.urls)
        ? (existingState.crawlState.urls as string[])
        : [];
      const result = await crawlProductUrls(
        crawlConfig,
        (msg) => pushLog(job.id, msg),
        (count) => pushEvent(job.id, { type: "crawl-progress", count }),
        {
          initialUrls,
          onCheckpoint: async (partial) => {
            await upsertRetailerPipelineState({
              retailer,
              baseUrl: crawlConfig.baseUrl,
              displayName: crawlConfig.retailerDisplayName,
              latestJobId: job.id,
              crawlState: {
                status: "running",
                lastCheckpointAt: partial.crawledAt,
                totalUrls: partial.totalUrls,
                method: partial.method,
                urls: partial.urls,
                resumable: true,
                resumedFromCheckpoint: partial.resumedFromCheckpoint,
              },
            });
          },
          onComplete: async (finalResult) => {
            await upsertRetailerPipelineState({
              retailer,
              latestJobId: job.id,
              crawlState: {
                status: "completed",
                completedAt: finalResult.crawledAt,
                crawledAt: finalResult.crawledAt,
                totalUrls: finalResult.totalUrls,
                method: finalResult.method,
                urls: finalResult.urls,
                resumedFromCheckpoint: finalResult.resumedFromCheckpoint,
              },
            });
          },
          onError: async (partial, error) => {
            logScrapeError(job.id, "crawl", error, {
              retailer,
              code: classifyError(error, ErrorCodes.CRAWL_UNCAUGHT),
              metadata: { method: partial.method, totalUrls: partial.totalUrls },
            });
            await upsertRetailerPipelineState({
              retailer,
              latestJobId: job.id,
              crawlState: {
                status: "failed",
                failedAt: new Date().toISOString(),
                crawledAt: partial.crawledAt,
                totalUrls: partial.totalUrls,
                method: partial.method,
                urls: partial.urls,
                error: error.message,
                errorCode: classifyError(error, ErrorCodes.CRAWL_UNCAUGHT),
                resumable: true,
                resumedFromCheckpoint: partial.resumedFromCheckpoint,
              },
            });
          },
        },
      );

      fs.mkdirSync(PRODUCT_URLS_DIR, { recursive: true });
      writeJsonAtomic(path.join(PRODUCT_URLS_DIR, `${retailer}.json`), result);

      job.results.push(result as unknown as Record<string, unknown>);
      job.status = result.completed === false ? "error" : "done";
      if (result.error) job.error = result.error;
      persistJob(job);
      pushEvent(job.id, {
        type: "done",
        totalUrls: result.totalUrls,
        ...(result.error ? { error: result.error } : {}),
      });
      // Conveyor: a finished crawl rolls into a queued upload (gated).
      if (result.completed !== false) {
        void maybeChain("crawl-completed", { retailer });
      }
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[crawl job=${job.id}] Error crawling ${retailer}:`, err);
      logScrapeError(job.id, "crawl", e, {
        retailer,
        code: classifyError(e, ErrorCodes.CRAWL_UNCAUGHT),
      });
      job.status = "error";
      job.error = e.message;
      persistJob(job);
      pushEvent(job.id, { type: "done", error: e.message });
    }

    closeAllSseClients(job.id);
  })();

  return { jobId: id };
}

app.post("/api/crawl/:retailer", async (req, res) => {
  const result = await startCrawlJob(req.params.retailer);
  if (result.error) {
    res.status(result.status ?? 500).json({
      error: result.error,
      ...(result.details !== undefined ? { details: result.details } : {}),
    });
    return;
  }
  res.json({ jobId: result.jobId });
});

app.get("/api/product-urls", async (_req, res) => {
  try {
    fs.mkdirSync(PRODUCT_URLS_DIR, { recursive: true });
    const files = fs
      .readdirSync(PRODUCT_URLS_DIR)
      .filter((f) => f.endsWith(".json") && !f.endsWith(".partial.json"));
    const stateRows = await listRetailerPipelineStates();
    const allRetailers = new Set<string>([
      ...files.map((filename) => filename.replace(/\.json$/, "")),
      ...stateRows.filter((row) => Array.isArray(row.crawlState?.urls)).map((row) => row.retailer),
    ]);

    const results = (
      await Promise.all(
        [...allRetailers].sort().map(async (retailer) => {
          const data = await loadStoredCrawlResult(retailer);
          if (!data) return null;
          return {
            retailer: data.retailer,
            crawledAt: data.crawledAt,
            method: data.method,
            totalUrls: data.totalUrls,
          };
        }),
      )
    ).filter(Boolean);

    res.json(results);
  } catch (err) {
    console.error("[api/product-urls] Error:", err);
    res.status(500).json({ error: "Failed to load product URLs." });
  }
});

app.get("/api/product-urls/:retailer", async (req, res) => {
  const data = await loadStoredCrawlResult(req.params.retailer);
  if (!data) {
    res.status(404).json({ error: "Product URLs not found." });
    return;
  }
  res.json(data);
});

// ---------------------------------------------------------------------------
// Upload endpoints — scrape product URLs, upload images to R2, upsert to DB
// ---------------------------------------------------------------------------

app.post("/api/upload/:retailer", async (req, res) => {
  const retailer = req.params.retailer;
  const config = await loadStoredConfig(retailer);
  if (!config) {
    res.status(404).json({ error: "Config not found." });
    return;
  }
  const urlsData = await loadStoredCrawlResult(retailer);
  if (!urlsData) {
    res.status(404).json({ error: "Product URLs not found. Crawl first." });
    return;
  }

  if (!urlsData.urls || urlsData.urls.length === 0) {
    res.status(400).json({ error: "No product URLs available." });
    return;
  }

  const id = crypto.randomUUID();
  const job = createJobRecord({
    id,
    kind: "upload",
    retailer,
    urls: urlsData.urls,
  });
  jobs.set(id, job);
  await createPipelineJob(job);

  (async () => {
    try {
      const resumeUploadedUrls = await getSuccessfulUploadUrls(retailer, urlsData.crawledAt);
      await upsertRetailerPipelineState({
        retailer,
        baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : undefined,
        displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : retailer,
        latestJobId: job.id,
        uploadState: {
          status: "running",
          startedAt: new Date().toISOString(),
          uploaded: 0,
          skipped: resumeUploadedUrls.size,
          failed: 0,
          total: urlsData.urls.length,
          crawlSourceCrawledAt: urlsData.crawledAt,
        },
      });
      const result = await uploadRetailer(
        config as Parameters<typeof uploadRetailer>[0],
        urlsData.urls,
        (msg) => pushLog(job.id, msg),
        async (progress) => {
          pushEvent(job.id, {
            type: "upload-progress",
            uploaded: progress.uploaded,
            skipped: progress.skipped,
            failed: progress.failed,
            total: progress.total,
            currentUrl: progress.currentUrl,
          });
          await upsertRetailerPipelineState({
            retailer,
            latestJobId: job.id,
            uploadState: {
              status: "running",
              updatedAt: new Date().toISOString(),
              uploaded: progress.uploaded,
              skipped: progress.skipped,
              failed: progress.failed,
              total: progress.total,
              currentUrl: progress.currentUrl,
              crawlSourceCrawledAt: urlsData.crawledAt,
            },
          });
        },
        {
          resumeUploadedUrls,
          onItemResult: async (itemResult) => {
            await recordUploadUrlResult({
              retailer,
              crawlSourceCrawledAt: urlsData.crawledAt,
              url: itemResult.url,
              jobId: job.id,
              status: itemResult.status,
              externalId: itemResult.externalId,
              itemName: itemResult.itemName,
              imageCount: itemResult.imageCount,
              uploadedToR2: itemResult.uploadedToR2,
              upsertedToDb: itemResult.upsertedToDb,
              error: itemResult.error,
              metadata: itemResult.metadata,
            });
            if (itemResult.status === "failed" && itemResult.error) {
              logScrapeError(job.id, "upload", itemResult.error, {
                retailer,
                url: itemResult.url,
                code: classifyError(itemResult.error, ErrorCodes.UPLOAD_UNCAUGHT),
                metadata: { itemName: itemResult.itemName, imageCount: itemResult.imageCount },
              });
            }
          },
        },
      );

      job.results.push(result as unknown as Record<string, unknown>);
      job.status = "done";
      persistJob(job);

      const uploadStatusPayload = buildUploadStatusPayload(retailer, urlsData.crawledAt, result, {
        jobId: job.id,
      });
      writeUploadStatusSnapshot(retailer, uploadStatusPayload);
      await upsertRetailerPipelineState({
        retailer,
        latestJobId: job.id,
        uploadState: {
          status: "completed",
          uploadedAt: result.uploadedAt,
          completedAt: result.uploadedAt,
          uploaded: result.uploaded,
          skipped: result.skipped,
          failed: result.failed,
          total: result.total,
          crawlSourceCrawledAt: urlsData.crawledAt,
        },
      });
      // Conveyor: fresh items roll into a processing batch (gated). The
      // queued-upload path triggers via the worker's drain-check instead.
      void maybeChain("upload-completed", { retailer });

      pushEvent(job.id, {
        type: "done",
        uploaded: result.uploaded,
        skipped: result.skipped,
        failed: result.failed,
        total: result.total,
      });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[upload job=${job.id}] Error uploading ${retailer}:`, err);
      logScrapeError(job.id, "upload", e, {
        retailer,
        code: classifyError(e, ErrorCodes.UPLOAD_UNCAUGHT),
      });
      job.status = "error";
      job.error = e.message;
      persistJob(job);
      await upsertRetailerPipelineState({
        retailer,
        latestJobId: job.id,
        uploadState: {
          status: "failed",
          failedAt: new Date().toISOString(),
          error: e.message,
          errorCode: classifyError(e, ErrorCodes.UPLOAD_UNCAUGHT),
          crawlSourceCrawledAt: urlsData.crawledAt,
        },
      });
      pushEvent(job.id, { type: "done", error: e.message });
    }

    closeAllSseClients(job.id);
  })();

  res.json({ jobId: id });
});

// ---------------------------------------------------------------------------
// Serve the dashboards
// ---------------------------------------------------------------------------

// New React dashboard (Vite build) under /app — falls back to index.html for SPA routing.
// The same bundle also serves the mobile swipe app at /swipe (App.tsx switches on
// the pathname; Vite's base:"/app/" makes asset URLs absolute, so they resolve
// from /swipe as well).
const DASHBOARD_DIST = path.join(__dirname, "..", "dashboard", "dist");
if (fs.existsSync(path.join(DASHBOARD_DIST, "index.html"))) {
  // Vite emits hashed asset filenames under /assets, so those are safe to cache
  // forever. The HTML entry must NOT be cached aggressively or browsers keep
  // pointing at the previous deploy's bundle hashes.
  app.use(
    "/app",
    express.static(DASHBOARD_DIST, {
      setHeaders: (res, filePath) => {
        if (filePath.endsWith("index.html")) {
          res.setHeader("Cache-Control", "no-cache");
        }
      },
    }),
  );
  app.get(/^\/(app|swipe)(\/.*)?$/, (_req, res) => {
    res.setHeader("Cache-Control", "no-cache");
    res.sendFile(path.join(DASHBOARD_DIST, "index.html"));
  });
  console.log("[dashboard] Serving React dashboard at /app (mobile swipe at /swipe)");
} else {
  console.log("[dashboard] React build not found; run `npm run build` in dashboard/ to enable /app");
}

// The React dashboard is the primary UI — send the bare domain straight to it.
// The legacy monolithic HTML UI stays reachable at /legacy as a fallback.
app.get("/", (_req, res) => {
  res.redirect("/app/");
});
app.get("/legacy", (_req, res) => {
  res.sendFile(path.join(__dirname, "Ui.html"));
});

// ---------------------------------------------------------------------------
// Conveyor wiring + weekly re-crawl sweep
// ---------------------------------------------------------------------------

configureAutoChain({
  startCrawlJob: (retailer, opts) => startCrawlJob(retailer, opts),
  enqueueUploadForRetailer: async (retailer) => {
    const result = await enqueueUploadForRetailer(retailer);
    return "error" in result ? { error: result.error } : { enqueued: result.enqueued };
  },
  countRunningAutoCrawls: () => {
    let n = 0;
    for (const job of jobs.values()) {
      if (job.kind === "crawl" && job.status === "running" && job.meta?.auto === true) n++;
    }
    return n;
  },
});

/** Poll the in-memory job map until a job leaves "running" (or the cap hits). */
async function waitForJobSettled(jobId: string, capMs: number): Promise<void> {
  const deadline = Date.now() + capMs;
  while (Date.now() < deadline) {
    const job = jobs.get(jobId);
    if (!job || job.status !== "running") return;
    await sleep(5000);
  }
}

/**
 * Weekly conveyor sweep: re-crawl every retailer whose config is
 * `recommended`, one at a time (stagger = wait for each crawl to settle, cap
 * 10 min) to stay polite. Each completed crawl auto-chains its own upload.
 */
async function runWeeklyRecrawlSweep(): Promise<void> {
  const states = await listRetailerPipelineStates();
  let started = 0;
  for (const state of states) {
    if (!(await isAutoPipelineEnabled())) {
      console.log("[sweep] kill switch off — stopping re-crawl sweep.");
      return;
    }
    if (!(await getSetting<boolean>("gate_crawl", true))) return;
    const retailer = state.retailer;
    const rec = String(
      (state.exploreState as { recommendation?: unknown } | undefined)?.recommendation ?? "",
    ).toLowerCase();
    if (rec !== "recommended") continue;
    if (await getSetting<boolean>(`autopilot_optout:${retailer}`, false)) continue;
    const result = await startCrawlJob(retailer, { auto: true });
    if (result.error) {
      console.log(`[sweep] ${retailer}: crawl not started (${result.error}).`);
      continue;
    }
    started++;
    console.log(`[sweep] ${retailer}: re-crawl started (job ${result.jobId}).`);
    await waitForJobSettled(result.jobId!, 10 * 60_000);
  }
  console.log(`[sweep] weekly re-crawl sweep done — ${started} crawls started.`);
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const server = app.listen(PORT, () => {
  void (async () => {
    try {
      await ensurePipelinePersistenceSchema();
      await markRunningJobsInterrupted();
    } catch (err) {
      console.error("[startup] Failed to initialize pipeline persistence:", err);
    }
    try {
      const { registerSchedules } = await import("./schedules.js");
      await registerSchedules();
      // The server claims the weekly sweep itself — crawl orchestration
      // (jobs map, SSE, checkpoints) lives in this process.
      const boss = await getBoss();
      if (boss) {
        await boss.work<PipelineSweepJobData>(
          QUEUES.PIPELINE_SWEEP,
          { batchSize: 1, pollingIntervalSeconds: 30 },
          async () => {
            await runWeeklyRecrawlSweep();
          },
        );
      }
    } catch (err) {
      console.error("[startup] Failed to register schedules/sweep worker:", err);
    }
    try {
      // Seed the durable brand store from any local JSON on first boot (no-op
      // once the DB has rows), then reconcile configs into the master list.
      const seeded = await bootstrapBrandsFromJson();
      if (seeded > 0) console.log(`[startup] Seeded ${seeded} brands into Postgres from local JSON.`);
      await bootstrapLeadsFromJson();
      await syncConfigsToMasterList(CONFIGS_DIR);
    } catch (err) {
      console.error("[startup] Failed to sync configs to master list:", err);
    }
    console.log(`\n  Crawler Config UI running at http://localhost:${PORT}\n`);
  })();
});

// Graceful shutdown — Railway sends SIGTERM to the old container on every
// redeploy. Without a handler, Node dies by signal (non-zero exit) and Railway
// emails a "deployment crashed" alert even though the new deploy succeeded.
// Exit 0 instead; anything in flight is already covered by the resume story
// (markRunningJobsInterrupted on next boot + crawl checkpoints + upload ledger).
let shuttingDown = false;
function shutdownGracefully(signal: string): void {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[shutdown] ${signal} received — exiting cleanly (interrupted jobs resume on next boot).`);
  server.close();
  // Hard deadline: open SSE sockets or a stuck pool query must not stall the swap.
  setTimeout(() => process.exit(0), 5000);
  void (async () => {
    try {
      await stopBoss();
    } catch {
      /* best-effort */
    }
    try {
      await getPool()?.end();
    } catch {
      /* best-effort */
    }
    process.exit(0);
  })();
}
process.on("SIGTERM", () => shutdownGracefully("SIGTERM"));
process.on("SIGINT", () => shutdownGracefully("SIGINT"));