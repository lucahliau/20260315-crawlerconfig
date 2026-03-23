import "dotenv/config";
import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { exploreRetailer } from "./explore.js";
import { crawlProductUrls, type CrawlResult } from "./crawl.js";
import { uploadRetailer, type UploadResult } from "./upload.js";
import {
  discoverBrands,
  normalizeDiscoverCategory,
  normalizeUrl,
  syncConfigsToMasterList,
  type DiscoveredBrand,
  type DiscoverBrandsOptions,
} from "./discoverBrands.js";
import { writeJsonAtomic } from "./jsonFs.js";
import { retailerSlugFromUrl } from "./retailerSlug.js";
import { recordDiscoverUsage, recordExploreUsage, getCostMetrics } from "./usageLedger.js";
import {
  appendPipelineEvent,
  createPipelineJob,
  ensurePipelinePersistenceSchema,
  getPipelineJob,
  getRetailerPipelineState,
  getSuccessfulUploadUrls,
  listRetailerPipelineStates,
  listPipelineEvents,
  markRunningJobsInterrupted,
  recordUploadUrlResult,
  savePipelineJob,
  upsertRetailerPipelineState,
  type PipelineJobRecord,
} from "./pipelineStore.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json());

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

// ---------------------------------------------------------------------------
// Job tracking — one Stagehand browser at a time
// ---------------------------------------------------------------------------

type Job = PipelineJobRecord;

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

async function processJob(job: Job, skippedUrls?: string[]) {
  if (skippedUrls && skippedUrls.length > 0) {
    pushLog(
      job.id,
      `Skipped ${skippedUrls.length} URL(s) with existing configs:\n${skippedUrls.join("\n")}\n`,
    );
  }
  for (let i = 0; i < job.urls.length; i++) {
    // Add a cooldown between URLs to avoid stacking rate limit usage
    if (i > 0 && INTER_URL_DELAY_MS > 0) {
      pushLog(job.id, `\nWaiting ${INTER_URL_DELAY_MS / 1000}s before next URL...\n`);
      await new Promise((r) => setTimeout(r, INTER_URL_DELAY_MS));
    }

    const url = job.urls[i];
    job.current = i;
    job.currentUrl = url;
    persistJob(job);
    pushEvent(job.id, {
      type: "progress",
      current: i + 1,
      total: job.urls.length,
      url,
    });
    pushLog(job.id, `\n========== Processing ${i + 1}/${job.urls.length}: ${url} ==========\n`);

    try {
      const { config, metrics, estimatedUsd } = await exploreRetailer(url, (msg) => pushLog(job.id, msg));
      recordExploreUsage({
        retailer: (config.retailer as string) ?? retailerSlugFromUrl(url),
        estimatedUsd,
        promptTokens: metrics?.totalPromptTokens ?? 0,
        completionTokens: metrics?.totalCompletionTokens ?? 0,
        inferenceTimeMs: metrics?.totalInferenceTimeMs ?? 0,
      });
      job.results.push(config);
      persistJob(job);
      const retailer = (config.retailer as string) ?? retailerSlugFromUrl(url);
      await upsertRetailerPipelineState({
        retailer,
        baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : url,
        displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : retailer,
        latestJobId: job.id,
        exploreState: {
          status: "completed",
          startedAt: job.createdAt ?? new Date().toISOString(),
          completedAt: new Date().toISOString(),
          configPath: path.join(CONFIGS_DIR, `${retailer}.json`),
          config,
          estimatedUsd,
          promptTokens: metrics?.totalPromptTokens ?? 0,
          completionTokens: metrics?.totalCompletionTokens ?? 0,
        },
      });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[processJob job=${job.id}] Error processing ${url}:`, err);
      pushLog(job.id, `ERROR processing ${url}: ${e.message}`, e);
      await upsertRetailerPipelineState({
        retailer: retailerSlugFromUrl(url),
        baseUrl: url,
        latestJobId: job.id,
        exploreState: {
          status: "failed",
          failedAt: new Date().toISOString(),
          error: e.message,
        },
      });
    }
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
  const { urls, skipExisting } = req.body as { urls?: string[]; skipExisting?: boolean };
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
        skippedUrls.push(url);
        continue;
      }
    }
    toExplore.push(url);
  }

  if (toExplore.length === 0) {
    res.status(400).json({
      error: "All URLs already have config files. Clear skip or remove configs to re-explore.",
      skippedUrls,
    });
    return;
  }

  const id = crypto.randomUUID();
  const job = createJobRecord({ id, kind: "explore", urls: toExplore });
  jobs.set(id, job);
  await createPipelineJob(job);

  processJob(job, skippedUrls.length > 0 ? skippedUrls : undefined).catch((err) => {
    job.status = "error";
    job.error = (err as Error).message;
    persistJob(job);
    console.error(`[processJob job=${job.id}] Error:`, err);
  });

  res.json({ jobId: id, skippedUrls, skippedCount: skippedUrls.length });
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
      pushLog(job.id, `ERROR: ${e.message}`, e);
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

async function loadStoredConfig(retailer: string): Promise<Record<string, unknown> | null> {
  const filePath = path.join(CONFIGS_DIR, `${retailer}.json`);
  const fromFile = readJsonFile<Record<string, unknown>>(filePath);
  if (fromFile) return fromFile;
  const state = await getRetailerPipelineState(retailer);
  const config = state?.exploreState?.config;
  return config && typeof config === "object" ? (config as Record<string, unknown>) : null;
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

      try {
        await upsertRetailerPipelineState({
          retailer: fallbackRetailer,
          baseUrl: url,
          displayName: brand.name,
          latestJobId: jobId,
          exploreState: {
            status: "running",
            startedAt: new Date().toISOString(),
          },
        });
        const { config, metrics, estimatedUsd } = await exploreRetailer(url, (msg) => pushLog(jobId, msg));
        recordExploreUsage({
          retailer: (config.retailer as string) ?? extractRetailerFromUrl(url),
          estimatedUsd,
          promptTokens: metrics?.totalPromptTokens ?? 0,
          completionTokens: metrics?.totalCompletionTokens ?? 0,
          inferenceTimeMs: metrics?.totalInferenceTimeMs ?? 0,
          e2eJobId: jobId,
        });
        configsSuccessful++;
        const retailer = (config.retailer as string) ?? extractRetailerFromUrl(url);
        const dq = config.dataQuality as Record<string, unknown> | undefined;
        const rec = dq?.overallRecommendation as string | undefined;
        await upsertRetailerPipelineState({
          retailer,
          baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : url,
          displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : brand.name,
          latestJobId: jobId,
          exploreState: {
            status: "completed",
            completedAt: new Date().toISOString(),
            recommendation: rec ?? "unknown",
            estimatedUsd,
            configPath: path.join(CONFIGS_DIR, `${retailer}.json`),
            config,
          },
        });
        if (rec === "recommended") {
          recommendedConfigs.push({ retailer, config });
        }
        pushLog(jobId, `  Config for ${brand.name}: ${rec ?? "unknown"}\n`);
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        pushLog(jobId, `  ERROR exploring ${brand.name}: ${e.message}\n`, e);
        await upsertRetailerPipelineState({
          retailer: fallbackRetailer,
          baseUrl: url,
          displayName: brand.name,
          latestJobId: jobId,
          exploreState: {
            status: "failed",
            failedAt: new Date().toISOString(),
            error: e.message,
          },
        });
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
        const crawlConfig = config as Parameters<typeof crawlProductUrls>[0];
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
        pushLog(jobId, `Crawl failed for ${retailer}: ${e.message}\n`, e);
        await upsertRetailerPipelineState({
          retailer,
          latestJobId: jobId,
          crawlState: {
            status: "failed",
            failedAt: new Date().toISOString(),
            error: e.message,
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
        pushLog(jobId, `Upload failed for ${retailer}: ${e.message}\n`, e);
        await upsertRetailerPipelineState({
          retailer,
          latestJobId: jobId,
          uploadState: {
            status: "failed",
            failedAt: new Date().toISOString(),
            error: e.message,
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
    pushLog(jobId, `ERROR: ${e.message}`, e);
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
      const recommendation =
        dq?.overallRecommendation ??
        (typeof state?.exploreState?.recommendation === "string" ? state.exploreState.recommendation : "unknown");

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

      retailers.push({
        retailer,
        filename,
        config,
        recommendation,
        crawl,
        upload,
        uploadMatchesCurrentCrawl,
      });
    }

    const identifiedWithoutConfig: { name: string; url: string }[] = [];
    const backlogSeen = new Set<string>();

    let discoveredBrandsList: DiscoveredBrand[] = [];
    let discoveredUrlStrings: string[] = [];
    if (fs.existsSync(DISCOVERED_BRANDS_PATH)) {
      try {
        const raw = fs.readFileSync(DISCOVERED_BRANDS_PATH, "utf-8");
        const data = JSON.parse(raw) as { brands?: unknown; urls?: unknown };
        discoveredBrandsList = Array.isArray(data.brands) ? (data.brands as DiscoveredBrand[]) : [];
        discoveredUrlStrings = Array.isArray(data.urls) ? (data.urls as string[]) : [];
      } catch {
        // ignore parse errors
      }
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
    for (const u of discoveredUrlStrings) {
      if (typeof u !== "string") continue;
      const n = normalizeUrl(u);
      if (!n || backlogSeen.has(n)) continue;
      if (configBaseUrls.has(n)) continue;
      backlogSeen.add(n);
      identifiedWithoutConfig.push({ name: n, url: n });
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

// ---------------------------------------------------------------------------
// Crawl endpoints — use a generated config to discover all product URLs
// ---------------------------------------------------------------------------

app.post("/api/crawl/:retailer", async (req, res) => {
  const retailer = req.params.retailer;
  const config = await loadStoredConfig(retailer);
  if (!config) {
    res.status(404).json({ error: "Config not found." });
    return;
  }

  const id = crypto.randomUUID();
  const job = createJobRecord({
    id,
    kind: "crawl",
    retailer,
    urls: typeof config.baseUrl === "string" ? [config.baseUrl] : [],
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
        config as Parameters<typeof crawlProductUrls>[0],
        (msg) => pushLog(job.id, msg),
        (count) => pushEvent(job.id, { type: "crawl-progress", count }),
        {
          initialUrls,
          onCheckpoint: async (partial) => {
            await upsertRetailerPipelineState({
              retailer,
              baseUrl: typeof config.baseUrl === "string" ? config.baseUrl : undefined,
              displayName: typeof config.retailerDisplayName === "string" ? config.retailerDisplayName : retailer,
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
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[crawl job=${job.id}] Error crawling ${retailer}:`, err);
      pushLog(job.id, `ERROR: ${e.message}`, e);
      job.status = "error";
      job.error = e.message;
      persistJob(job);
      pushEvent(job.id, { type: "done", error: e.message });
    }

    closeAllSseClients(job.id);
  })();

  res.json({ jobId: id });
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
      pushLog(job.id, `ERROR: ${e.message}`, e);
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
// Serve the HTML UI
// ---------------------------------------------------------------------------

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "Ui.html"));
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  void (async () => {
    try {
      await ensurePipelinePersistenceSchema();
      await markRunningJobsInterrupted();
    } catch (err) {
      console.error("[startup] Failed to initialize pipeline persistence:", err);
    }
    try {
      syncConfigsToMasterList(CONFIGS_DIR);
    } catch (err) {
      console.error("[startup] Failed to sync configs to master list:", err);
    }
    console.log(`\n  Crawler Config UI running at http://localhost:${PORT}\n`);
  })();
});