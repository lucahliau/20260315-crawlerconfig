import "dotenv/config";
import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { exploreRetailer } from "./explore.js";
import { crawlProductUrls, type CrawlResult } from "./crawl.js";
import { uploadRetailer, type UploadResult } from "./upload.js";
import { discoverBrands, syncConfigsToMasterList, type DiscoveredBrand } from "./discoverBrands.js";
import { writeJsonAtomic } from "./jsonFs.js";
import { retailerSlugFromUrl } from "./retailerSlug.js";
import { recordDiscoverUsage, recordExploreUsage, getCostMetrics } from "./usageLedger.js";

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

// Delay between processing consecutive URLs (helps with rate limits)
const INTER_URL_DELAY_MS = parseInt(process.env.INTER_URL_DELAY_MS ?? "5000", 10);

// ---------------------------------------------------------------------------
// Job tracking — one Stagehand browser at a time
// ---------------------------------------------------------------------------

interface Job {
  id: string;
  urls: string[];
  current: number;
  status: "running" | "done" | "error";
  logs: string[];
  results: Record<string, unknown>[];
  error?: string;
  discoveredBrands?: DiscoveredBrand[];
  e2eProgress?: Record<string, unknown>;
}

const jobs = new Map<string, Job>();
const sseClients = new Map<string, Set<express.Response>>();

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
  }
  appendJobNdjson(jobId, { type: "log", msg: full });

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
  }
  const clients = sseClients.get(jobId);
  if (clients) {
    const data = `data: ${JSON.stringify(event)}\n\n`;
    for (const res of clients) res.write(data);
  }
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

    job.current = i;
    const url = job.urls[i];
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
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[processJob job=${job.id}] Error processing ${url}:`, err);
      pushLog(job.id, `ERROR processing ${url}: ${e.message}`, e);
    }
  }

  job.status = "done";
  pushEvent(job.id, { type: "done" });

  const clients = sseClients.get(job.id);
  if (clients) {
    for (const res of clients) res.end();
    sseClients.delete(job.id);
  }
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

app.post("/api/explore", (req, res) => {
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
  const job: Job = {
    id,
    urls: toExplore,
    current: 0,
    status: "running",
    logs: [],
    results: [],
  };
  jobs.set(id, job);

  processJob(job, skippedUrls.length > 0 ? skippedUrls : undefined).catch((err) => {
    job.status = "error";
    job.error = (err as Error).message;
    console.error(`[processJob job=${job.id}] Error:`, err);
  });

  res.json({ jobId: id, skippedUrls, skippedCount: skippedUrls.length });
});

app.post("/api/discover-brands", (req, res) => {
  const id = crypto.randomUUID();
  const job: Job = {
    id,
    urls: [],
    current: 0,
    status: "running",
    logs: [],
    results: [],
  };
  jobs.set(id, job);

  (async () => {
    try {
      const result = await discoverBrands((msg) => {
        pushLog(job.id, msg);
        pushEvent(job.id, { type: "progress", message: msg });
      });
      recordDiscoverUsage({
        estimatedUsd: result.estimatedUsd,
        inputTokens: result.usage.input_tokens,
        outputTokens: result.usage.output_tokens,
        webSearchRequests: result.usage.server_tool_use?.web_search_requests ?? 0,
      });
      job.discoveredBrands = result.brands;
      job.status = "done";
      pushEvent(job.id, { type: "brands", brands: result.brands });
      pushEvent(job.id, { type: "done", brands: result.brands });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[discover-brands job=${job.id}] Error:`, err);
      pushLog(job.id, `ERROR: ${e.message}`, e);
      job.status = "error";
      job.error = e.message;
      pushEvent(job.id, { type: "done", error: e.message });
    }

    const clients = sseClients.get(job.id);
    if (clients) {
      for (const res of clients) res.end();
      sseClients.delete(job.id);
    }
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

async function runE2EOrchestrator(jobId: string) {
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
    pushLog(jobId, "\n========== Step 1: Discovering brands (Claude search) ==========\n");

    const discoverResult = await discoverBrands((msg) => pushLog(jobId, msg));
    recordDiscoverUsage({
      estimatedUsd: discoverResult.estimatedUsd,
      inputTokens: discoverResult.usage.input_tokens,
      outputTokens: discoverResult.usage.output_tokens,
      webSearchRequests: discoverResult.usage.server_tool_use?.web_search_requests ?? 0,
      e2eJobId: jobId,
    });
    const brands = discoverResult.brands;
    if (!brands || brands.length === 0) {
      throw new Error("No brands discovered.");
    }

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
        if (rec === "recommended") {
          recommendedConfigs.push({ retailer, config });
        }
        pushLog(jobId, `  Config for ${brand.name}: ${rec ?? "unknown"}\n`);
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        pushLog(jobId, `  ERROR exploring ${brand.name}: ${e.message}\n`, e);
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
      const clients = sseClients.get(jobId);
      if (clients) {
        for (const res of clients) res.end();
        sseClients.delete(jobId);
      }
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
        const result = await crawlProductUrls(
          config as Parameters<typeof crawlProductUrls>[0],
          (msg) => pushLog(jobId, msg),
        );
        fs.mkdirSync(PRODUCT_URLS_DIR, { recursive: true });
        writeJsonAtomic(path.join(PRODUCT_URLS_DIR, `${retailer}.json`), result);
        productUrlsTotal += result.totalUrls;
        crawlResults.push({ retailer, config, urls: result.urls });
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
        const result = await uploadRetailer(
          config as Parameters<typeof uploadRetailer>[0],
          urls,
          (msg) => pushLog(jobId, msg),
        );
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
    pushLog(jobId, `ERROR: ${e.message}`, e);
    pushEvent(jobId, { type: "done", error: e.message });
  }

  const clients = sseClients.get(jobId);
  if (clients) {
    for (const res of clients) res.end();
    sseClients.delete(jobId);
  }
}

app.post("/api/run-e2e", (_req, res) => {
  const id = crypto.randomUUID();
  const job: Job = {
    id,
    urls: [],
    current: 0,
    status: "running",
    logs: [],
    results: [],
  };
  jobs.set(id, job);

  runE2EOrchestrator(id).catch((err) => {
    console.error(`[run-e2e job=${id}] Unhandled error:`, err);
    const job = jobs.get(id);
    if (job) {
      job.status = "error";
      job.error = (err as Error).message;
      pushEvent(id, { type: "done", error: (err as Error).message });
    }
    const clients = sseClients.get(id);
    if (clients) {
      for (const res of clients) res.end();
      sseClients.delete(id);
    }
  });

  res.json({ jobId: id });
});

app.get("/api/job/:jobId", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) {
    res.status(404).json({ error: "Job not found." });
    return;
  }
  res.json({ exists: true, status: job.status });
});

app.get("/api/progress/:jobId", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) {
    res.status(404).json({ error: "Job not found." });
    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });

  // Replay existing logs
  for (const msg of job.logs) {
    res.write(`data: ${JSON.stringify({ type: "log", msg })}\n\n`);
  }

  // Replay last e2e-progress for late-joining clients
  if (job.e2eProgress) {
    res.write(`data: ${JSON.stringify(job.e2eProgress)}\n\n`);
  }

  if (job.status === "done") {
    const doneEvent: Record<string, unknown> = { type: "done" };
    if (job.discoveredBrands) doneEvent.brands = job.discoveredBrands;
    if (job.error) doneEvent.error = job.error;
    res.write(`data: ${JSON.stringify(doneEvent)}\n\n`);
    res.end();
    return;
  }

  if (!sseClients.has(job.id)) sseClients.set(job.id, new Set());
  sseClients.get(job.id)!.add(res);

  req.on("close", () => {
    sseClients.get(job.id)?.delete(res);
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
    const data = JSON.parse(raw) as { brands?: unknown[]; urls?: unknown[] };
    res.json({
      brands: Array.isArray(data.brands) ? data.brands : [],
      urls: Array.isArray(data.urls) ? data.urls : [],
    });
  } catch (err) {
    console.error("[api/discovered-brands] Error:", err);
    res.status(500).json({ error: "Failed to load discovered brands." });
  }
});

app.get("/api/retailers-overview", (_req, res) => {
  try {
    fs.mkdirSync(CONFIGS_DIR, { recursive: true });
    const files = fs.readdirSync(CONFIGS_DIR).filter((f) => f.endsWith(".json"));

    const retailers = files.map((filename) => {
      const fromFile = filename.replace(/\.json$/, "");
      const raw = fs.readFileSync(path.join(CONFIGS_DIR, filename), "utf-8");
      let config: Record<string, unknown> | null = null;
      try {
        config = JSON.parse(raw) as Record<string, unknown>;
      } catch {
        return null;
      }
      const retailer = (typeof config.retailer === "string" ? config.retailer : null) ?? fromFile;
      const dq = config.dataQuality as { overallRecommendation?: string } | undefined;
      const recommendation = dq?.overallRecommendation ?? "unknown";

      let crawl: { totalUrls: number; crawledAt: string; method: string } | null = null;
      const puPath = path.join(PRODUCT_URLS_DIR, `${retailer}.json`);
      if (fs.existsSync(puPath)) {
        try {
          const pu = JSON.parse(fs.readFileSync(puPath, "utf-8")) as CrawlResult;
          crawl = {
            totalUrls: pu.totalUrls,
            crawledAt: pu.crawledAt,
            method: pu.method,
          };
        } catch {
          // ignore
        }
      }

      let upload: {
        retailer: string;
        uploadedAt: string;
        uploaded: number;
        skipped: number;
        failed: number;
        total: number;
        crawlSourceCrawledAt: string;
      } | null = null;
      const upPath = path.join(UPLOAD_STATUS_DIR, `${retailer}.json`);
      if (fs.existsSync(upPath)) {
        try {
          upload = JSON.parse(fs.readFileSync(upPath, "utf-8"));
        } catch {
          // ignore
        }
      }

      const uploadMatchesCurrentCrawl = !!(
        crawl &&
        upload &&
        upload.crawlSourceCrawledAt === crawl.crawledAt
      );

      return {
        retailer,
        filename,
        config,
        recommendation,
        crawl,
        upload,
        uploadMatchesCurrentCrawl,
      };
    }).filter((x): x is NonNullable<typeof x> => x != null);

    res.json({ retailers });
  } catch (err) {
    console.error("[api/retailers-overview] Error:", err);
    res.status(500).json({ error: "Failed to load retailers overview." });
  }
});

app.get("/api/configs", (_req, res) => {
  try {
    fs.mkdirSync(CONFIGS_DIR, { recursive: true });
    const files = fs
      .readdirSync(CONFIGS_DIR)
      .filter((f) => f.endsWith(".json"));

    const configs = files.map((filename) => {
      const raw = fs.readFileSync(path.join(CONFIGS_DIR, filename), "utf-8");
      try {
        const config = JSON.parse(raw);
        return { retailer: config.retailer ?? filename.replace(".json", ""), filename, config };
      } catch (e) {
        console.error("[api/configs] Failed to parse config:", filename, e);
        return { retailer: filename.replace(".json", ""), filename, config: null };
      }
    });

    res.json(configs);
  } catch (err) {
    console.error("[api/configs] Error:", err);
    res.status(500).json({ error: "Failed to load configs." });
  }
});

app.get("/api/configs/:retailer", (req, res) => {
  const filePath = path.join(CONFIGS_DIR, `${req.params.retailer}.json`);
  if (!fs.existsSync(filePath)) {
    res.status(404).json({ error: "Config not found." });
    return;
  }
  res.sendFile(filePath);
});

// ---------------------------------------------------------------------------
// Crawl endpoints — use a generated config to discover all product URLs
// ---------------------------------------------------------------------------

app.post("/api/crawl/:retailer", (req, res) => {
  const retailer = req.params.retailer;
  const configPath = path.join(CONFIGS_DIR, `${retailer}.json`);
  if (!fs.existsSync(configPath)) {
    res.status(404).json({ error: "Config not found." });
    return;
  }

  let config;
  try {
    config = JSON.parse(fs.readFileSync(configPath, "utf-8"));
  } catch {
    res.status(500).json({ error: "Failed to parse config." });
    return;
  }

  const id = crypto.randomUUID();
  const job: Job = {
    id,
    urls: [config.baseUrl],
    current: 0,
    status: "running",
    logs: [],
    results: [],
  };
  jobs.set(id, job);

  (async () => {
    try {
      const result = await crawlProductUrls(
        config,
        (msg) => pushLog(job.id, msg),
        (count) => pushEvent(job.id, { type: "crawl-progress", count }),
      );

      fs.mkdirSync(PRODUCT_URLS_DIR, { recursive: true });
      writeJsonAtomic(path.join(PRODUCT_URLS_DIR, `${retailer}.json`), result);

      job.results.push(result as unknown as Record<string, unknown>);
      job.status = "done";
      pushEvent(job.id, { type: "done", totalUrls: result.totalUrls });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error(`[crawl job=${job.id}] Error crawling ${retailer}:`, err);
      pushLog(job.id, `ERROR: ${e.message}`, e);
      job.status = "error";
      job.error = e.message;
      pushEvent(job.id, { type: "done", error: e.message });
    }

    const clients = sseClients.get(job.id);
    if (clients) {
      for (const res of clients) res.end();
      sseClients.delete(job.id);
    }
  })();

  res.json({ jobId: id });
});

app.get("/api/product-urls", (_req, res) => {
  try {
    fs.mkdirSync(PRODUCT_URLS_DIR, { recursive: true });
    const files = fs
      .readdirSync(PRODUCT_URLS_DIR)
      .filter((f) => f.endsWith(".json"));

    const results = files.map((filename) => {
      const raw = fs.readFileSync(path.join(PRODUCT_URLS_DIR, filename), "utf-8");
      try {
        const data = JSON.parse(raw) as CrawlResult;
        return {
          retailer: data.retailer,
          crawledAt: data.crawledAt,
          method: data.method,
          totalUrls: data.totalUrls,
        };
      } catch {
        return null;
      }
    }).filter(Boolean);

    res.json(results);
  } catch (err) {
    console.error("[api/product-urls] Error:", err);
    res.status(500).json({ error: "Failed to load product URLs." });
  }
});

app.get("/api/product-urls/:retailer", (req, res) => {
  const filePath = path.join(PRODUCT_URLS_DIR, `${req.params.retailer}.json`);
  if (!fs.existsSync(filePath)) {
    res.status(404).json({ error: "Product URLs not found." });
    return;
  }
  try {
    const data = JSON.parse(fs.readFileSync(filePath, "utf-8"));
    res.json(data);
  } catch {
    res.status(500).json({ error: "Failed to read product URLs." });
  }
});

// ---------------------------------------------------------------------------
// Upload endpoints — scrape product URLs, upload images to R2, upsert to DB
// ---------------------------------------------------------------------------

app.post("/api/upload/:retailer", (req, res) => {
  const retailer = req.params.retailer;
  const configPath = path.join(CONFIGS_DIR, `${retailer}.json`);
  if (!fs.existsSync(configPath)) {
    res.status(404).json({ error: "Config not found." });
    return;
  }

  const urlsPath = path.join(PRODUCT_URLS_DIR, `${retailer}.json`);
  if (!fs.existsSync(urlsPath)) {
    res.status(404).json({ error: "Product URLs not found. Crawl first." });
    return;
  }

  let config;
  try {
    config = JSON.parse(fs.readFileSync(configPath, "utf-8"));
  } catch {
    res.status(500).json({ error: "Failed to parse config." });
    return;
  }

  let urlsData;
  try {
    urlsData = JSON.parse(fs.readFileSync(urlsPath, "utf-8")) as CrawlResult;
  } catch {
    res.status(500).json({ error: "Failed to parse product URLs." });
    return;
  }

  if (!urlsData.urls || urlsData.urls.length === 0) {
    res.status(400).json({ error: "No product URLs available." });
    return;
  }

  const id = crypto.randomUUID();
  const job: Job = {
    id,
    urls: urlsData.urls,
    current: 0,
    status: "running",
    logs: [],
    results: [],
  };
  jobs.set(id, job);

  (async () => {
    try {
      const result = await uploadRetailer(
        config,
        urlsData.urls,
        (msg) => pushLog(job.id, msg),
        (progress) => pushEvent(job.id, {
          type: "upload-progress",
          uploaded: progress.uploaded,
          skipped: progress.skipped,
          failed: progress.failed,
          total: progress.total,
          currentUrl: progress.currentUrl,
        }),
      );

      job.results.push(result as unknown as Record<string, unknown>);
      job.status = "done";

      writeJsonAtomic(path.join(UPLOAD_STATUS_DIR, `${retailer}.json`), {
        retailer,
        uploadedAt: result.uploadedAt,
        uploaded: result.uploaded,
        skipped: result.skipped,
        failed: result.failed,
        total: result.total,
        crawlSourceCrawledAt: urlsData.crawledAt,
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
      pushEvent(job.id, { type: "done", error: e.message });
    }

    const clients = sseClients.get(job.id);
    if (clients) {
      for (const res of clients) res.end();
      sseClients.delete(job.id);
    }
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
  try {
    syncConfigsToMasterList(CONFIGS_DIR);
  } catch (err) {
    console.error("[startup] Failed to sync configs to master list:", err);
  }
  console.log(`\n  Crawler Config UI running at http://localhost:${PORT}\n`);
});