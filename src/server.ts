import "dotenv/config";
import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { exploreRetailer } from "./explore.js";
import { crawlProductUrls, type CrawlResult } from "./crawl.js";
import { uploadRetailer, type UploadResult } from "./upload.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json());

const PORT = parseInt(process.env.PORT ?? "3456", 10);
const CONFIGS_DIR = process.env.CONFIGS_DIR ?? path.join(process.cwd(), "configs");
const PRODUCT_URLS_DIR = process.env.PRODUCT_URLS_DIR ?? path.join(process.cwd(), "product-urls");

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
}

const jobs = new Map<string, Job>();
const sseClients = new Map<string, Set<express.Response>>();

function pushLog(jobId: string, msg: string) {
  const job = jobs.get(jobId);
  if (job) job.logs.push(msg);

  const clients = sseClients.get(jobId);
  if (clients) {
    const data = `data: ${JSON.stringify({ type: "log", msg })}\n\n`;
    for (const res of clients) res.write(data);
  }
}

function pushEvent(jobId: string, event: Record<string, unknown>) {
  const clients = sseClients.get(jobId);
  if (clients) {
    const data = `data: ${JSON.stringify(event)}\n\n`;
    for (const res of clients) res.write(data);
  }
}

async function processJob(job: Job) {
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
      const config = await exploreRetailer(url, (msg) => pushLog(job.id, msg));
      job.results.push(config);
    } catch (err) {
      const msg = (err as Error).message ?? String(err);
      console.error(`[processJob] Error processing ${url}:`, err);
      pushLog(job.id, `ERROR processing ${url}: ${msg}`);
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
  const { urls } = req.body as { urls?: string[] };
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

  const id = crypto.randomUUID();
  const job: Job = {
    id,
    urls: cleanedUrls,
    current: 0,
    status: "running",
    logs: [],
    results: [],
  };
  jobs.set(id, job);

  processJob(job).catch((err) => {
    job.status = "error";
    job.error = (err as Error).message;
    console.error("[processJob] Error:", err);
  });

  res.json({ jobId: id });
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

  if (job.status === "done") {
    res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
    res.end();
    return;
  }

  if (!sseClients.has(job.id)) sseClients.set(job.id, new Set());
  sseClients.get(job.id)!.add(res);

  req.on("close", () => {
    sseClients.get(job.id)?.delete(res);
  });
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
      fs.writeFileSync(
        path.join(PRODUCT_URLS_DIR, `${retailer}.json`),
        JSON.stringify(result, null, 2),
      );

      job.results.push(result as unknown as Record<string, unknown>);
      job.status = "done";
      pushEvent(job.id, { type: "done", totalUrls: result.totalUrls });
    } catch (err) {
      const msg = (err as Error).message ?? String(err);
      console.error(`[crawl] Error crawling ${retailer}:`, err);
      pushLog(job.id, `ERROR: ${msg}`);
      job.status = "error";
      job.error = msg;
      pushEvent(job.id, { type: "done", error: msg });
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
      pushEvent(job.id, {
        type: "done",
        uploaded: result.uploaded,
        skipped: result.skipped,
        failed: result.failed,
        total: result.total,
      });
    } catch (err) {
      const msg = (err as Error).message ?? String(err);
      console.error(`[upload] Error uploading ${retailer}:`, err);
      pushLog(job.id, `ERROR: ${msg}`);
      job.status = "error";
      job.error = msg;
      pushEvent(job.id, { type: "done", error: msg });
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
  console.log(`\n  Crawler Config UI running at http://localhost:${PORT}\n`);
});