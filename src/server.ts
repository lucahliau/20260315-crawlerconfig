import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { exploreRetailer } from "./explore.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json());

const PORT = parseInt(process.env.PORT ?? "3456", 10);
const CONFIGS_DIR = path.join(process.cwd(), "configs");

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
// Serve the HTML UI
// ---------------------------------------------------------------------------

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "ui.html"));
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`\n  Crawler Config UI running at http://localhost:${PORT}\n`);
});