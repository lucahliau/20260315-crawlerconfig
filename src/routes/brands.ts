import { Router, type Request, type Response } from "express";
import crypto from "node:crypto";
import {
  listBrands,
  setBrandStatus,
  addToMasterList,
  isPipelineEligible,
  type BrandStatus,
  type DiscoveredBrand,
} from "../discoverBrands.js";
import { classifyPriceTier, probeAndRecordBrands, type PriceTier } from "../priceProbe.js";
import { mineAndWriteLeads, loadBrandLeads } from "../brandSources.js";
import { dbListDiscoveryRuns } from "../brandStore.js";

/**
 * Brand-curation API for the dashboard.
 *
 * Kept as a standalone router (mounted by server.ts) rather than inlined into the
 * 2400-line monolith. Read-only + instant writes only — long-running probe/mine
 * runs are triggered through the separate job/SSE endpoints.
 */

const VALID_STATUSES: readonly BrandStatus[] = ["candidate", "approved", "rejected"];

/** A brand enriched with curation-view fields the UI needs. */
interface BrandView extends DiscoveredBrand {
  /** Resolved curation state ("candidate" implied when absent for new entries). */
  effectiveStatus: BrandStatus;
  /** Price tier derived from priceSample, or "unknown" when unprobed. */
  tier: PriceTier;
  /** Whether this brand may enter the crawl pipeline. */
  eligible: boolean;
}

function toView(b: DiscoveredBrand): BrandView {
  const usd = b.priceSample?.usd;
  return {
    ...b,
    effectiveStatus: b.status ?? "candidate",
    tier: typeof usd === "number" ? classifyPriceTier(usd) : "unknown",
    eligible: isPipelineEligible(b),
  };
}

// ---------------------------------------------------------------------------
// Lightweight in-memory task registry for short-lived dashboard actions
// (mine stockists / re-check prices). These complete in seconds-to-minutes and
// don't need the durable Postgres-backed job replay the crawl pipeline uses, so
// they live here, decoupled from server.ts's heavy job system.
// ---------------------------------------------------------------------------

type TaskKind = "mine" | "probe";
type TaskStatus = "running" | "done" | "error";

interface Task {
  id: string;
  kind: TaskKind;
  status: TaskStatus;
  logs: string[];
  result?: unknown;
  error?: string;
  startedAt: string;
  finishedAt?: string;
  clients: Set<Response>;
}

const tasks = new Map<string, Task>();
const MAX_TASK_LOGS = 2000;

function sse(res: Response, payload: Record<string, unknown>) {
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function taskLog(task: Task, message: string) {
  task.logs.push(message);
  if (task.logs.length > MAX_TASK_LOGS) {
    task.logs.splice(0, task.logs.length - MAX_TASK_LOGS);
  }
  for (const res of task.clients) sse(res, { type: "log", message });
}

function finishTask(task: Task, outcome: { result?: unknown; error?: string }) {
  task.status = outcome.error ? "error" : "done";
  task.result = outcome.result;
  task.error = outcome.error;
  task.finishedAt = new Date().toISOString();
  const done: Record<string, unknown> = { type: "done", kind: task.kind };
  if (outcome.error) done.error = outcome.error;
  else done.result = outcome.result;
  for (const res of task.clients) {
    sse(res, done);
    res.end();
  }
  task.clients.clear();
}

/** Spawn a tracked task running `work`, streaming its onProgress lines over SSE. */
function startTask(
  kind: TaskKind,
  work: (onProgress: (msg: string) => void) => Promise<unknown>,
): Task {
  const task: Task = {
    id: crypto.randomUUID(),
    kind,
    status: "running",
    logs: [],
    startedAt: new Date().toISOString(),
    clients: new Set(),
  };
  tasks.set(task.id, task);
  void (async () => {
    try {
      const result = await work((msg) => taskLog(task, msg));
      finishTask(task, { result });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`[api/brands task=${task.id} kind=${kind}] Error:`, err);
      taskLog(task, `Error: ${message}`);
      finishTask(task, { error: message });
    }
  })();
  return task;
}

/** True if a task of this kind is already in flight (prevents racing master-list writes). */
function isKindRunning(kind: TaskKind): boolean {
  for (const t of tasks.values()) {
    if (t.kind === kind && t.status === "running") return true;
  }
  return false;
}

export interface BrandsRouterOptions {
  /**
   * Fired (and not awaited) after a brand is set to "approved" — server.ts uses
   * it to auto-generate a crawl config when the site is Shopify (zero AI cost).
   */
  onBrandApproved?: (url: string) => void;
}

export function createBrandsRouter(options: BrandsRouterOptions = {}): Router {
  const router = Router();

  /**
   * GET /api/brands — full curation list with derived tier + summary counts.
   * Optional ?status=candidate|approved|rejected and ?tier=accessible|... filters.
   */
  router.get("/api/brands", async (req: Request, res: Response) => {
    try {
      const all = (await listBrands()).map(toView);

      const statusFilter = String(req.query.status ?? "").trim();
      const tierFilter = String(req.query.tier ?? "").trim();
      let brands = all;
      if (VALID_STATUSES.includes(statusFilter as BrandStatus)) {
        brands = brands.filter((b) => b.effectiveStatus === statusFilter);
      }
      if (tierFilter) {
        brands = brands.filter((b) => b.tier === tierFilter);
      }

      const counts = {
        total: all.length,
        byStatus: { candidate: 0, approved: 0, rejected: 0 } as Record<BrandStatus, number>,
        byTier: { too_cheap: 0, accessible: 0, too_expensive: 0, unknown: 0 } as Record<
          PriceTier,
          number
        >,
      };
      for (const b of all) {
        counts.byStatus[b.effectiveStatus]++;
        counts.byTier[b.tier]++;
      }

      res.json({ brands, counts });
    } catch (err) {
      console.error("[api/brands] Error:", err);
      res.status(500).json({ error: "Failed to load brands." });
    }
  });

  /**
   * POST /api/brands/status — set curation state.
   * Body: { url: string, status: "candidate"|"approved"|"rejected" }
   */
  router.post("/api/brands/status", async (req: Request, res: Response) => {
    try {
      const url = typeof req.body?.url === "string" ? req.body.url.trim() : "";
      const status = req.body?.status as unknown;
      if (!url) {
        return res.status(400).json({ error: "Missing 'url'." });
      }
      if (!VALID_STATUSES.includes(status as BrandStatus)) {
        return res
          .status(400)
          .json({ error: `Invalid 'status'. Expected one of: ${VALID_STATUSES.join(", ")}.` });
      }
      const found = await setBrandStatus(url, status as BrandStatus);
      if (!found) {
        return res.status(404).json({ error: "Brand not found for that URL." });
      }
      if (status === "approved") options.onBrandApproved?.(url);
      res.json({ ok: true, url, status });
    } catch (err) {
      console.error("[api/brands/status] Error:", err);
      res.status(500).json({ error: "Failed to update brand status." });
    }
  });

  /**
   * POST /api/brands/add — promote a lead (or manual entry) into the master list as a candidate.
   * Body: { url: string, name?: string }
   */
  router.post("/api/brands/add", async (req: Request, res: Response) => {
    try {
      const url = typeof req.body?.url === "string" ? req.body.url.trim() : "";
      const name = typeof req.body?.name === "string" ? req.body.name.trim() : undefined;
      if (!url) {
        return res.status(400).json({ error: "Missing 'url'." });
      }
      await addToMasterList(url, name);
      res.json({ ok: true, url, name });
    } catch (err) {
      console.error("[api/brands/add] Error:", err);
      res.status(500).json({ error: "Failed to add brand." });
    }
  });

  /**
   * GET /api/brand-leads — the latest stockist-mining output (read-only artifact).
   * Returns { generatedAt, stockists, leads } or an empty shape if not yet generated.
   */
  router.get("/api/brand-leads", async (_req: Request, res: Response) => {
    try {
      const data = await loadBrandLeads();
      if (!data) {
        return res.json({ generatedAt: null, stockists: [], leads: [] });
      }
      res.json(data);
    } catch (err) {
      console.error("[api/brand-leads] Error:", err);
      res.status(500).json({ error: "Failed to load brand leads." });
    }
  });

  /**
   * GET /api/discovery-runs — recent discovery searches (category + counts), so
   * the UI can show "you already searched this" and avoid re-spending credits.
   */
  router.get("/api/discovery-runs", async (req: Request, res: Response) => {
    try {
      const limit = Number(req.query.limit ?? 50);
      const runs = await dbListDiscoveryRuns(Number.isFinite(limit) ? limit : 50);
      res.json({ runs });
    } catch (err) {
      console.error("[api/discovery-runs] Error:", err);
      res.status(500).json({ error: "Failed to load discovery runs." });
    }
  });

  /**
   * POST /api/brands/mine — kick off a stockist-mining run (zero AI).
   * Returns { taskId } immediately; stream progress at /api/brands/tasks/:taskId/stream.
   */
  router.post("/api/brands/mine", (_req: Request, res: Response) => {
    if (isKindRunning("mine")) {
      return res.status(409).json({ error: "A mining run is already in progress." });
    }
    const task = startTask("mine", async (onProgress) => {
      const file = await mineAndWriteLeads({ onProgress });
      onProgress(`Done — ${file.leads.length} leads from ${file.stockists.filter((s) => s.ok).length} stockists.`);
      return { leads: file.leads.length, stockists: file.stockists };
    });
    res.json({ taskId: task.id });
  });

  /**
   * POST /api/brands/probe — re-check prices for master-list brands (zero AI).
   * Body: { onlyCandidates?: boolean, force?: boolean }
   * Returns { taskId }; stream progress at /api/brands/tasks/:taskId/stream.
   */
  router.post("/api/brands/probe", (req: Request, res: Response) => {
    if (isKindRunning("probe")) {
      return res.status(409).json({ error: "A price re-check is already in progress." });
    }
    const onlyCandidates = req.body?.onlyCandidates !== false; // default true
    const force = req.body?.force === true;
    const task = startTask("probe", async (onProgress) => {
      onProgress(`Probing ${onlyCandidates ? "candidate" : "all"} brands${force ? " (force re-probe)" : ""}...`);
      const summary = await probeAndRecordBrands({ onlyCandidates, force, onProgress });
      onProgress(
        `Done — probed ${summary.probed}, unknown ${summary.unknown}, too pricey ${summary.byTier.too_expensive}.`,
      );
      return summary;
    });
    res.json({ taskId: task.id });
  });

  /**
   * GET /api/brands/tasks/:taskId/stream — SSE feed for a mine/probe task.
   * Replays buffered logs, then streams live lines, then a final { type:"done" }.
   */
  router.get("/api/brands/tasks/:taskId/stream", (req: Request, res: Response) => {
    const task = tasks.get(String(req.params.taskId));
    if (!task) {
      return res.status(404).json({ error: "Task not found." });
    }
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });

    // Replay everything buffered so a late subscriber sees full history.
    for (const message of task.logs) sse(res, { type: "log", message });

    if (task.status !== "running") {
      const done: Record<string, unknown> = { type: "done", kind: task.kind };
      if (task.error) done.error = task.error;
      else done.result = task.result;
      sse(res, done);
      return res.end();
    }

    task.clients.add(res);
    req.on("close", () => {
      task.clients.delete(res);
    });
  });

  return router;
}
