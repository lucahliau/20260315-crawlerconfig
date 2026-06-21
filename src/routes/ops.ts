import { Router, type Request, type Response } from "express";
import { S3Client, HeadBucketCommand } from "@aws-sdk/client-s3";
import {
  getPool,
  getSetting,
  listWorkerHeartbeats,
  setSetting,
} from "../pipelineStore.js";
import {
  cancelQueuedProcessing,
  enqueueProcessing,
  getQueueStats,
  QUEUES,
} from "../queue.js";
import { getProcessingBacklog } from "../processing/bridge.js";

/**
 * Operational endpoints for the dashboard's Process and Systems tabs.
 *
 * Process — post-crawl work that runs on the MacBook (background removal,
 * CLIP embeddings). Those workers write to the shared Supabase DB
 * (ClothingItem.hasNobg, ItemEmbedding rows), so coverage and throughput are
 * computed here from Postgres — no coupling to the laptop's local progress files.
 *
 * Systems — cheap health/usage checks across the third parties (Supabase,
 * Railway backend, R2, job queue) so one glance answers "is everything up".
 */

interface ProcessingRetailerRow {
  retailer: string;
  total: number;
  nobg: number;
  embedded: number;
}

export function createOpsRouter(): Router {
  const router = Router();

  router.get("/api/processing", async (_req: Request, res: Response) => {
    const pg = getPool();
    if (!pg) {
      return res.status(503).json({ error: "DATABASE_URL not configured." });
    }
    try {
      const perRetailerQ = pg.query(`
        SELECT ci.retailer,
               COUNT(*)::int                                            AS total,
               COUNT(*) FILTER (WHERE ci."hasNobg" = true)::int         AS nobg,
               COUNT(*) FILTER (WHERE e."itemId" IS NOT NULL)::int      AS embedded
        FROM "ClothingItem" ci
        LEFT JOIN (SELECT DISTINCT "itemId" FROM "ItemEmbedding") e ON e."itemId" = ci.id
        GROUP BY ci.retailer
        ORDER BY total DESC
      `);
      const embedRatesQ = pg.query(`
        SELECT COUNT(*) FILTER (WHERE "embeddedAt" > NOW() - interval '1 hour')::int  AS last1h,
               COUNT(*) FILTER (WHERE "embeddedAt" > NOW() - interval '24 hours')::int AS last24h
        FROM "ItemEmbedding"
      `);
      // hasNobg has no dedicated timestamp; updatedAt moves when the flag is set,
      // which makes it a serviceable throughput proxy while the worker runs.
      const nobgRatesQ = pg.query(`
        SELECT COUNT(*) FILTER (WHERE "updatedAt" > NOW() - interval '1 hour')::int   AS last1h,
               COUNT(*) FILTER (WHERE "updatedAt" > NOW() - interval '24 hours')::int AS last24h
        FROM "ClothingItem"
        WHERE "hasNobg" = true
      `);
      // People-photo scan coverage. Guarded (.catch) so a pre-migration DB
      // (no hasPerson/personScannedAt columns yet) returns zeros instead of
      // 500-ing the whole Process tab.
      const personStatsQ = pg
        .query(`
          SELECT COUNT(*) FILTER (WHERE "personScannedAt" IS NOT NULL)::int            AS scanned,
                 COUNT(*) FILTER (WHERE "hasPerson" = true)::int                        AS hidden,
                 COUNT(*) FILTER (WHERE "personScannedAt" > NOW() - interval '1 hour')::int  AS last1h,
                 COUNT(*) FILTER (WHERE "personScannedAt" > NOW() - interval '24 hours')::int AS last24h
          FROM "ClothingItem"
        `)
        .catch(() => ({ rows: [{ scanned: 0, hidden: 0, last1h: 0, last24h: 0 }] }));
      const [perRetailerR, embedRatesR, nobgRatesR, personStatsR, backlog, allQueues, workers] =
        await Promise.all([
          perRetailerQ,
          embedRatesQ,
          nobgRatesQ,
          personStatsQ,
          getProcessingBacklog().catch(() => ({ needsNobg: 0, needsEmbed: 0, needsPerson: 0 })),
          getQueueStats().catch(() => []),
          listWorkerHeartbeats().catch(() => []),
        ]);
      const perRetailer = perRetailerR.rows as ProcessingRetailerRow[];
      const totals = perRetailer.reduce(
        (acc, r) => ({
          total: acc.total + r.total,
          nobg: acc.nobg + r.nobg,
          embedded: acc.embedded + r.embedded,
        }),
        { total: 0, nobg: 0, embedded: 0 },
      );
      const processingQueues = allQueues.filter(
        (q) =>
          q.name === QUEUES.PROCESS_NOBG ||
          q.name === QUEUES.PROCESS_EMBED ||
          q.name === QUEUES.PROCESS_PERSON,
      );
      const personRow = (personStatsR.rows[0] ?? { scanned: 0, hidden: 0, last1h: 0, last24h: 0 }) as {
        scanned: number;
        hidden: number;
        last1h: number;
        last24h: number;
      };
      // "Home server online" = a fresh heartbeat from a worker that claims a
      // processing queue (heartbeats every 15s; stale after 120s).
      const homeServerOnline = workers.some(
        (w) =>
          w.ageSeconds <= 120 &&
          Array.isArray((w.metadata as { queues?: unknown }).queues) &&
          ((w.metadata as { queues: string[] }).queues ?? []).includes(QUEUES.PROCESS_NOBG),
      );
      res.json({
        totals,
        rates: {
          nobg: nobgRatesR.rows[0] ?? { last1h: 0, last24h: 0 },
          embeddings: embedRatesR.rows[0] ?? { last1h: 0, last24h: 0 },
          person: { last1h: personRow.last1h, last24h: personRow.last24h },
        },
        person: {
          scanned: personRow.scanned,
          hidden: personRow.hidden,
          needsScan: backlog.needsPerson ?? 0,
        },
        perRetailer,
        backlog,
        queues: processingQueues,
        workers,
        homeServerOnline,
      });
    } catch (err) {
      console.error("[api/processing] Error:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load processing stats." });
    }
  });

  // Enqueue processing batches (claimed by the home worker). Manual runs are
  // sweep:false so they bypass the conveyor pause flag.
  router.post("/api/processing/run", async (req: Request, res: Response) => {
    const kind = req.body?.kind as string | undefined;
    if (!kind || !["nobg", "embed", "person", "all"].includes(kind)) {
      return res.status(400).json({ error: 'kind must be "nobg", "embed", "person" or "all"' });
    }
    const nobgDefault = Math.max(1, parseInt(process.env.PROCESS_NOBG_DEFAULT_LIMIT ?? "100", 10));
    const embedDefault = Math.max(1, parseInt(process.env.PROCESS_EMBED_DEFAULT_LIMIT ?? "2000", 10));
    const personDefault = Math.max(1, parseInt(process.env.PROCESS_PERSON_DEFAULT_LIMIT ?? "500", 10));
    const rawLimit = Number(req.body?.limit);
    try {
      const jobIds: Record<string, string | null> = {};
      if (kind === "nobg" || kind === "all") {
        const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.floor(rawLimit) : nobgDefault;
        jobIds.nobg = await enqueueProcessing({ kind: "nobg", limit });
      }
      if (kind === "embed" || kind === "all") {
        const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.floor(rawLimit) : embedDefault;
        jobIds.embed = await enqueueProcessing({ kind: "embed", limit });
      }
      if (kind === "person" || kind === "all") {
        const limit =
          Number.isFinite(rawLimit) && rawLimit > 0 ? Math.floor(rawLimit) : personDefault;
        jobIds.person = await enqueueProcessing({ kind: "person", limit });
      }
      res.json({ jobIds });
    } catch (err) {
      console.error("[api/processing/run] Error:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to enqueue." });
    }
  });

  // Drop queued (unclaimed) processing jobs; an active batch finishes cleanly.
  router.post("/api/processing/cancel", async (req: Request, res: Response) => {
    const kind = req.body?.kind as string | undefined;
    if (!kind || !["nobg", "embed", "person"].includes(kind)) {
      return res.status(400).json({ error: 'kind must be "nobg", "embed" or "person"' });
    }
    try {
      const cancelled = await cancelQueuedProcessing(kind as "nobg" | "embed" | "person");
      res.json({ cancelled });
    } catch (err) {
      console.error("[api/processing/cancel] Error:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to cancel." });
    }
  });

  // Conveyor settings: kill switch + per-stage gates.
  const SETTING_KEYS = ["auto_pipeline", "gate_crawl", "gate_upload", "gate_processing"] as const;

  router.get("/api/pipeline/settings", async (_req: Request, res: Response) => {
    try {
      const autoDefault = process.env.AUTO_PIPELINE !== "false";
      const out: Record<string, boolean> = {};
      for (const key of SETTING_KEYS) {
        out[key] = await getSetting<boolean>(key, key === "auto_pipeline" ? autoDefault : true);
      }
      res.json(out);
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load settings." });
    }
  });

  // Per-retailer autopilot opt-out (read by the auto-chain gates).
  router.get("/api/pipeline/optout/:retailer", async (req: Request, res: Response) => {
    try {
      const optout = await getSetting<boolean>(`autopilot_optout:${req.params.retailer}`, false);
      res.json({ retailer: req.params.retailer, optout });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load opt-out." });
    }
  });

  router.post("/api/pipeline/optout", async (req: Request, res: Response) => {
    const retailer = req.body?.retailer as string | undefined;
    const optout = req.body?.optout;
    if (!retailer || typeof optout !== "boolean") {
      return res.status(400).json({ error: "Body must be {retailer, optout: boolean}" });
    }
    try {
      await setSetting(`autopilot_optout:${retailer}`, optout);
      res.json({ retailer, optout });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to save opt-out." });
    }
  });

  router.post("/api/pipeline/settings", async (req: Request, res: Response) => {
    try {
      const patch: Record<string, boolean> = {};
      for (const key of SETTING_KEYS) {
        const v = req.body?.[key];
        if (typeof v === "boolean") {
          await setSetting(key, v);
          patch[key] = v;
        }
      }
      res.json({ updated: patch });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to save settings." });
    }
  });

  router.get("/api/systems", async (_req: Request, res: Response) => {
    const checks = await Promise.allSettled([
      checkDatabase(),
      checkBackend(),
      checkR2(),
      checkQueue(),
    ]);
    const [db, backend, r2, queue] = checks.map((c) =>
      c.status === "fulfilled" ? c.value : { ok: false, error: String(c.reason) },
    );
    res.json({
      checkedAt: new Date().toISOString(),
      crawler: {
        ok: true,
        uptimeSeconds: Math.round(process.uptime()),
        rssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
      },
      database: db,
      backend,
      r2,
      queue,
    });
  });

  return router;
}

async function checkDatabase(): Promise<Record<string, unknown>> {
  const pg = getPool();
  if (!pg) return { ok: false, error: "DATABASE_URL not configured" };
  const t0 = Date.now();
  try {
    const sizeR = await pg.query(
      `SELECT pg_database_size(current_database())::bigint AS bytes,
              (SELECT COUNT(*)::int FROM "ClothingItem") AS items`,
    );
    const bytes = Number(sizeR.rows[0]?.bytes ?? 0);
    // Supabase Pro plan: 8 GB disk included (auto-scales beyond that). The DB outgrew
    // the old 500 MB free-tier cap (measured 693 MB on 2026-06-20), so the base now
    // reflects the Pro allotment. Override with SUPABASE_DB_LIMIT_MB if the disk
    // auto-scales past 8 GB.
    const SUPABASE_PLAN_DISK_MB = 8192;
    const limitMb = parseInt(process.env.SUPABASE_DB_LIMIT_MB ?? String(SUPABASE_PLAN_DISK_MB), 10);
    return {
      ok: true,
      latencyMs: Date.now() - t0,
      sizeMb: Math.round(bytes / 1024 / 1024),
      limitMb: Number.isFinite(limitMb) ? limitMb : SUPABASE_PLAN_DISK_MB,
      items: sizeR.rows[0]?.items ?? 0,
    };
  } catch (err) {
    return { ok: false, latencyMs: Date.now() - t0, error: err instanceof Error ? err.message : String(err) };
  }
}

async function checkBackend(): Promise<Record<string, unknown>> {
  const url =
    process.env.BACKEND_HEALTH_URL ??
    "https://20260311-clothes-backend-production.up.railway.app/health";
  const t0 = Date.now();
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    return { ok: res.ok, latencyMs: Date.now() - t0, status: res.status, url };
  } catch (err) {
    return { ok: false, latencyMs: Date.now() - t0, error: err instanceof Error ? err.message : String(err), url };
  }
}

async function checkR2(): Promise<Record<string, unknown>> {
  const { R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME } = process.env;
  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET_NAME) {
    return { ok: false, error: "R2 credentials not configured" };
  }
  const t0 = Date.now();
  try {
    const s3 = new S3Client({
      region: "auto",
      endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
    });
    await s3.send(new HeadBucketCommand({ Bucket: R2_BUCKET_NAME }));
    return { ok: true, latencyMs: Date.now() - t0, bucket: R2_BUCKET_NAME };
  } catch (err) {
    return { ok: false, latencyMs: Date.now() - t0, error: err instanceof Error ? err.message : String(err) };
  }
}

async function checkQueue(): Promise<Record<string, unknown>> {
  try {
    const queues = await getQueueStats();
    return { ok: true, queues };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  }
}
