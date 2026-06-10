import { Router, type Request, type Response } from "express";
import { S3Client, HeadBucketCommand } from "@aws-sdk/client-s3";
import { getPool } from "../pipelineStore.js";
import { getQueueStats } from "../queue.js";

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
      const [perRetailerR, embedRatesR, nobgRatesR] = await Promise.all([
        perRetailerQ,
        embedRatesQ,
        nobgRatesQ,
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
      res.json({
        totals,
        rates: {
          nobg: nobgRatesR.rows[0] ?? { last1h: 0, last24h: 0 },
          embeddings: embedRatesR.rows[0] ?? { last1h: 0, last24h: 0 },
        },
        perRetailer,
      });
    } catch (err) {
      console.error("[api/processing] Error:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load processing stats." });
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
    const limitMb = parseInt(process.env.SUPABASE_DB_LIMIT_MB ?? "500", 10);
    return {
      ok: true,
      latencyMs: Date.now() - t0,
      sizeMb: Math.round(bytes / 1024 / 1024),
      limitMb: Number.isFinite(limitMb) ? limitMb : 500,
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
