/**
 * Durable job queue layer built on pg-boss (Postgres-backed).
 *
 * Why this exists:
 *   The original pipeline runs uploads serially inside the orchestrator process
 *   (one `for` loop on one Node worker). That has two problems we hit at scale:
 *     1. A single failure stalls the whole loop with no easy retry.
 *     2. We cannot run a separate, beefier worker (e.g. on the M4) without
 *        also moving the orchestrator/dashboard there.
 *
 *   Putting per-URL upload work into a durable queue:
 *     - lets multiple workers (Railway, M4 laptop, anywhere with the
 *       DATABASE_URL) compete for jobs
 *     - gives us retries-with-backoff + dead-letter handling for free
 *     - is observable in the dashboard via getQueueSize / getDLQ
 *     - is *additive* — the existing in-process upload flow still works,
 *       this is opt-in via the new /api/upload/:retailer/enqueue endpoint.
 *
 * Notes on pg-boss v10:
 *   - Queues must be created explicitly via `createQueue` before send/work.
 *   - Failed jobs land in a `__state__failed` partition; we expose them as
 *     "dead letter" rows. retry-failed reschedules them.
 *   - Per-domain politeness uses `singletonKey` so two jobs on the same domain
 *     can't run concurrently on different workers (rate-limit guard).
 */
import PgBoss from "pg-boss";

let bossInstance: PgBoss | null = null;
let startPromise: Promise<PgBoss> | null = null;

export const QUEUES = {
  UPLOAD_URL: "upload-url",
  CRAWL_RETAILER: "crawl-retailer",
  /** Image background removal — bridged to the bgremover tools by the home worker. */
  PROCESS_NOBG: "process-nobg",
  /** CLIP embedding generation — bridged to embed_worker.py by the home worker. */
  PROCESS_EMBED: "process-embed",
  /** People-photo detection/cleanup — bridged to person_scan_worker.py by the home worker. */
  PROCESS_PERSON: "process-person",
  /** Control jobs claimed by the SERVER (weekly re-crawl sweep). */
  PIPELINE_SWEEP: "pipeline-sweep",
  /** Cheap price/sale refresh via Shopify /products.json — claimed by the SERVER. */
  PRICE_REFRESH: "price-refresh",
} as const;

export type QueueName = (typeof QUEUES)[keyof typeof QUEUES];

export interface UploadUrlJobData {
  retailer: string;
  url: string;
  /** The crawl checkpoint this URL came from (used for dedup + reporting). */
  crawlSourceCrawledAt: string;
  /** Domain string for per-domain singleton-key politeness. */
  domain: string;
  /**
   * Optional parent job ID — purely for tracing back to the dashboard's
   * pipeline_jobs row that originally fanned out this URL.
   */
  parentJobId?: string;
}

export interface CrawlRetailerJobData {
  retailer: string;
  parentJobId?: string;
}

export interface ProcessingJobData {
  kind: "nobg" | "embed" | "person";
  /**
   * Bounded batch size — keeps every job comfortably under the 2h expiry.
   * The worker re-enqueues a follow-up batch while backlog remains.
   */
  limit: number;
  /** True when enqueued by cron/auto-chain; the worker honors the pause flag. */
  sweep?: boolean;
  /** Loop-until-done guard — incremented on each chained re-enqueue. */
  chainDepth?: number;
  parentJobId?: string;
}

export interface PipelineSweepJobData {
  kind: "weekly-recrawl";
}

export interface PriceRefreshJobData {
  /** Single retailer, or absent for a full sweep over every configured retailer. */
  retailer?: string;
  /** True when enqueued by the daily cron. */
  sweep?: boolean;
}

/**
 * Enqueue a price refresh (single retailer or full sweep). Date-bucketed
 * singletonKey so a double-fire can't stack identical runs.
 */
export async function enqueuePriceRefresh(data: PriceRefreshJobData): Promise<string | null> {
  const boss = await getBoss();
  if (!boss) return null;
  const day = new Date().toISOString().slice(0, 10);
  const singletonKey = `price-refresh:${data.retailer ?? "all"}:${day}`;
  return boss.send(QUEUES.PRICE_REFRESH, data as unknown as Record<string, unknown>, {
    retryLimit: 1,
    retryDelay: 60,
    expireInHours: 4,
    singletonKey,
  });
}

/**
 * Enqueue a processing batch (background removal or embeddings).
 * Sweep/cron callers pass a date-bucketed `singletonKey` so a double-fire
 * can't stack identical jobs; chained re-enqueues pass none (a duplicate
 * batch that finds nothing to do no-ops in seconds).
 */
export async function enqueueProcessing(
  data: ProcessingJobData,
  opts?: { singletonKey?: string },
): Promise<string | null> {
  const boss = await getBoss();
  if (!boss) return null;
  const queue =
    data.kind === "nobg"
      ? QUEUES.PROCESS_NOBG
      : data.kind === "person"
        ? QUEUES.PROCESS_PERSON
        : QUEUES.PROCESS_EMBED;
  return boss.send(queue, data as unknown as Record<string, unknown>, {
    // Embed retries are cheap (DB-resumable; exit-124 MPS watchdog is expected
    // occasionally); nobg/person batches re-scan from the DB so a retry only
    // redoes whatever is still unprocessed.
    retryLimit: data.kind === "embed" ? 5 : 2,
    retryDelay: 30,
    retryBackoff: true,
    expireInHours: 2,
    ...(opts?.singletonKey ? { singletonKey: opts.singletonKey } : {}),
  });
}

/** Delete queued (not-yet-claimed) jobs for a processing queue. Active batches finish cleanly. */
export async function cancelQueuedProcessing(kind: "nobg" | "embed" | "person"): Promise<number> {
  const boss = await getBoss();
  if (!boss) return 0;
  const db = getBossDb(boss);
  if (!db) return 0;
  const queue =
    kind === "nobg"
      ? QUEUES.PROCESS_NOBG
      : kind === "person"
        ? QUEUES.PROCESS_PERSON
        : QUEUES.PROCESS_EMBED;
  const result = await db.executeSql(
    `DELETE FROM pgboss.job WHERE name = $1 AND state = 'created'`,
    [queue],
  );
  return result.rowCount ?? 0;
}

/**
 * Delete pending upload-url jobs for one retailer. Used when the worker
 * discovers a retailer has no valid config: rather than let every queued URL
 * churn through its full retry budget (each emitting an identical "No config"
 * issue), drop them in one shot. Only `created`/`retry` jobs are removed —
 * any in-flight `active` job finishes, and terminal rows are left for history.
 * Returns the number of rows deleted.
 */
export async function purgeQueuedUploadsForRetailer(retailer: string): Promise<number> {
  const boss = await getBoss();
  if (!boss) return 0;
  const db = getBossDb(boss);
  if (!db) return 0;
  const result = await db.executeSql(
    `DELETE FROM pgboss.job
       WHERE name = $1
         AND state IN ('created', 'retry')
         AND data->>'retailer' = $2`,
    [QUEUES.UPLOAD_URL, retailer],
  );
  return result.rowCount ?? 0;
}

/**
 * Start (or return) the singleton pg-boss instance. Safe to call repeatedly.
 * Returns null if DATABASE_URL is not configured — callers should fall back to
 * the in-process path in that case.
 */
export async function getBoss(): Promise<PgBoss | null> {
  if (bossInstance) return bossInstance;
  if (startPromise) return startPromise;
  const dbUrl = (process.env.DATABASE_URL ?? "").replace(/^["']+|["']+$/g, "");
  if (!dbUrl) {
    console.warn("[queue] DATABASE_URL not set — queue layer disabled.");
    return null;
  }
  startPromise = (async () => {
    const boss = new PgBoss({
      connectionString: dbUrl,
      // pg-boss creates its own `pgboss` schema by default — leave it. The
      // maintenance/scheduling supervisor runs by default (needed for
      // boss.schedule cron sweeps).
    });
    boss.on("error", (err) => {
      console.error("[pg-boss] error:", err);
    });
    await boss.start();
    // v10: queues must be created before send/work. createQueue is idempotent.
    for (const name of Object.values(QUEUES)) {
      try {
        await boss.createQueue(name);
      } catch (err) {
        // Already exists is fine; rethrow anything else.
        const msg = err instanceof Error ? err.message : String(err);
        if (!/already exists/i.test(msg)) {
          console.error(`[pg-boss] createQueue(${name}) failed:`, err);
        }
      }
    }
    bossInstance = boss;
    return boss;
  })();
  return startPromise;
}

/**
 * Enqueue one URL for upload. Idempotency: we set a singletonKey based on the
 * url itself so a fan-out that's accidentally fired twice does not create two
 * jobs for the same URL.
 */
export async function enqueueUploadUrl(data: UploadUrlJobData): Promise<string | null> {
  const boss = await getBoss();
  if (!boss) return null;
  // Per-URL idempotency: `singletonKey` blocks duplicate active jobs for the
  // same key. We hash retailer+url to avoid the 100-char limit.
  const singletonKey = `upload:${data.retailer}:${truncateKey(data.url)}`;
  const jobId = await boss.send(QUEUES.UPLOAD_URL, data, {
    retryLimit: 3,
    retryDelay: 30,
    retryBackoff: true,
    expireInHours: 2,
    singletonKey,
  });
  return jobId;
}

/**
 * Bulk enqueue. Returns counts of accepted vs rejected (duplicates).
 */
export async function enqueueUploadUrls(
  jobs: UploadUrlJobData[],
): Promise<{ accepted: number; duplicates: number }> {
  let accepted = 0;
  let duplicates = 0;
  for (const j of jobs) {
    const id = await enqueueUploadUrl(j);
    if (id) accepted++;
    else duplicates++;
  }
  return { accepted, duplicates };
}

function truncateKey(s: string): string {
  // pg-boss singletonKey is varchar — keep it well under any reasonable limit.
  if (s.length <= 80) return s;
  return s.slice(0, 40) + "…" + s.slice(-37);
}

interface BossDb {
  executeSql: (
    sql: string,
    vals: unknown[],
  ) => Promise<{ rows: Record<string, unknown>[]; rowCount?: number }>;
}

/**
 * Raw SQL access to the pgboss schema. v10 keeps the db on a PRIVATE field
 * (`#db`) — `(boss as any).db` is always undefined; the public accessor is
 * `getDb()`. (This was silently no-op'ing every raw-SQL helper here.)
 */
function getBossDb(boss: PgBoss): BossDb | null {
  const accessor = (boss as unknown as { getDb?: () => unknown }).getDb;
  const db = typeof accessor === "function" ? accessor.call(boss) : undefined;
  if (db && typeof (db as { executeSql?: unknown }).executeSql === "function") {
    return db as BossDb;
  }
  return null;
}

/**
 * Dashboard helpers.
 */
export interface QueueStats {
  name: string;
  /** Jobs available to be claimed. */
  waiting: number;
  /** Jobs currently being processed by a worker. */
  active: number;
  /** Jobs that failed and exhausted retries (dead-letter). */
  failed: number;
  /** Jobs that completed successfully. */
  completed: number;
}

export async function getQueueStats(): Promise<QueueStats[]> {
  const boss = await getBoss();
  if (!boss) return [];
  const stats: QueueStats[] = [];
  for (const name of Object.values(QUEUES)) {
    try {
      // pg-boss v10 getQueueSize returns waiting; we query the pgboss schema
      // directly for the other states because the public API is limited.
      const waiting = await boss.getQueueSize(name);
      const counts = await getStateCountsForQueue(name);
      stats.push({
        name,
        waiting,
        active: counts.active,
        failed: counts.failed,
        completed: counts.completed,
      });
    } catch (err) {
      console.error(`[queue] getQueueStats(${name}) failed:`, err);
      stats.push({ name, waiting: 0, active: 0, failed: 0, completed: 0 });
    }
  }
  return stats;
}

async function getStateCountsForQueue(
  queueName: string,
): Promise<{ active: number; failed: number; completed: number }> {
  const boss = await getBoss();
  if (!boss) return { active: 0, failed: 0, completed: 0 };
  // pg-boss exposes the underlying pool via getDb() in v10.
  const db = getBossDb(boss);
  if (!db) {
    return { active: 0, failed: 0, completed: 0 };
  }
  const { rows } = await db.executeSql(
    `SELECT state, COUNT(*)::text AS n FROM pgboss.job WHERE name = $1 GROUP BY state`,
    [queueName],
  );
  const out = { active: 0, failed: 0, completed: 0 };
  for (const r of rows) {
    const n = Number(r.n) || 0;
    if (r.state === "active") out.active = n;
    else if (r.state === "failed") out.failed = n;
    else if (r.state === "completed") out.completed = n;
  }
  return out;
}

export interface DeadLetterRow {
  id: string;
  name: string;
  data: unknown;
  output: unknown;
  retryCount: number;
  createdOn: string;
  completedOn: string | null;
}

export async function listDeadLetter(opts?: { limit?: number; queue?: string }): Promise<DeadLetterRow[]> {
  const boss = await getBoss();
  if (!boss) return [];
  const db = getBossDb(boss);
  if (!db) return [];
  const limit = Math.max(1, Math.min(opts?.limit ?? 50, 500));
  const params: unknown[] = [];
  let where = "WHERE state = 'failed'";
  if (opts?.queue) {
    params.push(opts.queue);
    where += ` AND name = $${params.length}`;
  }
  params.push(limit);
  const { rows } = await db.executeSql(
    `SELECT id::text, name, data, output, retry_count AS "retryCount",
            created_on AS "createdOn", completed_on AS "completedOn"
     FROM pgboss.job
     ${where}
     ORDER BY completed_on DESC NULLS LAST, created_on DESC
     LIMIT $${params.length}`,
    params,
  );
  return rows.map((r) => ({
    id: String(r.id),
    name: String(r.name),
    data: r.data,
    output: r.output,
    retryCount: Number(r.retryCount) || 0,
    createdOn: r.createdOn instanceof Date ? r.createdOn.toISOString() : String(r.createdOn ?? ""),
    completedOn: r.completedOn instanceof Date ? r.completedOn.toISOString() : (r.completedOn ? String(r.completedOn) : null),
  }));
}

/** Re-enqueue a failed job (copy its data into a new job). */
export async function retryDeadLetterJob(jobId: string): Promise<string | null> {
  const boss = await getBoss();
  if (!boss) return null;
  const db = getBossDb(boss);
  if (!db) return null;
  const { rows } = await db.executeSql(
    `SELECT name, data FROM pgboss.job WHERE id = $1`,
    [jobId],
  );
  if (rows.length === 0) return null;
  const queue = String(rows[0].name);
  const data = rows[0].data;
  // Don't reuse singletonKey — the failed job still occupies that slot. We
  // intentionally skip the dedup guard for a retry.
  return await boss.send(queue, data as Record<string, unknown>, {
    retryLimit: 3,
    retryDelay: 30,
    retryBackoff: true,
  });
}

export async function purgeCompletedOlderThan(hours: number): Promise<number> {
  const boss = await getBoss();
  if (!boss) return 0;
  const db = getBossDb(boss);
  if (!db) return 0;
  const result = await db.executeSql(
    `DELETE FROM pgboss.job
     WHERE state = 'completed' AND completed_on < NOW() - ($1::text || ' hours')::interval`,
    [String(hours)],
  );
  return result.rowCount ?? 0;
}

export async function stopBoss(): Promise<void> {
  if (!bossInstance) return;
  try {
    await bossInstance.stop({ graceful: true, wait: true });
  } catch (err) {
    console.error("[pg-boss] stop failed:", err);
  }
  bossInstance = null;
  startPromise = null;
}
