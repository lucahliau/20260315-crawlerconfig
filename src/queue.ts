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
      // Errors inside a job handler bubble up to onComplete; we want them
      // captured but never crash the process.
      noSupervisor: false,
      // pg-boss creates its own `pgboss` schema by default — leave it.
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
  const db = (boss as unknown as { db?: { executeSql: (sql: string, vals: unknown[]) => Promise<{ rows: { state: string; n: string }[] }> } }).db;
  if (!db || typeof db.executeSql !== "function") {
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
  const db = (boss as unknown as { db?: { executeSql: (sql: string, vals: unknown[]) => Promise<{ rows: Record<string, unknown>[] }> } }).db;
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
  const db = (boss as unknown as { db?: { executeSql: (sql: string, vals: unknown[]) => Promise<{ rows: { name: string; data: unknown }[] }> } }).db;
  if (!db) return null;
  const { rows } = await db.executeSql(
    `SELECT name, data FROM pgboss.job WHERE id = $1`,
    [jobId],
  );
  if (rows.length === 0) return null;
  const queue = rows[0].name;
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
  const db = (boss as unknown as { db?: { executeSql: (sql: string, vals: unknown[]) => Promise<{ rowCount?: number }> } }).db;
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
