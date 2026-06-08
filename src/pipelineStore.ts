import { Pool } from "pg";

export type PipelineJobKind = "explore" | "discover" | "crawl" | "upload" | "e2e";
export type PipelineJobStatus = "running" | "done" | "error" | "interrupted";

export interface PipelineJobRecord {
  id: string;
  kind: PipelineJobKind;
  retailer: string | null;
  urls: string[];
  current: number;
  status: PipelineJobStatus;
  logs: string[];
  results: Record<string, unknown>[];
  error?: string;
  discoveredBrands?: unknown[];
  e2eProgress?: Record<string, unknown>;
  discoverModelResponse?: string;
  meta?: Record<string, unknown>;
  currentUrl?: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface RetailerPipelineState {
  retailer: string;
  baseUrl?: string;
  displayName?: string;
  latestJobId?: string;
  discoveryState?: Record<string, unknown>;
  exploreState?: Record<string, unknown>;
  crawlState?: Record<string, unknown>;
  uploadState?: Record<string, unknown>;
  updatedAt?: string;
}

export interface UploadUrlResultRecord {
  retailer: string;
  crawlSourceCrawledAt: string;
  url: string;
  jobId: string;
  status: "uploaded" | "skipped" | "failed";
  externalId?: string;
  itemName?: string;
  imageCount?: number;
  uploadedToR2?: boolean;
  upsertedToDb?: boolean;
  error?: string;
  metadata?: Record<string, unknown>;
}

let pool: Pool | null = null;
let schemaReady: Promise<void> | null = null;

function getPool(): Pool | null {
  if (pool) return pool;
  const dbUrl = (process.env.DATABASE_URL ?? "").replace(/^["']+|["']+$/g, "");
  if (!dbUrl) return null;
  pool = new Pool({
    connectionString: dbUrl,
    max: 5,
    idleTimeoutMillis: 30_000,
  });
  return pool;
}

function rowToJob(row: Record<string, unknown>): PipelineJobRecord {
  return {
    id: String(row.id),
    kind: row.kind as PipelineJobKind,
    retailer: typeof row.retailer === "string" ? row.retailer : null,
    urls: Array.isArray(row.urls) ? (row.urls as string[]) : [],
    current: typeof row.current_index === "number" ? row.current_index : 0,
    currentUrl: typeof row.current_url === "string" ? row.current_url : undefined,
    status: row.status as PipelineJobStatus,
    logs: Array.isArray(row.logs) ? (row.logs as string[]) : [],
    results: Array.isArray(row.results) ? (row.results as Record<string, unknown>[]) : [],
    error: typeof row.error === "string" ? row.error : undefined,
    discoveredBrands: Array.isArray(row.discovered_brands) ? (row.discovered_brands as unknown[]) : undefined,
    e2eProgress:
      row.e2e_progress && typeof row.e2e_progress === "object"
        ? (row.e2e_progress as Record<string, unknown>)
        : undefined,
    discoverModelResponse:
      typeof row.discover_model_response === "string" ? row.discover_model_response : undefined,
    meta: row.meta && typeof row.meta === "object" ? (row.meta as Record<string, unknown>) : {},
    createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at ?? ""),
    updatedAt: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at ?? ""),
  };
}

function rowToRetailerState(row: Record<string, unknown>): RetailerPipelineState {
  return {
    retailer: String(row.retailer),
    baseUrl: typeof row.base_url === "string" ? row.base_url : undefined,
    displayName: typeof row.display_name === "string" ? row.display_name : undefined,
    latestJobId: typeof row.latest_job_id === "string" ? row.latest_job_id : undefined,
    discoveryState:
      row.discovery_state && typeof row.discovery_state === "object"
        ? (row.discovery_state as Record<string, unknown>)
        : undefined,
    exploreState:
      row.explore_state && typeof row.explore_state === "object"
        ? (row.explore_state as Record<string, unknown>)
        : undefined,
    crawlState:
      row.crawl_state && typeof row.crawl_state === "object"
        ? (row.crawl_state as Record<string, unknown>)
        : undefined,
    uploadState:
      row.upload_state && typeof row.upload_state === "object"
        ? (row.upload_state as Record<string, unknown>)
        : undefined,
    updatedAt: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at ?? ""),
  };
}

function mergeJson(
  current: Record<string, unknown> | undefined,
  patch: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!patch) return current;
  return {
    ...(current ?? {}),
    ...patch,
  };
}

export async function ensurePipelinePersistenceSchema(): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  if (schemaReady) return schemaReady;
  schemaReady = (async () => {
    await pg.query(`
      CREATE TABLE IF NOT EXISTS pipeline_jobs (
        id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        retailer TEXT,
        status TEXT NOT NULL,
        urls JSONB NOT NULL DEFAULT '[]'::jsonb,
        total_items INTEGER NOT NULL DEFAULT 0,
        current_index INTEGER NOT NULL DEFAULT 0,
        current_url TEXT,
        logs JSONB NOT NULL DEFAULT '[]'::jsonb,
        results JSONB NOT NULL DEFAULT '[]'::jsonb,
        error TEXT,
        discovered_brands JSONB,
        e2e_progress JSONB,
        discover_model_response TEXT,
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS pipeline_job_events (
        id BIGSERIAL PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES pipeline_jobs(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS pipeline_job_events_job_id_id_idx
      ON pipeline_job_events (job_id, id);
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS retailer_pipeline_state (
        retailer TEXT PRIMARY KEY,
        base_url TEXT,
        display_name TEXT,
        latest_job_id TEXT,
        discovery_state JSONB NOT NULL DEFAULT '{}'::jsonb,
        explore_state JSONB NOT NULL DEFAULT '{}'::jsonb,
        crawl_state JSONB NOT NULL DEFAULT '{}'::jsonb,
        upload_state JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS upload_url_results (
        retailer TEXT NOT NULL,
        crawl_source_crawled_at TEXT NOT NULL,
        url TEXT NOT NULL,
        job_id TEXT NOT NULL,
        status TEXT NOT NULL,
        external_id TEXT,
        item_name TEXT,
        image_count INTEGER,
        uploaded_to_r2 BOOLEAN NOT NULL DEFAULT FALSE,
        upserted_to_db BOOLEAN NOT NULL DEFAULT FALSE,
        error TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (retailer, crawl_source_crawled_at, url)
      );
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS upload_url_results_lookup_idx
      ON upload_url_results (retailer, crawl_source_crawled_at, status);
    `);
    // Unified error feed across all pipeline stages.
    await pg.query(`
      CREATE TABLE IF NOT EXISTS scrape_errors (
        id BIGSERIAL PRIMARY KEY,
        code TEXT NOT NULL,
        detail TEXT NOT NULL DEFAULT '',
        retailer TEXT,
        stage TEXT NOT NULL,
        url TEXT,
        attempt INTEGER NOT NULL DEFAULT 1,
        job_id TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS scrape_errors_occurred_idx
      ON scrape_errors (occurred_at DESC);
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS scrape_errors_retailer_idx
      ON scrape_errors (retailer, occurred_at DESC);
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS scrape_errors_code_idx
      ON scrape_errors (code, occurred_at DESC);
    `);
    // Best-effort retention: keep ~30 days of errors so the table doesn't grow
    // unbounded. Runs at most every 5 minutes via the partial index trick on
    // schema-init; the real prune happens lazily in the API handler.

    // Worker liveness table — each worker process upserts its row every ~15s.
    // Dashboard reads this to show "who's online and draining the queue."
    await pg.query(`
      CREATE TABLE IF NOT EXISTS worker_heartbeats (
        worker_id TEXT PRIMARY KEY,
        hostname TEXT,
        pid INTEGER,
        concurrency INTEGER NOT NULL DEFAULT 1,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS worker_heartbeats_last_seen_idx
      ON worker_heartbeats (last_seen_at DESC);
    `);
  })();
  return schemaReady;
}

export async function markRunningJobsInterrupted(): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  await pg.query(
    `
      UPDATE pipeline_jobs
      SET status = 'interrupted',
          error = COALESCE(error, 'Job interrupted by process restart or redeploy.'),
          updated_at = NOW()
      WHERE status = 'running'
    `,
  );
}

export async function createPipelineJob(job: PipelineJobRecord): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  await pg.query(
    `
      INSERT INTO pipeline_jobs (
        id, kind, retailer, status, urls, total_items, current_index, current_url,
        logs, results, error, discovered_brands, e2e_progress, discover_model_response, meta
      ) VALUES (
        $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
        $9::jsonb, $10::jsonb, $11, $12::jsonb, $13::jsonb, $14, $15::jsonb
      )
      ON CONFLICT (id) DO UPDATE SET
        kind = EXCLUDED.kind,
        retailer = EXCLUDED.retailer,
        status = EXCLUDED.status,
        urls = EXCLUDED.urls,
        total_items = EXCLUDED.total_items,
        current_index = EXCLUDED.current_index,
        current_url = EXCLUDED.current_url,
        logs = EXCLUDED.logs,
        results = EXCLUDED.results,
        error = EXCLUDED.error,
        discovered_brands = EXCLUDED.discovered_brands,
        e2e_progress = EXCLUDED.e2e_progress,
        discover_model_response = EXCLUDED.discover_model_response,
        meta = EXCLUDED.meta,
        updated_at = NOW()
    `,
    [
      job.id,
      job.kind,
      job.retailer,
      job.status,
      JSON.stringify(job.urls ?? []),
      job.urls.length,
      job.current ?? 0,
      job.currentUrl ?? null,
      JSON.stringify(job.logs ?? []),
      JSON.stringify(job.results ?? []),
      job.error ?? null,
      JSON.stringify(job.discoveredBrands ?? null),
      JSON.stringify(job.e2eProgress ?? null),
      job.discoverModelResponse ?? null,
      JSON.stringify(job.meta ?? {}),
    ],
  );
}

export async function savePipelineJob(job: PipelineJobRecord): Promise<void> {
  await createPipelineJob(job);
}

export async function getPipelineJob(jobId: string): Promise<PipelineJobRecord | null> {
  const pg = getPool();
  if (!pg) return null;
  await ensurePipelinePersistenceSchema();
  const { rows } = await pg.query(`SELECT * FROM pipeline_jobs WHERE id = $1`, [jobId]);
  if (rows.length === 0) return null;
  return rowToJob(rows[0] as Record<string, unknown>);
}

export async function appendPipelineEvent(jobId: string, event: Record<string, unknown>): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  await pg.query(
    `INSERT INTO pipeline_job_events (job_id, event_type, payload) VALUES ($1, $2, $3::jsonb)`,
    [jobId, String(event.type ?? "event"), JSON.stringify(event)],
  );
}

export async function listPipelineEvents(jobId: string): Promise<Record<string, unknown>[]> {
  const pg = getPool();
  if (!pg) return [];
  await ensurePipelinePersistenceSchema();
  const { rows } = await pg.query(
    `SELECT payload FROM pipeline_job_events WHERE job_id = $1 ORDER BY id ASC`,
    [jobId],
  );
  return rows
    .map((row) => row.payload)
    .filter((payload): payload is Record<string, unknown> => !!payload && typeof payload === "object");
}

export async function getRetailerPipelineState(retailer: string): Promise<RetailerPipelineState | null> {
  const pg = getPool();
  if (!pg) return null;
  await ensurePipelinePersistenceSchema();
  const { rows } = await pg.query(`SELECT * FROM retailer_pipeline_state WHERE retailer = $1`, [retailer]);
  if (rows.length === 0) return null;
  return rowToRetailerState(rows[0] as Record<string, unknown>);
}

export async function listRetailerPipelineStates(): Promise<RetailerPipelineState[]> {
  const pg = getPool();
  if (!pg) return [];
  await ensurePipelinePersistenceSchema();
  const { rows } = await pg.query(`SELECT * FROM retailer_pipeline_state ORDER BY retailer ASC`);
  return rows.map((row) => rowToRetailerState(row as Record<string, unknown>));
}

export async function upsertRetailerPipelineState(
  patch: Omit<RetailerPipelineState, "updatedAt">,
): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  const current = await getRetailerPipelineState(patch.retailer);
  const next: RetailerPipelineState = {
    retailer: patch.retailer,
    baseUrl: patch.baseUrl ?? current?.baseUrl,
    displayName: patch.displayName ?? current?.displayName,
    latestJobId: patch.latestJobId ?? current?.latestJobId,
    discoveryState: mergeJson(current?.discoveryState, patch.discoveryState),
    exploreState: mergeJson(current?.exploreState, patch.exploreState),
    crawlState: mergeJson(current?.crawlState, patch.crawlState),
    uploadState: mergeJson(current?.uploadState, patch.uploadState),
  };
  await pg.query(
    `
      INSERT INTO retailer_pipeline_state (
        retailer, base_url, display_name, latest_job_id,
        discovery_state, explore_state, crawl_state, upload_state, updated_at
      ) VALUES (
        $1, $2, $3, $4,
        $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, NOW()
      )
      ON CONFLICT (retailer) DO UPDATE SET
        base_url = EXCLUDED.base_url,
        display_name = EXCLUDED.display_name,
        latest_job_id = EXCLUDED.latest_job_id,
        discovery_state = EXCLUDED.discovery_state,
        explore_state = EXCLUDED.explore_state,
        crawl_state = EXCLUDED.crawl_state,
        upload_state = EXCLUDED.upload_state,
        updated_at = NOW()
    `,
    [
      next.retailer,
      next.baseUrl ?? null,
      next.displayName ?? null,
      next.latestJobId ?? null,
      JSON.stringify(next.discoveryState ?? {}),
      JSON.stringify(next.exploreState ?? {}),
      JSON.stringify(next.crawlState ?? {}),
      JSON.stringify(next.uploadState ?? {}),
    ],
  );
}

export async function recordUploadUrlResult(record: UploadUrlResultRecord): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  await pg.query(
    `
      INSERT INTO upload_url_results (
        retailer, crawl_source_crawled_at, url, job_id, status,
        external_id, item_name, image_count, uploaded_to_r2, upserted_to_db, error, metadata, processed_at
      ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10, $11, $12::jsonb, NOW()
      )
      ON CONFLICT (retailer, crawl_source_crawled_at, url) DO UPDATE SET
        job_id = EXCLUDED.job_id,
        status = EXCLUDED.status,
        external_id = EXCLUDED.external_id,
        item_name = EXCLUDED.item_name,
        image_count = EXCLUDED.image_count,
        uploaded_to_r2 = EXCLUDED.uploaded_to_r2,
        upserted_to_db = EXCLUDED.upserted_to_db,
        error = EXCLUDED.error,
        metadata = EXCLUDED.metadata,
        processed_at = NOW()
    `,
    [
      record.retailer,
      record.crawlSourceCrawledAt,
      record.url,
      record.jobId,
      record.status,
      record.externalId ?? null,
      record.itemName ?? null,
      record.imageCount ?? 0,
      !!record.uploadedToR2,
      !!record.upsertedToDb,
      record.error ?? null,
      JSON.stringify(record.metadata ?? {}),
    ],
  );
}

// ---------------------------------------------------------------------------
// Unified error feed (surfaces failures from all stages in one place so the UI
// doesn't need to crawl multiple tables / JSON files to find what broke).
// ---------------------------------------------------------------------------

export interface ScrapeErrorRecord {
  /** Stable category of failure — used for grouping/filtering in the UI. */
  code: string;
  /** Free-text detail (usually the message from the underlying Error). */
  detail: string;
  retailer: string | null;
  /** Stage that produced the failure: explore | crawl | upload | discover. */
  stage: string;
  /** URL the failure was tied to, if any. */
  url: string | null;
  /** Caller-supplied attempt counter (defaults to 1). */
  attempt: number;
  /** Optional FK to a pipeline_jobs row. */
  jobId: string | null;
  /** Optional extra context. */
  metadata: Record<string, unknown>;
  occurredAt: string;
}

export async function recordScrapeError(input: {
  code: string;
  detail: string;
  retailer?: string | null;
  stage: string;
  url?: string | null;
  attempt?: number;
  jobId?: string | null;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  try {
    await pg.query(
      `
        INSERT INTO scrape_errors (
          code, detail, retailer, stage, url, attempt, job_id, metadata
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8::jsonb
        )
      `,
      [
        input.code.slice(0, 80),
        (input.detail ?? "").slice(0, 4000),
        input.retailer ?? null,
        input.stage,
        input.url ?? null,
        input.attempt ?? 1,
        input.jobId ?? null,
        JSON.stringify(input.metadata ?? {}),
      ],
    );
  } catch (err) {
    // Never throw from a logger — degrade silently to stderr so the caller's
    // own error path is unaffected.
    console.error("[pipelineStore] Failed to record scrape_errors row:", err);
  }
}

export async function listRecentScrapeErrors(opts?: {
  limit?: number;
  sinceMinutes?: number;
  retailer?: string;
  stage?: string;
  code?: string;
}): Promise<ScrapeErrorRecord[]> {
  const pg = getPool();
  if (!pg) return [];
  await ensurePipelinePersistenceSchema();
  const limit = Math.max(1, Math.min(opts?.limit ?? 200, 1000));
  const where: string[] = [];
  const params: unknown[] = [];
  if (opts?.sinceMinutes && Number.isFinite(opts.sinceMinutes)) {
    params.push(opts.sinceMinutes);
    where.push(`occurred_at >= NOW() - ($${params.length}::text || ' minutes')::interval`);
  }
  if (opts?.retailer) {
    params.push(opts.retailer);
    where.push(`retailer = $${params.length}`);
  }
  if (opts?.stage) {
    params.push(opts.stage);
    where.push(`stage = $${params.length}`);
  }
  if (opts?.code) {
    params.push(opts.code);
    where.push(`code = $${params.length}`);
  }
  params.push(limit);
  const sql = `
    SELECT code, detail, retailer, stage, url, attempt, job_id, metadata, occurred_at
    FROM scrape_errors
    ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
    ORDER BY occurred_at DESC, id DESC
    LIMIT $${params.length}
  `;
  const { rows } = await pg.query(sql, params);
  return rows.map((row) => ({
    code: String(row.code ?? "UNKNOWN"),
    detail: String(row.detail ?? ""),
    retailer: typeof row.retailer === "string" ? row.retailer : null,
    stage: String(row.stage ?? ""),
    url: typeof row.url === "string" ? row.url : null,
    attempt: typeof row.attempt === "number" ? row.attempt : 1,
    jobId: typeof row.job_id === "string" ? row.job_id : null,
    metadata:
      row.metadata && typeof row.metadata === "object"
        ? (row.metadata as Record<string, unknown>)
        : {},
    occurredAt:
      row.occurred_at instanceof Date
        ? row.occurred_at.toISOString()
        : String(row.occurred_at ?? ""),
  }));
}

export async function getScrapeErrorCounts(opts?: {
  sinceMinutes?: number;
}): Promise<{ byCode: Record<string, number>; byStage: Record<string, number>; total: number }> {
  const pg = getPool();
  if (!pg) return { byCode: {}, byStage: {}, total: 0 };
  await ensurePipelinePersistenceSchema();
  const params: unknown[] = [];
  let whereClause = "";
  if (opts?.sinceMinutes && Number.isFinite(opts.sinceMinutes)) {
    params.push(opts.sinceMinutes);
    whereClause = `WHERE occurred_at >= NOW() - ($${params.length}::text || ' minutes')::interval`;
  }
  const [codeRows, stageRows, totalRows] = await Promise.all([
    pg.query(`SELECT code, COUNT(*)::int AS n FROM scrape_errors ${whereClause} GROUP BY code`, params),
    pg.query(`SELECT stage, COUNT(*)::int AS n FROM scrape_errors ${whereClause} GROUP BY stage`, params),
    pg.query(`SELECT COUNT(*)::int AS n FROM scrape_errors ${whereClause}`, params),
  ]);
  const byCode: Record<string, number> = {};
  for (const r of codeRows.rows) byCode[String(r.code)] = Number(r.n) || 0;
  const byStage: Record<string, number> = {};
  for (const r of stageRows.rows) byStage[String(r.stage)] = Number(r.n) || 0;
  return { byCode, byStage, total: Number(totalRows.rows[0]?.n) || 0 };
}

// ---------------------------------------------------------------------------
// Worker heartbeats — drives the "online workers" view on the dashboard.
// ---------------------------------------------------------------------------

export interface WorkerHeartbeatRow {
  workerId: string;
  hostname: string | null;
  pid: number | null;
  concurrency: number;
  metadata: Record<string, unknown>;
  firstSeenAt: string;
  lastSeenAt: string;
  /** Seconds since last_seen_at — convenience for the UI. */
  ageSeconds: number;
}

export async function upsertWorkerHeartbeat(input: {
  workerId: string;
  hostname?: string;
  pid?: number;
  concurrency?: number;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensurePipelinePersistenceSchema();
  await pg.query(
    `
      INSERT INTO worker_heartbeats (worker_id, hostname, pid, concurrency, metadata, first_seen_at, last_seen_at)
      VALUES ($1, $2, $3, $4, $5::jsonb, NOW(), NOW())
      ON CONFLICT (worker_id) DO UPDATE SET
        hostname = EXCLUDED.hostname,
        pid = EXCLUDED.pid,
        concurrency = EXCLUDED.concurrency,
        metadata = EXCLUDED.metadata,
        last_seen_at = NOW()
    `,
    [
      input.workerId,
      input.hostname ?? null,
      input.pid ?? null,
      input.concurrency ?? 1,
      JSON.stringify(input.metadata ?? {}),
    ],
  );
}

export async function listWorkerHeartbeats(opts?: {
  staleAfterSeconds?: number;
}): Promise<WorkerHeartbeatRow[]> {
  const pg = getPool();
  if (!pg) return [];
  await ensurePipelinePersistenceSchema();
  const { rows } = await pg.query(
    `SELECT worker_id, hostname, pid, concurrency, metadata, first_seen_at, last_seen_at,
            EXTRACT(EPOCH FROM (NOW() - last_seen_at))::int AS age_seconds
     FROM worker_heartbeats
     ORDER BY last_seen_at DESC`,
  );
  const staleAfter = opts?.staleAfterSeconds ?? 0;
  return rows
    .filter((r) => !staleAfter || Number(r.age_seconds) <= staleAfter)
    .map((row) => ({
      workerId: String(row.worker_id),
      hostname: typeof row.hostname === "string" ? row.hostname : null,
      pid: typeof row.pid === "number" ? row.pid : null,
      concurrency: typeof row.concurrency === "number" ? row.concurrency : 1,
      metadata:
        row.metadata && typeof row.metadata === "object"
          ? (row.metadata as Record<string, unknown>)
          : {},
      firstSeenAt:
        row.first_seen_at instanceof Date ? row.first_seen_at.toISOString() : String(row.first_seen_at ?? ""),
      lastSeenAt:
        row.last_seen_at instanceof Date ? row.last_seen_at.toISOString() : String(row.last_seen_at ?? ""),
      ageSeconds: Number(row.age_seconds) || 0,
    }));
}

export async function getSuccessfulUploadUrls(
  retailer: string,
  crawlSourceCrawledAt: string,
): Promise<Set<string>> {
  const pg = getPool();
  if (!pg) return new Set();
  await ensurePipelinePersistenceSchema();
  const { rows } = await pg.query(
    `
      SELECT url
      FROM upload_url_results
      WHERE retailer = $1
        AND crawl_source_crawled_at = $2
        AND status = 'uploaded'
        AND upserted_to_db = TRUE
    `,
    [retailer, crawlSourceCrawledAt],
  );
  return new Set(
    rows
      .map((row) => row.url)
      .filter((url): url is string => typeof url === "string" && url.length > 0),
  );
}
