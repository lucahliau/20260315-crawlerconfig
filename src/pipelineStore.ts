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
