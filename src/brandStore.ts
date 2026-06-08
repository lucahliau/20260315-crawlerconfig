import { getPool } from "./pipelineStore.js";
import type {
  DiscoveredBrand,
  BrandStatus,
  BrandPriceSample,
  DiscoveryCountSnapshot,
} from "./discoverBrands.js";
import type { VendorLead, StockistMineResult, BrandLeadsFile } from "./brandSources.js";

/**
 * Durable Postgres-backed store for brand-curation state. The crawler is
 * Railway-hosted on an ephemeral filesystem, so the legacy JSON files
 * (discovered-brands.json / brand-leads.json) were wiped on every redeploy —
 * losing curation, mined leads, and search history. This module persists all of
 * that to the shared Supabase Postgres (same DB the backend reads), so state
 * survives refreshes, restarts, and redeploys.
 *
 * Three tables:
 *  - `discovered_brands`  — one row per brand (url PK) with curation + price.
 *  - `brand_leads`        — latest mined stockist leads (brand_key PK).
 *  - `discovery_runs`     — one row per discovery search (category + counts),
 *                           so the UI can show "you already searched this" and
 *                           avoid wasting AI/search credits re-running it.
 *
 * When DATABASE_URL is unset, `brandStoreEnabled()` is false and callers fall
 * back to the original local-JSON behavior (useful for offline dev).
 */

export function brandStoreEnabled(): boolean {
  return getPool() !== null;
}

let schemaReady: Promise<void> | null = null;

export function ensureBrandSchema(): Promise<void> {
  const pg = getPool();
  if (!pg) return Promise.resolve();
  if (schemaReady) return schemaReady;
  schemaReady = (async () => {
    await pg.query(`
      CREATE TABLE IF NOT EXISTS discovered_brands (
        url TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT,
        source TEXT,
        region TEXT,
        fit_score DOUBLE PRECISION,
        price_sample JSONB,
        discovered_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS discovered_brands_status_idx
      ON discovered_brands (status);
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS brand_leads (
        brand_key TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        usd DOUBLE PRECISION NOT NULL DEFAULT 0,
        tier TEXT NOT NULL DEFAULT 'unknown',
        product_count INTEGER NOT NULL DEFAULT 0,
        stockists JSONB NOT NULL DEFAULT '[]'::jsonb,
        generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    // The mine summary (which stockists were reachable) is small; store the last
    // run's metadata in a single-row table keyed by a constant.
    await pg.query(`
      CREATE TABLE IF NOT EXISTS brand_lead_runs (
        id INTEGER PRIMARY KEY DEFAULT 1,
        generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        stockists JSONB NOT NULL DEFAULT '[]'::jsonb,
        CONSTRAINT brand_lead_runs_singleton CHECK (id = 1)
      );
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS discovery_runs (
        id BIGSERIAL PRIMARY KEY,
        category TEXT,
        category_key TEXT,
        new_count INTEGER NOT NULL DEFAULT 0,
        total_count INTEGER NOT NULL DEFAULT 0,
        ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pg.query(`
      CREATE INDEX IF NOT EXISTS discovery_runs_ran_at_idx
      ON discovery_runs (ran_at DESC);
    `);
  })();
  return schemaReady;
}

// ---------------------------------------------------------------------------
// Brands
// ---------------------------------------------------------------------------

export interface MasterListShape {
  brands: DiscoveredBrand[];
  urls: string[];
  history: DiscoveryCountSnapshot[];
}

function rowToBrand(row: Record<string, unknown>): DiscoveredBrand {
  const priceSample =
    row.price_sample && typeof row.price_sample === "object"
      ? (row.price_sample as BrandPriceSample)
      : undefined;
  return {
    url: String(row.url),
    name: String(row.name),
    status: typeof row.status === "string" ? (row.status as BrandStatus) : undefined,
    source: typeof row.source === "string" ? row.source : undefined,
    region: typeof row.region === "string" ? row.region : undefined,
    fitScore: typeof row.fit_score === "number" ? row.fit_score : undefined,
    priceSample,
    discoveredAt:
      row.discovered_at instanceof Date
        ? row.discovered_at.toISOString()
        : typeof row.discovered_at === "string"
          ? row.discovered_at
          : undefined,
  };
}

/** Load the full brand master list from Postgres. Returns null when DB is off. */
export async function dbLoadMasterList(): Promise<MasterListShape | null> {
  const pg = getPool();
  if (!pg) return null;
  await ensureBrandSchema();
  const [brandsRes, runsRes] = await Promise.all([
    pg.query(`SELECT * FROM discovered_brands ORDER BY created_at ASC, url ASC`),
    pg.query(
      `SELECT total_count, ran_at FROM discovery_runs ORDER BY ran_at ASC`,
    ),
  ]);
  const brands = brandsRes.rows.map((r) => rowToBrand(r as Record<string, unknown>));
  const history: DiscoveryCountSnapshot[] = runsRes.rows.map((r) => ({
    at: r.ran_at instanceof Date ? r.ran_at.toISOString() : String(r.ran_at ?? ""),
    count: Number(r.total_count) || 0,
  }));
  return { brands, urls: brands.map((b) => b.url), history };
}

/**
 * Insert brands that don't already exist (ON CONFLICT DO NOTHING) so we never
 * clobber existing curation status / price samples. Used by both discovery
 * (status='candidate') and lead promotion (status=null → eligible).
 */
export async function dbInsertBrandsIfMissing(brands: DiscoveredBrand[]): Promise<void> {
  const pg = getPool();
  if (!pg || brands.length === 0) return;
  await ensureBrandSchema();
  for (const b of brands) {
    await pg.query(
      `
        INSERT INTO discovered_brands (url, name, status, source, region, fit_score, price_sample, discovered_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
        ON CONFLICT (url) DO NOTHING
      `,
      [
        b.url,
        b.name,
        b.status ?? null,
        b.source ?? null,
        b.region ?? null,
        b.fitScore ?? null,
        b.priceSample ? JSON.stringify(b.priceSample) : null,
        b.discoveredAt ?? new Date().toISOString(),
      ],
    );
  }
}

export async function dbSetBrandStatus(url: string, status: BrandStatus): Promise<boolean> {
  const pg = getPool();
  if (!pg) return false;
  await ensureBrandSchema();
  const res = await pg.query(
    `UPDATE discovered_brands SET status = $2, updated_at = NOW() WHERE url = $1`,
    [url, status],
  );
  return (res.rowCount ?? 0) > 0;
}

export async function dbSetBrandPriceSample(
  url: string,
  sample: BrandPriceSample,
): Promise<boolean> {
  const pg = getPool();
  if (!pg) return false;
  await ensureBrandSchema();
  const res = await pg.query(
    `UPDATE discovered_brands SET price_sample = $2::jsonb, updated_at = NOW() WHERE url = $1`,
    [url, JSON.stringify(sample)],
  );
  return (res.rowCount ?? 0) > 0;
}

// ---------------------------------------------------------------------------
// Discovery run history (search log)
// ---------------------------------------------------------------------------

function categoryKey(category: string | null | undefined): string | null {
  if (!category) return null;
  return category.toLowerCase().replace(/\s+/g, " ").trim() || null;
}

export async function dbRecordDiscoveryRun(args: {
  category: string | null;
  newCount: number;
  totalCount: number;
}): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensureBrandSchema();
  await pg.query(
    `INSERT INTO discovery_runs (category, category_key, new_count, total_count) VALUES ($1, $2, $3, $4)`,
    [args.category, categoryKey(args.category), args.newCount, args.totalCount],
  );
}

export interface DiscoveryRunRow {
  category: string | null;
  newCount: number;
  totalCount: number;
  ranAt: string;
}

export async function dbListDiscoveryRuns(limit = 50): Promise<DiscoveryRunRow[]> {
  const pg = getPool();
  if (!pg) return [];
  await ensureBrandSchema();
  const { rows } = await pg.query(
    `SELECT category, new_count, total_count, ran_at FROM discovery_runs ORDER BY ran_at DESC LIMIT $1`,
    [Math.max(1, Math.min(limit, 500))],
  );
  return rows.map((r) => ({
    category: typeof r.category === "string" ? r.category : null,
    newCount: Number(r.new_count) || 0,
    totalCount: Number(r.total_count) || 0,
    ranAt: r.ran_at instanceof Date ? r.ran_at.toISOString() : String(r.ran_at ?? ""),
  }));
}

// ---------------------------------------------------------------------------
// Brand leads (stockist mining output)
// ---------------------------------------------------------------------------

/** Replace the stored leads with a fresh mining run's output. */
export async function dbReplaceBrandLeads(file: BrandLeadsFile): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensureBrandSchema();
  const client = await pg.connect();
  try {
    await client.query("BEGIN");
    await client.query(`DELETE FROM brand_leads`);
    for (const l of file.leads) {
      const key = l.name.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim() || l.name;
      await client.query(
        `
          INSERT INTO brand_leads (brand_key, name, usd, tier, product_count, stockists, generated_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, NOW())
          ON CONFLICT (brand_key) DO UPDATE SET
            name = EXCLUDED.name, usd = EXCLUDED.usd, tier = EXCLUDED.tier,
            product_count = EXCLUDED.product_count, stockists = EXCLUDED.stockists,
            generated_at = EXCLUDED.generated_at, updated_at = NOW()
        `,
        [
          key,
          l.name,
          l.usd,
          l.tier,
          l.productCount,
          JSON.stringify(l.stockists),
          file.generatedAt,
        ],
      );
    }
    await client.query(
      `
        INSERT INTO brand_lead_runs (id, generated_at, stockists)
        VALUES (1, $1, $2::jsonb)
        ON CONFLICT (id) DO UPDATE SET generated_at = EXCLUDED.generated_at, stockists = EXCLUDED.stockists
      `,
      [file.generatedAt, JSON.stringify(file.stockists)],
    );
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function dbLoadBrandLeads(): Promise<BrandLeadsFile | null> {
  const pg = getPool();
  if (!pg) return null;
  await ensureBrandSchema();
  const [leadsRes, runRes] = await Promise.all([
    pg.query(
      `SELECT name, usd, tier, product_count, stockists FROM brand_leads
       ORDER BY jsonb_array_length(stockists) DESC, product_count DESC`,
    ),
    pg.query(`SELECT generated_at, stockists FROM brand_lead_runs WHERE id = 1`),
  ]);
  const leads: VendorLead[] = leadsRes.rows.map((r) => ({
    name: String(r.name),
    usd: Number(r.usd) || 0,
    tier: String(r.tier) as VendorLead["tier"],
    productCount: Number(r.product_count) || 0,
    stockists: Array.isArray(r.stockists) ? (r.stockists as string[]) : [],
  }));
  const run = runRes.rows[0];
  const stockists: StockistMineResult[] =
    run && Array.isArray(run.stockists) ? (run.stockists as StockistMineResult[]) : [];
  const generatedAt =
    run && run.generated_at
      ? run.generated_at instanceof Date
        ? run.generated_at.toISOString()
        : String(run.generated_at)
      : new Date(0).toISOString();
  if (leads.length === 0 && !run) return null;
  return { generatedAt, stockists, leads };
}

// ---------------------------------------------------------------------------
// One-time bootstrap: migrate existing local JSON into Postgres on first run.
// Safe to call every startup — it only seeds tables that are still empty.
// ---------------------------------------------------------------------------

export async function dbBootstrapBrandsIfEmpty(jsonBrands: DiscoveredBrand[]): Promise<number> {
  const pg = getPool();
  if (!pg || jsonBrands.length === 0) return 0;
  await ensureBrandSchema();
  const { rows } = await pg.query(`SELECT COUNT(*)::int AS n FROM discovered_brands`);
  if ((Number(rows[0]?.n) || 0) > 0) return 0;
  for (const b of jsonBrands) {
    await pg.query(
      `
        INSERT INTO discovered_brands (url, name, status, source, region, fit_score, price_sample, discovered_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
        ON CONFLICT (url) DO NOTHING
      `,
      [
        b.url,
        b.name,
        b.status ?? null,
        b.source ?? null,
        b.region ?? null,
        b.fitScore ?? null,
        b.priceSample ? JSON.stringify(b.priceSample) : null,
        b.discoveredAt ?? null,
      ],
    );
  }
  return jsonBrands.length;
}

export async function dbBootstrapLeadsIfEmpty(file: BrandLeadsFile | null): Promise<number> {
  const pg = getPool();
  if (!pg || !file || file.leads.length === 0) return 0;
  await ensureBrandSchema();
  const { rows } = await pg.query(`SELECT COUNT(*)::int AS n FROM brand_leads`);
  if ((Number(rows[0]?.n) || 0) > 0) return 0;
  await dbReplaceBrandLeads(file);
  return file.leads.length;
}
