/**
 * Lightweight price/sale refresh — the cheap sibling of a full re-crawl.
 *
 * A full re-crawl fetches every product page AND moves every image through
 * R2; that's what makes weekly sweeps slow and bandwidth-bound. But for
 * Shopify stores the entire catalog's CURRENT prices (variant `price` +
 * `compare_at_price` sale data + gallery src lists) are available from the
 * public `/products.json` listing at 250 products/request. So refreshing
 * prices for a 2,000-item brand costs ~8 polite HTTP requests and zero image
 * traffic.
 *
 * What one retailer refresh does:
 *   1. Resolve the store's real currency from `/cart.json` (fixes .com
 *      GBP/EUR stores that TLD inference mislabels as USD).
 *   2. If the store isn't USD, probe for a dedicated `us.<domain>` USD
 *      storefront — when it exists, its prices ARE the brand's real US
 *      prices (no VAT-strip approximation needed).
 *   3. Paginate the listing(s), match products to catalog rows by product
 *      handle, normalize (VAT strip + live FX) and update ONLY the price
 *      fields of rows whose values changed. Images/embeddings/nobg are never
 *      touched.
 *   4. Compare each product's gallery src list against the row's stored
 *      `metadata.sourceImages` fingerprint — when the retailer changed the
 *      gallery, enqueue a full re-upload for just those URLs.
 *
 * Refresh-only by design: rows missing from the listing are counted but NOT
 * deactivated (existing product decision — no auto-deactivation).
 */
import type { Pool } from "pg";
import type { Config } from "./schemas/config.js";
import { safeParseConfig } from "./schemas/config.js";
import { normalizePriceTriple } from "./currencyToUsd.js";
import { ensureFreshFxRates } from "./fxRates.js";
import {
  getShopifyStoreCurrency,
  listShopifyProducts,
  resolveUsStorefront,
} from "./shopifyStore.js";
import { representativeVariantPricing, type ShopifyProduct } from "./shopifyIngest.js";
import { enqueueUploadUrls } from "./queue.js";
import {
  getPool,
  getSetting,
  setSetting,
  listRetailerPipelineStates,
} from "./pipelineStore.js";

/**
 * Dedicated switch for the DAILY price-refresh cron, independent of the
 * conveyor kill switch (`auto_pipeline`) — pausing crawling shouldn't stop
 * cheap price/sale freshness. Default ON; toggle via
 * `POST /api/price-refresh/enabled` or the pipeline_settings KV.
 */
export async function isPriceRefreshEnabled(): Promise<boolean> {
  return getSetting<boolean>("price_refresh_enabled", true);
}

export async function setPriceRefreshEnabled(enabled: boolean): Promise<void> {
  await setSetting("price_refresh_enabled", enabled);
}

export interface PriceRefreshOutcome {
  retailer: string;
  status: "ok" | "skipped" | "error";
  reason?: string;
  storeCurrency?: string | null;
  usStorefront?: string | null;
  listed: number;
  matchedRows: number;
  updated: number;
  onSale: number;
  imagesChanged: number;
  reuploadsEnqueued: number;
  missingFromListing: number;
  /** Rows marked inStock=false because they vanished from a COMPLETE listing. */
  markedSoldOut: number;
  durationMs: number;
}

export interface PriceRefreshOptions {
  dryRun?: boolean;
  /** Enqueue full re-uploads for products whose source gallery changed (default true). */
  enqueueImageChanges?: boolean;
  /** Stop when the price-refresh switch is off — true for CRON sweeps only.
   *  Manual runs (CLI / dashboard API) are explicit user intent and bypass it. */
  honorKillSwitch?: boolean;
  onLog?: (msg: string) => void;
}

/** Hard cap per retailer inside a sweep — see the Promise.race below. */
const RETAILER_DEADLINE_MS = Math.max(
  60_000,
  parseInt(process.env.PRICE_REFRESH_RETAILER_DEADLINE_MS ?? "1200000", 10),
);

interface CatalogRow {
  id: string;
  sourceUrl: string;
  price: number | null;
  salePrice: number | null;
  compareAtPrice: number | null;
  inStock: boolean | null;
  sourceImages: string[] | null;
}

function extractHandle(url: string): string | null {
  const m = url.match(/\/products\/([^/?#]+)/);
  return m ? m[1] : null;
}

function productImageSrcs(product: ShopifyProduct): string[] {
  const srcs = (product.images ?? [])
    .map((i) => (typeof i?.src === "string" ? i.src : ""))
    .filter(Boolean);
  if (srcs.length === 0 && typeof product.image?.src === "string") return [product.image.src];
  return srcs;
}

function numbersEqual(a: number | null, b: number | null): boolean {
  if (a == null || b == null) return a === b;
  return Math.abs(a - b) < 0.005;
}

function toNum(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === "number" ? v : parseFloat(String(v));
  return Number.isFinite(n) ? n : null;
}

/** The origin whose /products.json mirrors this config's catalog. */
function configOrigin(config: Config): string | null {
  try {
    if (config.discovery.method === "api") {
      return new URL(config.discovery.api.endpoint).origin;
    }
    return new URL(config.baseUrl).origin;
  } catch {
    return null;
  }
}

export async function refreshRetailerPrices(
  config: Config,
  pool: Pool,
  opts?: PriceRefreshOptions,
): Promise<PriceRefreshOutcome> {
  const log = opts?.onLog ?? console.log;
  const dryRun = opts?.dryRun === true;
  const started = Date.now();
  const out: PriceRefreshOutcome = {
    retailer: config.retailer,
    status: "error",
    storeCurrency: null,
    usStorefront: null,
    listed: 0,
    matchedRows: 0,
    updated: 0,
    onSale: 0,
    imagesChanged: 0,
    reuploadsEnqueued: 0,
    missingFromListing: 0,
    markedSoldOut: 0,
    durationMs: 0,
  };
  const finish = (status: PriceRefreshOutcome["status"], reason?: string): PriceRefreshOutcome => {
    out.status = status;
    if (reason) out.reason = reason;
    out.durationMs = Date.now() - started;
    return out;
  };

  const origin = configOrigin(config);
  if (!origin) return finish("skipped", "no usable base URL in config");

  // Shopify check: any store answering /cart.json with a currency is Shopify —
  // this covers both native-api configs and Shopify stores crawled via sitemap.
  const storeCurrency = await getShopifyStoreCurrency(origin);
  out.storeCurrency = storeCurrency;
  if (!storeCurrency) return finish("skipped", "not a Shopify store (no /cart.json)");

  await ensureFreshFxRates();

  log(`[price-refresh:${config.retailer}] store ${origin} currency=${storeCurrency}${dryRun ? " (dry-run)" : ""}`);

  // Dedicated US storefront: exact US prices, no VAT/FX approximation.
  let usOrigin: string | null = null;
  let usByHandle: Map<string, ShopifyProduct> | null = null;
  if (storeCurrency !== "USD") {
    usOrigin = await resolveUsStorefront(origin);
    out.usStorefront = usOrigin;
    if (usOrigin) {
      const usListing = await listShopifyProducts(usOrigin, log);
      if (usListing.products.length > 0) {
        usByHandle = new Map();
        for (const p of usListing.products) {
          if (p.handle) usByHandle.set(p.handle, p);
        }
        log(`[price-refresh:${config.retailer}] US storefront ${usOrigin}: ${usByHandle.size} products (exact USD prices)`);
      } else {
        usByHandle = null;
      }
    }
  }

  const listing = await listShopifyProducts(origin, log);
  out.listed = listing.products.length;
  if (listing.products.length === 0) {
    return finish("error", "empty /products.json listing");
  }

  // Catalog rows for this retailer, keyed by product handle from sourceUrl.
  const res = await pool.query<{
    id: string;
    sourceUrl: string;
    price: string | null;
    salePrice: string | null;
    compareAtPrice: string | null;
    inStock: boolean | null;
    sourceImages: unknown;
  }>(
    `SELECT id, "sourceUrl", price::text, "salePrice"::text AS "salePrice",
            "compareAtPrice"::text AS "compareAtPrice", "inStock",
            metadata->'sourceImages' AS "sourceImages"
       FROM "ClothingItem"
      WHERE retailer = $1`,
    [config.retailer],
  );
  const rowsByHandle = new Map<string, CatalogRow[]>();
  for (const r of res.rows) {
    const handle = extractHandle(r.sourceUrl ?? "");
    if (!handle) continue;
    const row: CatalogRow = {
      id: r.id,
      sourceUrl: r.sourceUrl,
      price: toNum(r.price),
      salePrice: toNum(r.salePrice),
      compareAtPrice: toNum(r.compareAtPrice),
      inStock: typeof r.inStock === "boolean" ? r.inStock : null,
      sourceImages: Array.isArray(r.sourceImages)
        ? (r.sourceImages as unknown[]).filter((s): s is string => typeof s === "string")
        : null,
    };
    const list = rowsByHandle.get(handle);
    if (list) list.push(row);
    else rowsByHandle.set(handle, [row]);
  }

  const seenHandles = new Set<string>();
  const changedImageUrls: string[] = [];

  for (const product of listing.products) {
    const handle = (product.handle ?? "").trim();
    if (!handle) continue;
    seenHandles.add(handle);
    const rows = rowsByHandle.get(handle);
    if (!rows || rows.length === 0) continue;
    out.matchedRows += rows.length;

    // Price source: exact US-storefront listing when available, else the home
    // listing (VAT-stripped + FX'd by normalizePriceTriple).
    const usProduct = usByHandle?.get(handle) ?? null;
    const pricingSource = usProduct ?? product;
    const raw = representativeVariantPricing(pricingSource);
    if (raw.price == null) continue;
    // null availability data (rare) → keep the row's existing verdict.
    const newInStock = raw.inStock;
    const rawCurrency = usProduct ? "USD" : storeCurrency;
    const priceUrl = usProduct ? `${usOrigin}/products/${handle}` : rows[0].sourceUrl;

    const n = normalizePriceTriple(priceUrl, rawCurrency, raw.price, raw.salePrice, raw.compareAtPrice);
    if (n.compareAtPrice != null) out.onSale += rows.length;

    const newSrcs = productImageSrcs(product);

    for (const row of rows) {
      // Gallery-change detection (fingerprint recorded by the upload path).
      if (row.sourceImages && row.sourceImages.length > 0 && newSrcs.length > 0) {
        const same =
          row.sourceImages.length === newSrcs.length &&
          row.sourceImages.every((s, i) => s === newSrcs[i]);
        if (!same) {
          out.imagesChanged++;
          changedImageUrls.push(row.sourceUrl);
        }
      }

      const effectiveStock = newInStock ?? row.inStock;
      const changed =
        !numbersEqual(row.price, n.price) ||
        !numbersEqual(row.salePrice, n.salePrice) ||
        !numbersEqual(row.compareAtPrice, n.compareAtPrice) ||
        effectiveStock !== row.inStock;
      if (!changed) continue;

      out.updated++;
      if (dryRun) continue;

      const metaPatch: Record<string, unknown> = {
        priceRefreshedAt: new Date().toISOString(),
      };
      if (n.iso !== "USD" || n.vatStripped > 0) {
        metaPatch.originalPrice = raw.price;
        metaPatch.originalCurrency = n.iso;
      }
      if (n.vatStripped > 0) metaPatch.vatRateStripped = n.vatStripped;
      if (usProduct) {
        metaPatch.usStorefrontPriced = true;
        metaPatch.usProductUrl = priceUrl;
      }

      await pool.query(
        `UPDATE "ClothingItem"
            SET price = $2,
                "salePrice" = $3,
                "compareAtPrice" = $4,
                "inStock" = $6,
                currency = 'USD',
                "lastVerifiedAt" = NOW(),
                "updatedAt" = NOW(),
                metadata = COALESCE(metadata, '{}'::jsonb) || $5::jsonb
          WHERE id = $1`,
        [row.id, n.price, n.salePrice, n.compareAtPrice, JSON.stringify(metaPatch), effectiveStock],
      );
    }
  }

  if (listing.complete) {
    // Rows whose product vanished from the store's COMPLETE listing =
    // delisted. Per product decision (2026-07-09): never hide them — mark
    // inStock=false so the app shows a "SOLD OUT" tag. If the product ever
    // reappears in a listing, the matched path above flips it back.
    const delistedIds: string[] = [];
    for (const [handle, rows] of rowsByHandle) {
      if (seenHandles.has(handle)) continue;
      out.missingFromListing += rows.length;
      for (const row of rows) {
        if (row.inStock !== false) delistedIds.push(row.id);
      }
    }
    if (delistedIds.length > 0 && !dryRun) {
      await pool.query(
        `UPDATE "ClothingItem"
            SET "inStock" = false, "updatedAt" = NOW(),
                metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb
          WHERE id = ANY($1::text[])`,
        [delistedIds, JSON.stringify({ delistedAt: new Date().toISOString() })],
      );
    }
    out.markedSoldOut = delistedIds.length;
  }

  // Changed galleries → full re-upload for just those URLs (images + reprocess
  // reset happen there; the price fields were already refreshed above).
  if (!dryRun && opts?.enqueueImageChanges !== false && changedImageUrls.length > 0) {
    const crawledAt = new Date().toISOString();
    const jobs = changedImageUrls.map((url) => {
      let domain = config.retailer;
      try {
        domain = new URL(url).hostname;
      } catch {
        /* keep retailer fallback */
      }
      return { retailer: config.retailer, url, crawlSourceCrawledAt: crawledAt, domain };
    });
    const { accepted } = await enqueueUploadUrls(jobs);
    out.reuploadsEnqueued = accepted;
  }

  log(
    `[price-refresh:${config.retailer}] listed=${out.listed} matched=${out.matchedRows} updated=${out.updated} onSale=${out.onSale} imagesChanged=${out.imagesChanged} (reuploads=${out.reuploadsEnqueued}) missing=${out.missingFromListing} markedSoldOut=${out.markedSoldOut}${dryRun ? " [dry-run]" : ""}`,
  );
  return finish("ok");
}

/** All retailers with a valid stored config (PG is source of truth in prod). */
export async function loadRefreshableConfigs(): Promise<Config[]> {
  const states = await listRetailerPipelineStates();
  const configs: Config[] = [];
  for (const state of states) {
    const stored = (state.exploreState as { config?: unknown } | undefined)?.config;
    if (!stored || typeof stored !== "object") continue;
    const parsed = safeParseConfig(stored);
    if (parsed.success) configs.push(parsed.data);
  }
  return configs;
}

/**
 * Sweep every configured retailer (Shopify stores refresh; others skip on the
 * cart.json check). Serial with a politeness gap; respects the conveyor kill
 * switch. Persists the last run summary to pipeline_settings for the dashboard.
 */
export async function runPriceRefreshSweep(opts?: PriceRefreshOptions): Promise<PriceRefreshOutcome[]> {
  const log = opts?.onLog ?? console.log;
  const pool = getPool();
  if (!pool) {
    log("[price-refresh] DATABASE_URL not set — sweep skipped.");
    return [];
  }
  const configs = await loadRefreshableConfigs();
  log(`[price-refresh] sweep starting over ${configs.length} configured retailer(s).`);
  const outcomes: PriceRefreshOutcome[] = [];
  for (const config of configs) {
    if (opts?.honorKillSwitch && !(await isPriceRefreshEnabled())) {
      log("[price-refresh] price_refresh_enabled off — stopping sweep.");
      break;
    }
    if (await getSetting<boolean>(`autopilot_optout:${config.retailer}`, false)) continue;
    try {
      // Hard per-retailer deadline: one pathological store (stalled body,
      // hostile pacing) must never wedge the whole sweep — it wedged the first
      // production run for 2h+. The raced-out promise is abandoned; its
      // sockets die with their own fetch timeouts.
      let deadlineTimer: NodeJS.Timeout | undefined;
      const deadline = new Promise<PriceRefreshOutcome>((resolve) => {
        deadlineTimer = setTimeout(() => {
          log(`[price-refresh:${config.retailer}] retailer deadline (${RETAILER_DEADLINE_MS / 60000} min) exceeded — moving on.`);
          resolve({
            retailer: config.retailer,
            status: "error",
            reason: "retailer deadline exceeded",
            storeCurrency: null,
            usStorefront: null,
            listed: 0,
            matchedRows: 0,
            updated: 0,
            onSale: 0,
            imagesChanged: 0,
            reuploadsEnqueued: 0,
            missingFromListing: 0,
            markedSoldOut: 0,
            durationMs: RETAILER_DEADLINE_MS,
          });
        }, RETAILER_DEADLINE_MS);
      });
      const outcome = await Promise.race([refreshRetailerPrices(config, pool, opts), deadline]);
      clearTimeout(deadlineTimer);
      outcomes.push(outcome);
    } catch (err) {
      log(`[price-refresh:${config.retailer}] FAILED: ${(err as Error).message}`);
      outcomes.push({
        retailer: config.retailer,
        status: "error",
        reason: (err as Error).message,
        storeCurrency: null,
        usStorefront: null,
        listed: 0,
        matchedRows: 0,
        updated: 0,
        onSale: 0,
        imagesChanged: 0,
        reuploadsEnqueued: 0,
        missingFromListing: 0,
        markedSoldOut: 0,
        durationMs: 0,
      });
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
  const totals = outcomes.reduce(
    (acc, o) => {
      acc.updated += o.updated;
      acc.onSale += o.onSale;
      acc.ok += o.status === "ok" ? 1 : 0;
      return acc;
    },
    { updated: 0, onSale: 0, ok: 0 },
  );
  log(
    `[price-refresh] sweep done: ${totals.ok}/${outcomes.length} retailers refreshed, ${totals.updated} rows updated, ${totals.onSale} matched rows on sale.`,
  );
  if (!opts?.dryRun) {
    try {
      await setSetting("price_refresh:last", { at: new Date().toISOString(), outcomes });
    } catch {
      /* summary is best-effort */
    }
  }
  return outcomes;
}
