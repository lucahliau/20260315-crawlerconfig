/**
 * Brand previews — the imagery behind the mobile swipe app's vibe check.
 *
 * For each candidate brand we cache a small visual sample in Postgres:
 *   - Shopify sites: up to 6 product photos + titles from /products.json
 *     (one request, hotlinkable CDN URLs — the richest signal).
 *   - Everything else: the homepage og:image as hero, plus product images
 *     pulled from JSON-LD on up to two sitemap-sampled product pages.
 *
 * Plain HTTP only (no AI). Successes cache forever; failures cache for a day
 * so a flaky site can't be hammered by repeated queue loads.
 */

import { getPool } from "./pipelineStore.js";
import { fetchProductsPage } from "./shopifyExplore.js";
import {
  fetchWithTimeout,
  discoverSitemapCandidates,
  collectSitemapUrls,
  NON_PRODUCT_PATH,
} from "./platformExplore.js";
import {
  extractJsonLdBlocks,
  findProductInJsonLd,
  extractMetaContent,
} from "./htmlExtract.js";

export interface PreviewImage {
  src: string;
  title?: string;
}

export interface BrandPreview {
  url: string;
  ok: boolean;
  hero: string | null;
  images: PreviewImage[];
  source: "shopify" | "sampled" | "og" | "none";
  fetchedAt: string;
}

const FAILURE_RETRY_MS = 24 * 60 * 60 * 1000;

let schemaReady: Promise<void> | null = null;
function ensureSchema(): Promise<void> {
  const pg = getPool();
  if (!pg) return Promise.resolve();
  schemaReady ??= (async () => {
    await pg.query(`
      CREATE TABLE IF NOT EXISTS brand_previews (
        url TEXT PRIMARY KEY,
        ok BOOLEAN NOT NULL DEFAULT FALSE,
        hero TEXT,
        images JSONB NOT NULL DEFAULT '[]'::jsonb,
        source TEXT NOT NULL DEFAULT 'none',
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
  })();
  return schemaReady;
}

function rowToPreview(row: Record<string, unknown>): BrandPreview {
  return {
    url: String(row.url),
    ok: !!row.ok,
    hero: typeof row.hero === "string" ? row.hero : null,
    images: Array.isArray(row.images) ? (row.images as PreviewImage[]) : [],
    source: (row.source as BrandPreview["source"]) ?? "none",
    fetchedAt: new Date(String(row.fetched_at)).toISOString(),
  };
}

async function loadCached(url: string): Promise<BrandPreview | null> {
  const pg = getPool();
  if (!pg) return null;
  await ensureSchema();
  const r = await pg.query(`SELECT * FROM brand_previews WHERE url = $1`, [url]);
  if (r.rows.length === 0) return null;
  return rowToPreview(r.rows[0]);
}

async function persist(preview: BrandPreview): Promise<void> {
  const pg = getPool();
  if (!pg) return;
  await ensureSchema();
  await pg.query(
    `INSERT INTO brand_previews (url, ok, hero, images, source, fetched_at)
     VALUES ($1, $2, $3, $4::jsonb, $5, NOW())
     ON CONFLICT (url) DO UPDATE
       SET ok = EXCLUDED.ok, hero = EXCLUDED.hero, images = EXCLUDED.images,
           source = EXCLUDED.source, fetched_at = NOW()`,
    [preview.url, preview.ok, preview.hero, JSON.stringify(preview.images), preview.source],
  );
}

// ---------------------------------------------------------------------------
// Fetchers
// ---------------------------------------------------------------------------

function firstImageString(image: unknown): string | null {
  if (typeof image === "string") return image;
  if (Array.isArray(image)) {
    for (const i of image) {
      const s = firstImageString(i);
      if (s) return s;
    }
    return null;
  }
  if (image && typeof image === "object") {
    const url = (image as { url?: unknown }).url;
    if (typeof url === "string") return url;
  }
  return null;
}

function absolutize(src: string | null, base: string): string | null {
  if (!src) return null;
  try {
    return new URL(src, base).toString();
  } catch {
    return null;
  }
}

async function fetchShopifyPreview(origin: string): Promise<PreviewImage[] | null> {
  const products = await fetchProductsPage(origin, 12);
  if (!products || products.length === 0) return null;
  const images: PreviewImage[] = [];
  for (const p of products) {
    const src = p.images?.[0]?.src;
    if (typeof src === "string" && src.startsWith("http")) {
      images.push({ src, title: p.title });
    }
    if (images.length >= 6) break;
  }
  return images.length > 0 ? images : null;
}

async function fetchSampledPreview(
  origin: string,
): Promise<{ hero: string | null; images: PreviewImage[] }> {
  let hero: string | null = null;
  const images: PreviewImage[] = [];

  const homeRes = await fetchWithTimeout(origin, "text/html");
  if (homeRes?.ok) {
    const html = await homeRes.text();
    hero = absolutize(extractMetaContent(html, "og:image"), origin);
  }

  // Sample sitemap pages for JSON-LD product images. Prefer conventional
  // product paths; when a site uses unconventional PDP URLs (e.g. asket.com's
  // /en-us/mens-t-shirt-black), fall back to sampling generic deep pages and
  // keep only those that actually carry Product JSON-LD.
  try {
    const candidates = await discoverSitemapCandidates(origin);
    for (const candidate of candidates.slice(0, 2)) {
      const urls = await collectSitemapUrls(candidate);
      const productish = urls.filter(
        (u) => !NON_PRODUCT_PATH.test(u) && /\/(products?|p|item|shop\/p)\//i.test(u),
      );
      const generic = urls.filter((u) => {
        if (NON_PRODUCT_PATH.test(u)) return false;
        try {
          return new URL(u).pathname.split("/").filter(Boolean).length >= 2;
        } catch {
          return false;
        }
      });
      // Spread generic samples across the sitemap so one section can't dominate.
      const spread = generic.filter(
        (_, i) => i % Math.max(1, Math.floor(generic.length / 6)) === 0,
      );
      const toSample = (productish.length > 0 ? productish.slice(0, 2) : spread.slice(0, 5));
      for (const pageUrl of toSample) {
        const res = await fetchWithTimeout(pageUrl, "text/html");
        if (!res?.ok) continue;
        const html = await res.text();
        const product = findProductInJsonLd(extractJsonLdBlocks(html));
        const src = absolutize(firstImageString(product?.image), pageUrl);
        if (src) {
          images.push({
            src,
            title: typeof product?.name === "string" ? product.name : undefined,
          });
        }
        if (images.length >= 4) break;
        await new Promise((r) => setTimeout(r, 300));
      }
      if (images.length > 0) break;
    }
  } catch {
    /* sampling is best-effort */
  }
  return { hero, images };
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

export async function getOrFetchBrandPreview(url: string): Promise<BrandPreview> {
  let origin: string;
  try {
    origin = new URL(url).origin;
  } catch {
    return { url, ok: false, hero: null, images: [], source: "none", fetchedAt: new Date().toISOString() };
  }

  const cached = await loadCached(url).catch(() => null);
  if (cached && (cached.ok || Date.now() - new Date(cached.fetchedAt).getTime() < FAILURE_RETRY_MS)) {
    return cached;
  }

  let preview: BrandPreview;
  const shopifyImages = await fetchShopifyPreview(origin).catch(() => null);
  if (shopifyImages) {
    preview = {
      url,
      ok: true,
      hero: shopifyImages[0]?.src ?? null,
      images: shopifyImages,
      source: "shopify",
      fetchedAt: new Date().toISOString(),
    };
  } else {
    const { hero, images } = await fetchSampledPreview(origin).catch(() => ({
      hero: null,
      images: [] as PreviewImage[],
    }));
    const ok = !!hero || images.length > 0;
    preview = {
      url,
      ok,
      hero: hero ?? images[0]?.src ?? null,
      images,
      source: ok ? (images.length > 0 ? "sampled" : "og") : "none",
      fetchedAt: new Date().toISOString(),
    };
  }
  await persist(preview).catch((err) => {
    console.error(`[brandPreview] persist failed for ${url}:`, err);
  });
  return preview;
}

/** Bounded-concurrency enrichment with a per-brand timeout — used by the swipe queue. */
export async function getPreviewsFor(
  urls: string[],
  opts?: { concurrency?: number; perBrandTimeoutMs?: number },
): Promise<Map<string, BrandPreview>> {
  const concurrency = opts?.concurrency ?? 3;
  const timeoutMs = opts?.perBrandTimeoutMs ?? 10_000;
  const out = new Map<string, BrandPreview>();
  const queue = [...urls];

  const withTimeout = (url: string): Promise<BrandPreview> =>
    Promise.race([
      getOrFetchBrandPreview(url),
      new Promise<BrandPreview>((resolve) =>
        setTimeout(
          () =>
            resolve({
              url,
              ok: false,
              hero: null,
              images: [],
              source: "none",
              fetchedAt: new Date().toISOString(),
            }),
          timeoutMs,
        ),
      ),
    ]);

  await Promise.all(
    Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
      while (queue.length > 0) {
        const url = queue.shift()!;
        try {
          out.set(url, await withTimeout(url));
        } catch {
          /* leave missing — UI degrades gracefully */
        }
      }
    }),
  );
  return out;
}
