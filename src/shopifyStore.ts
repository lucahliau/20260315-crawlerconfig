/**
 * Store-level Shopify helpers shared by the upload pipeline and the
 * price-refresh sweep.
 *
 * - Store currency from the public `/cart.json` (authoritative — fixes the
 *   ".com host defaults to USD" mislabels, e.g. Drake's pricing in GBP on
 *   drakes.com; TLD inference can't see that).
 * - US storefront detection: many non-US brands run a separate `us.<domain>`
 *   Shopify store priced in USD (us.sunspel.com). When it exists, its prices
 *   ARE the brand's real US prices — better than VAT-strip + FX approximation.
 * - `/products.json` listing pagination: price + compare_at_price + gallery
 *   src list for the whole catalog in ~N/250 requests, no product-page or
 *   image fetches.
 */
import type { ShopifyProduct } from "./shopifyIngest.js";

const USER_AGENT =
  "CrawlerConfigBot/1.0 (product-scraper; +https://github.com/example/crawler-config)";
const FETCH_TIMEOUT_MS = 20_000;
const MAX_RETRIES = 5;
const BACKOFF_BASE_MS = 2000;
// Shopify rate-limits /products.json per IP with multi-second windows. The
// sweep is a daily background job, so patience beats speed: wait out a 429
// (Retry-After when present, else escalating 15s/30s/45s/60s) before failing
// the whole retailer.
const RATE_LIMIT_BACKOFF_MS = [15_000, 30_000, 45_000, 60_000];
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

const PAGE_SIZE = 250;
const MAX_PAGES = 60; // 15k products — far beyond any current retailer

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface FetchTextResult {
  ok: boolean;
  status: number;
  text: string | null;
}

/**
 * Minimal polite fetch with retry/backoff (429/5xx). Returns the BODY TEXT,
 * read while the abort timeout is still armed — `fetch()` resolves when the
 * headers arrive, so an unguarded `res.json()` afterwards can hang forever on
 * a stalled body stream (this wedged the first production sweep for 2h+).
 */
async function fetchTextWithRetry(
  url: string,
  timeoutMs = FETCH_TIMEOUT_MS,
): Promise<FetchTextResult | null> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": USER_AGENT },
        signal: controller.signal,
      });
      if (res.ok) {
        // Still under the abort window — a stalled body aborts, not hangs.
        const text = await res.text();
        return { ok: true, status: res.status, text };
      }
      // Free the socket; we never read non-ok bodies.
      try {
        await res.body?.cancel();
      } catch {
        /* already closed */
      }
      if ((res.status === 429 || res.status >= 500) && attempt < MAX_RETRIES) {
        const retryAfter = Number(res.headers.get("retry-after"));
        let backoff: number;
        if (Number.isFinite(retryAfter) && retryAfter > 0) {
          backoff = Math.min(retryAfter * 1000, 90_000);
        } else if (res.status === 429) {
          backoff = RATE_LIMIT_BACKOFF_MS[Math.min(attempt - 1, RATE_LIMIT_BACKOFF_MS.length - 1)];
        } else {
          backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1);
        }
        await delay(backoff);
        continue;
      }
      return { ok: false, status: res.status, text: null };
    } catch {
      if (attempt === MAX_RETRIES) return null;
      await delay(BACKOFF_BASE_MS * 2 ** (attempt - 1));
    } finally {
      clearTimeout(timeout);
    }
  }
  return null;
}

function parseJson<T>(text: string | null): T | null {
  if (text == null) return null;
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

interface CachedValue<T> {
  value: T;
  at: number;
}

const currencyCache = new Map<string, CachedValue<string | null>>();
const usStorefrontCache = new Map<string, CachedValue<string | null>>();

function normalizeOrigin(originOrUrl: string): string | null {
  try {
    return new URL(originOrUrl).origin;
  } catch {
    return null;
  }
}

/**
 * The store's checkout currency from `/cart.json` (e.g. drakes.com → GBP).
 * Cached in-process for 24h; null when the endpoint is unreachable/not Shopify.
 */
export async function getShopifyStoreCurrency(originOrUrl: string): Promise<string | null> {
  const origin = normalizeOrigin(originOrUrl);
  if (!origin) return null;
  const cached = currencyCache.get(origin);
  if (cached && Date.now() - cached.at < CACHE_TTL_MS) return cached.value;

  let value: string | null = null;
  try {
    const res = await fetchTextWithRetry(`${origin}/cart.json`, 10_000);
    const data = res?.ok ? parseJson<{ currency?: unknown }>(res.text) : null;
    if (data && typeof data.currency === "string" && /^[A-Za-z]{3}$/.test(data.currency.trim())) {
      value = data.currency.trim().toUpperCase();
    }
  } catch {
    value = null;
  }
  currencyCache.set(origin, { value, at: Date.now() });
  return value;
}

/**
 * Probe for a dedicated USD storefront at `us.<apex-domain>`. Returns its
 * origin only when it answers `/cart.json` with currency USD; cached 24h.
 * Only meaningful for stores whose home currency isn't USD.
 */
export async function resolveUsStorefront(originOrUrl: string): Promise<string | null> {
  const origin = normalizeOrigin(originOrUrl);
  if (!origin) return null;
  const cached = usStorefrontCache.get(origin);
  if (cached && Date.now() - cached.at < CACHE_TTL_MS) return cached.value;

  let value: string | null = null;
  try {
    const host = new URL(origin).hostname.replace(/^www\./, "");
    if (!host.startsWith("us.")) {
      const usOrigin = `https://us.${host}`;
      const currency = await getShopifyStoreCurrency(usOrigin);
      if (currency === "USD") value = usOrigin;
    }
  } catch {
    value = null;
  }
  usStorefrontCache.set(origin, { value, at: Date.now() });
  return value;
}

export interface ShopifyListing {
  products: ShopifyProduct[];
  pagesFetched: number;
  /** True when pagination ended cleanly (empty page), false on an error/cap stop. */
  complete: boolean;
}

/**
 * Paginate the public `/products.json` listing (250/page, ~1s politeness gap).
 * This is the whole point of the cheap price sweep: the entire catalog's
 * variant prices + compare_at_price + image src lists with zero product-page
 * or image traffic.
 */
export async function listShopifyProducts(
  originOrUrl: string,
  onLog?: (msg: string) => void,
  interPageDelayMs = 1000,
): Promise<ShopifyListing> {
  const origin = normalizeOrigin(originOrUrl);
  const out: ShopifyListing = { products: [], pagesFetched: 0, complete: false };
  if (!origin) return out;

  for (let page = 1; page <= MAX_PAGES; page++) {
    if (page > 1) await delay(interPageDelayMs);
    const res = await fetchTextWithRetry(`${origin}/products.json?limit=${PAGE_SIZE}&page=${page}`);
    if (!res?.ok) {
      onLog?.(`    [listing] ${origin} page ${page}: ${res ? `HTTP ${res.status}` : "fetch failed"} — stopping`);
      return out;
    }
    const data = parseJson<{ products?: ShopifyProduct[] }>(res.text);
    if (!data) {
      onLog?.(`    [listing] ${origin} page ${page}: invalid JSON — stopping`);
      return out;
    }
    const products: ShopifyProduct[] = Array.isArray(data.products) ? data.products : [];
    out.pagesFetched = page;
    if (products.length === 0) {
      out.complete = true;
      return out;
    }
    out.products.push(...products);
    if (products.length < PAGE_SIZE) {
      out.complete = true;
      return out;
    }
  }
  onLog?.(`    [listing] ${origin}: hit ${MAX_PAGES}-page cap — treating as complete`);
  out.complete = true;
  return out;
}
