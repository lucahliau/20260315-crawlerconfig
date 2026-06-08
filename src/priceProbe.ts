import "dotenv/config";
import { pathToFileURL } from "node:url";
import {
  convertToUsd,
  resolveCurrencyForUsd,
  roundUsdPrice,
} from "./currencyToUsd.js";
import {
  extractJsonLdBlocks,
  findProductInJsonLd,
  extractMetaContent,
  normalizeOffers,
} from "./htmlExtract.js";

/**
 * Lightweight, zero-AI price probe for a brand's homepage. Used to verify a discovered
 * brand sits in the accessible price tier BEFORE spending crawl/AI budget on it.
 *
 * Strategy (cheapest first):
 *  1. Shopify `/products.json` — most cool indie brands run Shopify; no browser, no AI.
 *  2. Fallback: fetch homepage, find a product link, parse JSON-LD / og:price (reuses upload.ts).
 *
 * All prices are normalized to USD via currencyToUsd.ts.
 */

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";
const DEFAULT_TIMEOUT_MS = 15000;

export type PriceTier = "too_cheap" | "accessible" | "too_expensive" | "unknown";

export interface PriceProbeResult {
  /** Representative price for tier classification, in USD. */
  usd: number;
  /** Tee/T-shirt price in USD if one was identified (preferred classification signal). */
  teeUsd?: number;
  /** Resolved currency the raw price was in before conversion. */
  currency: string;
  /** Page or endpoint the price came from. */
  sourceUrl: string;
  /** How the price was obtained. */
  method: "shopify" | "jsonld" | "og";
  /** Number of distinct product prices observed (confidence signal). */
  sampleCount: number;
}

/** Accessible tee band is ~$20–$90; classify with generous edges to avoid false rejects. */
export function classifyPriceTier(usd: number): PriceTier {
  if (!Number.isFinite(usd) || usd <= 0) return "unknown";
  if (usd < 10) return "too_cheap";
  if (usd > 140) return "too_expensive";
  return "accessible";
}

function originOf(rawUrl: string): string | null {
  try {
    return new URL(rawUrl).origin;
  } catch {
    return null;
  }
}

async function fetchWithTimeout(
  url: string,
  timeoutMs = DEFAULT_TIMEOUT_MS,
): Promise<Response | null> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": UA, Accept: "*/*" },
      redirect: "follow",
      signal: controller.signal,
    });
    return res.ok ? res : null;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

interface ShopifyVariant {
  price?: string | number;
}
interface ShopifyProduct {
  title?: string;
  handle?: string;
  product_type?: string;
  tags?: string[] | string;
  variants?: ShopifyVariant[];
}

function looksLikeTee(p: ShopifyProduct): boolean {
  const hay = `${p.title ?? ""} ${p.product_type ?? ""} ${
    Array.isArray(p.tags) ? p.tags.join(" ") : p.tags ?? ""
  }`.toLowerCase();
  return /\b(t-?shirt|tee|tees)\b/.test(hay);
}

function minVariantPrice(p: ShopifyProduct): number | null {
  let min: number | null = null;
  for (const v of p.variants ?? []) {
    const amt = typeof v.price === "number" ? v.price : parseFloat(String(v.price ?? ""));
    if (Number.isFinite(amt) && amt > 0) {
      min = min === null ? amt : Math.min(min, amt);
    }
  }
  return min;
}

function median(nums: number[]): number {
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/**
 * Shopify's products.json omits currency, and a `.com` host can hide a non-USD store
 * (e.g. a JP brand priced in JPY). The public /cart.json endpoint reports the store's
 * active currency, so we read it to avoid treating ¥17,600 as $17,600.
 */
async function shopifyStoreCurrency(origin: string): Promise<string> {
  const res = await fetchWithTimeout(`${origin}/cart.json`, 8000);
  if (!res) return "";
  try {
    const data = (await res.json()) as { currency?: unknown };
    return typeof data.currency === "string" ? data.currency.trim() : "";
  } catch {
    return "";
  }
}

/** Probe a Shopify storefront via its public products.json endpoint. */
async function probeShopify(origin: string): Promise<PriceProbeResult | null> {
  const endpoint = `${origin}/products.json?limit=100`;
  const res = await fetchWithTimeout(endpoint);
  if (!res) return null;
  const ct = res.headers.get("content-type") ?? "";
  if (!ct.includes("json")) return null;
  let data: { products?: ShopifyProduct[] };
  try {
    data = (await res.json()) as { products?: ShopifyProduct[] };
  } catch {
    return null;
  }
  const products = data.products ?? [];
  if (products.length === 0) return null;

  const perProductMins: number[] = [];
  const teePrices: number[] = [];
  for (const p of products) {
    const min = minVariantPrice(p);
    if (min === null) continue;
    perProductMins.push(min);
    if (looksLikeTee(p)) teePrices.push(min);
  }
  if (perProductMins.length === 0) return null;

  const representativeRaw = teePrices.length > 0 ? median(teePrices) : median(perProductMins);
  const storeCurrency = await shopifyStoreCurrency(origin);
  const { iso } = resolveCurrencyForUsd(origin, representativeRaw, storeCurrency);
  const repUsd = roundUsdPrice(convertToUsd(representativeRaw, iso).usd);
  const teeUsd =
    teePrices.length > 0
      ? roundUsdPrice(convertToUsd(median(teePrices), iso).usd)
      : undefined;

  return {
    usd: repUsd,
    teeUsd,
    currency: iso,
    sourceUrl: endpoint,
    method: "shopify",
    sampleCount: perProductMins.length,
  };
}

const PRODUCT_LINK_RE = /href\s*=\s*["']([^"']*\/(?:products?|shop|p)\/[^"'#?]+)["']/gi;

function firstProductUrl(html: string, origin: string): string | null {
  const seen = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = PRODUCT_LINK_RE.exec(html)) !== null) {
    const href = m[1];
    if (!href || href.includes("/products.json")) continue;
    try {
      const abs = new URL(href, origin);
      if (abs.origin !== origin) continue;
      const s = abs.toString();
      if (seen.has(s)) continue;
      seen.add(s);
      return s;
    } catch {
      /* skip */
    }
  }
  return null;
}

function priceFromJsonLd(product: Record<string, unknown>): number | null {
  const offers = normalizeOffers(product.offers);
  for (const o of offers) {
    const amt = parseFloat(String(o.price ?? ""));
    if (Number.isFinite(amt) && amt > 0) return amt;
  }
  // AggregateOffer lowPrice fallback
  if (product.offers && typeof product.offers === "object" && !Array.isArray(product.offers)) {
    const agg = product.offers as Record<string, unknown>;
    const low = parseFloat(String(agg.lowPrice ?? ""));
    if (Number.isFinite(low) && low > 0) return low;
  }
  return null;
}

function currencyFromJsonLd(product: Record<string, unknown>): string {
  const offers = normalizeOffers(product.offers);
  for (const o of offers) {
    const c = o.priceCurrency;
    if (c != null && String(c).trim() !== "") return String(c).trim();
  }
  if (product.offers && typeof product.offers === "object" && !Array.isArray(product.offers)) {
    const agg = product.offers as Record<string, unknown>;
    if (agg.priceCurrency != null && String(agg.priceCurrency).trim() !== "") {
      return String(agg.priceCurrency).trim();
    }
  }
  return "";
}

/** Fallback: find one product page and read its JSON-LD / og:price. */
async function probeViaProductPage(
  homepageUrl: string,
  origin: string,
): Promise<PriceProbeResult | null> {
  const homeRes = await fetchWithTimeout(homepageUrl);
  if (!homeRes) return null;
  const homeHtml = await homeRes.text();
  const productUrl = firstProductUrl(homeHtml, origin);
  if (!productUrl) return null;

  const prodRes = await fetchWithTimeout(productUrl);
  if (!prodRes) return null;
  const html = await prodRes.text();

  // Layer 1: JSON-LD
  const product = findProductInJsonLd(extractJsonLdBlocks(html));
  if (product) {
    const raw = priceFromJsonLd(product);
    if (raw !== null) {
      const { iso } = resolveCurrencyForUsd(productUrl, raw, currencyFromJsonLd(product));
      const usd = roundUsdPrice(convertToUsd(raw, iso).usd);
      return { usd, currency: iso, sourceUrl: productUrl, method: "jsonld", sampleCount: 1 };
    }
  }

  // Layer 2: Open Graph / product meta
  const ogPrice =
    extractMetaContent(html, "og:price:amount") ?? extractMetaContent(html, "product:price:amount");
  if (ogPrice) {
    const raw = parseFloat(ogPrice);
    if (Number.isFinite(raw) && raw > 0) {
      const ogCur =
        extractMetaContent(html, "og:price:currency") ??
        extractMetaContent(html, "product:price:currency") ??
        "";
      const { iso } = resolveCurrencyForUsd(productUrl, raw, ogCur);
      const usd = roundUsdPrice(convertToUsd(raw, iso).usd);
      return { usd, currency: iso, sourceUrl: productUrl, method: "og", sampleCount: 1 };
    }
  }

  return null;
}

/** Probe a brand homepage for a representative entry price in USD. Returns null if unknown. */
export async function probeEntryPriceUsd(homepageUrl: string): Promise<PriceProbeResult | null> {
  const origin = originOf(homepageUrl);
  if (!origin) return null;
  const shopify = await probeShopify(origin);
  if (shopify) return shopify;
  return probeViaProductPage(homepageUrl, origin);
}

export interface BatchProbeSummary {
  total: number;
  probed: number;
  unknown: number;
  byTier: Record<PriceTier, number>;
  flaggedTooExpensive: { name: string; url: string; usd: number }[];
  flaggedTooCheap: { name: string; url: string; usd: number }[];
}

/**
 * Probe brands from the master list and record a price sample on each.
 * Cheap (no AI). Skips brands that already have a sample unless `force` is set.
 * Used by the dashboard / CLI to verify price tier before crawling.
 */
export async function probeAndRecordBrands(opts?: {
  onlyCandidates?: boolean;
  force?: boolean;
  concurrency?: number;
  onProgress?: (msg: string) => void;
}): Promise<BatchProbeSummary> {
  // Imported lazily so this dependency-light module isn't pulled into discovery's hot path.
  const { listBrands, setBrandPriceSample, isPipelineEligible } = await import(
    "./discoverBrands.js"
  );
  const onProgress = opts?.onProgress ?? (() => {});
  const concurrency = Math.max(1, Math.min(opts?.concurrency ?? 4, 8));

  let brands = listBrands();
  if (opts?.onlyCandidates) brands = brands.filter((b) => !isPipelineEligible(b));
  if (!opts?.force) brands = brands.filter((b) => !b.priceSample);

  const summary: BatchProbeSummary = {
    total: brands.length,
    probed: 0,
    unknown: 0,
    byTier: { too_cheap: 0, accessible: 0, too_expensive: 0, unknown: 0 },
    flaggedTooExpensive: [],
    flaggedTooCheap: [],
  };

  let cursor = 0;
  async function worker(): Promise<void> {
    while (cursor < brands.length) {
      const b = brands[cursor++];
      const r = await probeEntryPriceUsd(b.url);
      if (!r) {
        summary.unknown++;
        summary.byTier.unknown++;
        onProgress(`? ${b.name} — could not probe (unknown tier)`);
        continue;
      }
      const usd = r.teeUsd ?? r.usd;
      const tier = classifyPriceTier(usd);
      setBrandPriceSample(b.url, { usd, sourceUrl: r.sourceUrl, at: new Date().toISOString() });
      summary.probed++;
      summary.byTier[tier]++;
      if (tier === "too_expensive") summary.flaggedTooExpensive.push({ name: b.name, url: b.url, usd });
      if (tier === "too_cheap") summary.flaggedTooCheap.push({ name: b.name, url: b.url, usd });
      onProgress(`${tier === "accessible" ? "✓" : "!"} ${b.name} — $${usd} (${tier})`);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return summary;
}

// --- CLI: `tsx src/priceProbe.ts <homepageUrl>` -----------------------------
const isMain =
  typeof process !== "undefined" &&
  !!process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const arg = process.argv[2];
  if (arg === "--all" || arg === "--all-candidates") {
    probeAndRecordBrands({
      onlyCandidates: arg === "--all-candidates",
      force: process.argv.includes("--force"),
      onProgress: (m) => console.log(m),
    })
      .then((s) => {
        console.log("\nSummary:", JSON.stringify(s, null, 2));
      })
      .catch((e) => {
        console.error("Batch probe failed:", e);
        process.exit(1);
      });
  } else {
  const url = arg;
  if (!url) {
    console.error("Usage: tsx src/priceProbe.ts <homepageUrl> | --all [--force] | --all-candidates [--force]");
    process.exit(1);
  }
  probeEntryPriceUsd(url)
    .then((r) => {
      if (!r) {
        console.log(JSON.stringify({ url, result: null, tier: "unknown" }, null, 2));
        return;
      }
      const tier = classifyPriceTier(r.teeUsd ?? r.usd);
      console.log(JSON.stringify({ url, ...r, tier }, null, 2));
    })
    .catch((e) => {
      console.error("Probe failed:", e);
      process.exit(1);
    });
  }
}
