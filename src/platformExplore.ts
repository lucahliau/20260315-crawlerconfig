/**
 * Deterministic, zero-AI explore paths beyond Shopify.
 *
 * The explore ladder tries cheap platform fingerprints before spending any AI:
 *   1. Shopify        (shopifyExplore.ts — /products.json)
 *   2. WooCommerce    (here — /wp-json/wc/store/v1/products, public Store API)
 *   3. Any site with a product sitemap (here — robots.txt/sitemap.xml walk +
 *      URL-pattern inference + JSON-LD verification). This covers Squarespace,
 *      BigCommerce, Magento, SFCC and most custom storefronts, since product
 *      sitemaps + JSON-LD are table stakes for SEO.
 *
 * Every helper returns a complete, schema-valid config object or null —
 * null means "fall through to the next rung" (ultimately the AI explore).
 */

import {
  extractJsonLdBlocks,
  findProductInJsonLd,
  extractMetaContent,
  normalizeOffers,
} from "./htmlExtract.js";
import { extractLocsFromXml, fetchSitemapXml, rankSubSitemaps } from "./crawl.js";

const FETCH_TIMEOUT_MS = 15_000;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

export async function fetchWithTimeout(url: string, accept: string): Promise<Response | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    return await fetch(url, {
      headers: { "User-Agent": USER_AGENT, Accept: accept },
      redirect: "follow",
      signal: controller.signal,
    });
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// Shared product-page quality sampling (plain fetch + JSON-LD/OG checks)
// ---------------------------------------------------------------------------

export interface UrlQualitySample {
  url: string;
  hasJsonLd: boolean;
  hasOpenGraph: boolean;
  missingFields: string[];
}

export async function sampleUrlQuality(url: string): Promise<UrlQualitySample | null> {
  const res = await fetchWithTimeout(url, "text/html");
  if (!res || !res.ok) return null;
  const html = await res.text();
  const product = findProductInJsonLd(extractJsonLdBlocks(html));
  const hasOpenGraph = !!(extractMetaContent(html, "og:title") && extractMetaContent(html, "og:image"));
  const missingFields: string[] = [];
  if (product) {
    if (!product.name) missingFields.push("name");
    const hasImage = !!product.image && (!Array.isArray(product.image) || product.image.length > 0);
    if (!hasImage) missingFields.push("image");
    const offers = normalizeOffers(product.offers);
    if (!offers.some((o) => o.price != null || o.lowPrice != null)) missingFields.push("price");
    if (!product.brand) missingFields.push("brand");
    if (!product.description) missingFields.push("description");
  }
  return { url, hasJsonLd: !!product, hasOpenGraph, missingFields };
}

export interface QualityBlocks {
  productPage: Record<string, unknown>;
  dataQuality: Record<string, unknown>;
}

/** Turn sampled pages into the config's productPage + dataQuality blocks. */
export function buildQualityBlocks(
  samples: UrlQualitySample[],
  estimatedProductCount: number,
): QualityBlocks {
  const allHaveJsonLd = samples.length > 0 && samples.every((s) => s.hasJsonLd);
  const anyOpenGraph = samples.some((s) => s.hasOpenGraph);
  const missingFields = [...new Set(samples.flatMap((s) => s.missingFields))];
  const missingCritical = missingFields.filter((f) => f === "name" || f === "image" || f === "price");

  const jsonLdCompleteness: "high" | "medium" | "low" = !allHaveJsonLd
    ? "low"
    : missingCritical.length > 0
      ? "medium"
      : "high";
  const overallRecommendation: "recommended" | "usable" | "not recommended" =
    jsonLdCompleteness === "high"
      ? "recommended"
      : allHaveJsonLd || anyOpenGraph
        ? "usable"
        : "not recommended";

  return {
    productPage: {
      hasJsonLd: allHaveJsonLd,
      jsonLdNotes: allHaveJsonLd
        ? "Server-rendered product JSON-LD (sampled via plain fetch)."
        : "No product JSON-LD on sampled pages; OG fallback " +
          (anyOpenGraph ? "available." : "also missing."),
      hasOpenGraph: anyOpenGraph,
    },
    dataQuality: {
      jsonLdCompleteness,
      estimatedProductCount,
      sampleProductUrls: samples.map((s) => s.url),
      missingFields,
      overallRecommendation,
    },
  };
}

const DEFAULT_REQUEST_CONFIG = {
  requiresJsRendering: false,
  delayBetweenRequestsMs: 2000,
  blocksHeadlessBrowsers: false,
};

// ---------------------------------------------------------------------------
// WooCommerce — public Store API (no auth): /wp-json/wc/store/v1/products
// ---------------------------------------------------------------------------

interface WooProduct {
  permalink?: string;
  name?: string;
}

export async function tryWooCommerceExplore(
  url: string,
  identity: { retailer: string; displayName: string },
  onLog?: (msg: string) => void,
): Promise<Record<string, unknown> | null> {
  const log = (msg: string) => onLog?.(msg);
  let origin: string;
  try {
    origin = new URL(url).origin;
  } catch {
    return null;
  }

  const endpoint = `${origin}/wp-json/wc/store/v1/products`;
  const res = await fetchWithTimeout(`${endpoint}?per_page=5`, "application/json");
  if (!res || !res.ok) return null;
  let products: WooProduct[];
  try {
    const data = (await res.json()) as unknown;
    if (!Array.isArray(data) || data.length === 0) return null;
    products = data as WooProduct[];
  } catch {
    return null;
  }
  const permalinks = products
    .map((p) => p.permalink)
    .filter((p): p is string => typeof p === "string" && p.startsWith("http"));
  if (permalinks.length === 0) return null;

  // The Store API exposes the catalog size in a response header.
  const totalHeader = parseInt(res.headers.get("x-wp-total") ?? "", 10);
  const estimatedProductCount = Number.isFinite(totalHeader) ? totalHeader : products.length;
  log(`WooCommerce detected: ${endpoint} (${estimatedProductCount} products).`);

  const samples: UrlQualitySample[] = [];
  for (const link of permalinks.slice(0, 2)) {
    const s = await sampleUrlQuality(link);
    if (s) {
      samples.push(s);
      log(`Sampled ${s.url} — JSON-LD: ${s.hasJsonLd ? "yes" : "no"}, OG: ${s.hasOpenGraph ? "yes" : "no"}`);
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  const quality = buildQualityBlocks(samples, estimatedProductCount);

  return {
    retailer: identity.retailer,
    retailerDisplayName: identity.displayName,
    baseUrl: origin,
    discovery: {
      method: "api",
      api: {
        endpoint,
        method: "GET",
        paginationParam: "page",
        pageSizeParam: "per_page",
        pageSize: 100,
        productUrlTemplate: "{permalink}",
      },
    },
    ...quality,
    requestConfig: {
      ...DEFAULT_REQUEST_CONFIG,
      notes: "WooCommerce store — config generated deterministically from the public Store API (no AI).",
    },
    generatedBy: "woocommerce-fast-path",
  };
}

// ---------------------------------------------------------------------------
// Universal sitemap explore — works for any platform with a product sitemap
// ---------------------------------------------------------------------------

/** Candidate product-URL path shapes, broadest coverage first. */
const PRODUCT_PATH_PATTERNS: { source: string; re: RegExp }[] = [
  { source: "\\/products\\/[^/?#]+", re: /\/products\/[^/?#]+/ },
  { source: "\\/product\\/[^/?#]+", re: /\/product\/[^/?#]+/ },
  { source: "\\/shop\\/p\\/[^/?#]+", re: /\/shop\/p\/[^/?#]+/ },
  { source: "\\/store\\/p\\/[^/?#]+", re: /\/store\/p\/[^/?#]+/ },
  { source: "\\/p\\/[^/?#]+", re: /\/p\/[^/?#]+/ },
  { source: "\\/item\\/[^/?#]+", re: /\/item\/[^/?#]+/ },
  { source: "\\/produkt\\/[^/?#]+", re: /\/produkt\/[^/?#]+/ },
  { source: "\\/itm\\/[^/?#]+", re: /\/itm\/[^/?#]+/ },
];

const MIN_PATTERN_MATCHES = 8;

/**
 * Paths that match product-ish shapes but are editorial/support content
 * (e.g. /journal/products/the-story-of-the-hoodie). Excluded from pattern
 * inference and sampling so blog posts can't masquerade as the catalog.
 */
export const NON_PRODUCT_PATH =
  /\/(journal|blog|blogs|news|article|articles|stories|story|magazine|editorial|press|lookbook|about|policy|policies|legal|help|faq|support|careers|account|cart|checkout)\//i;

export async function discoverSitemapCandidates(origin: string): Promise<string[]> {
  const candidates: string[] = [];
  const robotsRes = await fetchWithTimeout(`${origin}/robots.txt`, "text/plain");
  if (robotsRes?.ok) {
    const text = await robotsRes.text();
    for (const m of text.matchAll(/^\s*sitemap:\s*(\S+)\s*$/gim)) {
      candidates.push(m[1]);
    }
  }
  // Locale-split sites list one sitemap per locale in robots.txt with identical
  // shapes — probing a handful is enough, and the well-known fallbacks are only
  // worth trying when robots.txt offered nothing.
  if (candidates.length > 0) return [...new Set(candidates)].slice(0, 4);
  return ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml", "/wp-sitemap.xml"].map(
    (p) => `${origin}${p}`,
  );
}

/** Walk sitemaps (bounded) and collect page URLs. */
export async function collectSitemapUrls(
  entry: string,
  onLog?: (msg: string) => void,
): Promise<string[]> {
  const collected: string[] = [];
  const visited = new Set<string>();
  let fetches = 0;
  const MAX_FETCHES = 12;
  const MAX_LOCS = 8000;

  const walk = async (url: string, depth: number): Promise<void> => {
    if (visited.has(url) || fetches >= MAX_FETCHES || collected.length >= MAX_LOCS || depth > 2) return;
    visited.add(url);
    fetches++;
    let xml: string | null = null;
    try {
      xml = await fetchSitemapXml(url);
    } catch {
      return;
    }
    if (!xml) return;
    if (/<sitemapindex[\s>]/i.test(xml)) {
      for (const sub of rankSubSitemaps(extractLocsFromXml(xml))) {
        await walk(sub, depth + 1);
        if (fetches >= MAX_FETCHES || collected.length >= MAX_LOCS) break;
      }
      return;
    }
    const locs = extractLocsFromXml(xml);
    onLog?.(`Sitemap ${url}: ${locs.length} URLs`);
    for (const loc of locs) {
      collected.push(loc);
      if (collected.length >= MAX_LOCS) break;
    }
  };

  await walk(entry, 0);
  return collected;
}

export async function trySitemapExplore(
  url: string,
  identity: { retailer: string; displayName: string },
  onLog?: (msg: string) => void,
): Promise<Record<string, unknown> | null> {
  const log = (msg: string) => onLog?.(msg);
  let origin: string;
  try {
    origin = new URL(url).origin;
  } catch {
    return null;
  }

  const candidates = await discoverSitemapCandidates(origin);
  for (const candidate of candidates) {
    const pageUrls = await collectSitemapUrls(candidate, onLog);
    if (pageUrls.length === 0) continue;

    // Infer which path shape marks product pages on this site, ignoring
    // editorial paths that merely contain a product-ish segment.
    const productish = pageUrls.filter((u) => !NON_PRODUCT_PATH.test(u));
    let best: { source: string; re: RegExp; matches: string[] } | null = null;
    for (const p of PRODUCT_PATH_PATTERNS) {
      const matches = productish.filter((u) => p.re.test(u));
      if (matches.length >= MIN_PATTERN_MATCHES && (!best || matches.length > best.matches.length)) {
        best = { ...p, matches };
      }
    }
    if (!best) {
      log(`Sitemap ${candidate}: no product URL pattern matched ≥${MIN_PATTERN_MATCHES} URLs.`);
      continue;
    }
    log(`Sitemap ${candidate}: pattern ${best.source} matched ${best.matches.length} URLs.`);

    // Verify the pattern actually lands on product pages before trusting it —
    // sample three URLs spread across the match list.
    const sampleAt = [0, Math.floor(best.matches.length / 2), best.matches.length - 1];
    const samples: UrlQualitySample[] = [];
    for (const idx of [...new Set(sampleAt)]) {
      const s = await sampleUrlQuality(best.matches[idx]);
      if (s) {
        samples.push(s);
        log(`Sampled ${s.url} — JSON-LD: ${s.hasJsonLd ? "yes" : "no"}, OG: ${s.hasOpenGraph ? "yes" : "no"}`);
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    if (!samples.some((s) => s.hasJsonLd || s.hasOpenGraph)) {
      log("Sampled pages had neither product JSON-LD nor OG tags — not confident, skipping fast path.");
      continue;
    }

    // Exclude the editorial paths from the crawl pattern itself so the crawl
    // result matches what we verified here.
    const patternSource = `^(?!.*\\/(journal|blogs?|news|articles?|stories)\\/).*${best.source}`;
    const quality = buildQualityBlocks(samples, best.matches.length);
    return {
      retailer: identity.retailer,
      retailerDisplayName: identity.displayName,
      baseUrl: origin,
      discovery: {
        method: "sitemap",
        sitemap: { url: candidate, productUrlPattern: patternSource },
      },
      ...quality,
      requestConfig: {
        ...DEFAULT_REQUEST_CONFIG,
        notes: "Product sitemap detected — config generated deterministically (no AI).",
      },
      generatedBy: "sitemap-fast-path",
    };
  }
  return null;
}
