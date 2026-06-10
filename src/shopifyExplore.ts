/**
 * Deterministic, zero-AI explore for Shopify stores.
 *
 * Most of the funnel's target brands run on Shopify, which exposes its whole
 * catalog at the public `/products.json` endpoint (paged, `?limit=250&page=N`).
 * That makes the expensive Stagehand+Gemini exploration unnecessary for them:
 * one fetch detects the platform, a couple more sample product pages for
 * JSON-LD quality, and we can emit a complete crawl config in seconds for $0.
 *
 * The generated config uses the existing `api` discovery method, which
 * `crawl.ts` already executes (items at `data.products`, `{handle}` URL
 * template, stops on a short page) — no crawl-side changes needed.
 *
 * Non-Shopify sites return null and fall back to the LLM explore.
 */

import {
  extractJsonLdBlocks,
  findProductInJsonLd,
  extractMetaContent,
  normalizeOffers,
} from "./htmlExtract.js";

const FETCH_TIMEOUT_MS = 15_000;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

interface ShopifyProduct {
  handle?: string;
  title?: string;
  vendor?: string;
}

async function fetchWithTimeout(url: string, accept: string): Promise<Response | null> {
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

async function fetchProductsPage(origin: string, limit: number): Promise<ShopifyProduct[] | null> {
  const res = await fetchWithTimeout(`${origin}/products.json?limit=${limit}`, "application/json");
  if (!res || !res.ok) return null;
  try {
    const data = (await res.json()) as { products?: unknown };
    if (!Array.isArray(data.products)) return null;
    return data.products as ShopifyProduct[];
  } catch {
    return null;
  }
}

/**
 * Cheap platform check: one request, tiny payload. Returns true only when the
 * site serves a non-empty Shopify products feed (i.e. the fast path will work).
 */
export async function isShopifyStore(url: string): Promise<boolean> {
  let origin: string;
  try {
    origin = new URL(url).origin;
  } catch {
    return false;
  }
  const products = await fetchProductsPage(origin, 1);
  return products !== null && products.length > 0;
}

interface ProductPageSample {
  url: string;
  hasJsonLd: boolean;
  hasOpenGraph: boolean;
  missingFields: string[];
}

async function sampleProductPage(origin: string, handle: string): Promise<ProductPageSample | null> {
  const url = `${origin}/products/${handle}`;
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

/**
 * Detect Shopify and build a complete, schema-valid crawl config without any
 * AI. Returns null when the site is not Shopify (or the feed is unusable),
 * letting the caller fall back to the Stagehand/Gemini explore.
 */
export async function tryShopifyExplore(
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

  const firstPage = await fetchProductsPage(origin, 250);
  if (!firstPage || firstPage.length === 0) return null;
  log(`Shopify detected: ${origin}/products.json returned ${firstPage.length} products.`);

  // Brand stores almost always set `vendor` to the brand itself — prefer it as
  // the display name when one vendor dominates the first page.
  const vendorCounts = new Map<string, number>();
  for (const p of firstPage) {
    const v = (p.vendor ?? "").trim();
    if (v) vendorCounts.set(v, (vendorCounts.get(v) ?? 0) + 1);
  }
  const topVendor = [...vendorCounts.entries()].sort((a, b) => b[1] - a[1])[0];
  const displayName =
    topVendor && topVendor[1] >= firstPage.length * 0.6 ? topVendor[0] : identity.displayName;

  // Sample two product pages for JSON-LD/OG quality (plain fetch — Shopify
  // renders both server-side).
  const handles = firstPage.map((p) => p.handle).filter((h): h is string => !!h);
  const samples: ProductPageSample[] = [];
  for (const handle of handles.slice(0, 2)) {
    const sample = await sampleProductPage(origin, handle);
    if (sample) {
      samples.push(sample);
      log(
        `Sampled ${sample.url} — JSON-LD: ${sample.hasJsonLd ? "yes" : "no"}, OG: ${sample.hasOpenGraph ? "yes" : "no"}${
          sample.missingFields.length ? `, missing: ${sample.missingFields.join(", ")}` : ""
        }`,
      );
    }
    await new Promise((r) => setTimeout(r, 500));
  }

  const allHaveJsonLd = samples.length > 0 && samples.every((s) => s.hasJsonLd);
  const anyOpenGraph = samples.some((s) => s.hasOpenGraph);
  const missingFields = [...new Set(samples.flatMap((s) => s.missingFields))];
  // name/image/price are what upload.ts actually needs; brand/description are nice-to-have.
  const missingCritical = missingFields.filter((f) => f === "name" || f === "image" || f === "price");

  const jsonLdCompleteness: "high" | "medium" | "low" = !allHaveJsonLd
    ? "low"
    : missingCritical.length > 0
      ? "medium"
      : "high";
  const overallRecommendation: "recommended" | "usable" | "not recommended" =
    jsonLdCompleteness === "high" ? "recommended" : allHaveJsonLd || anyOpenGraph ? "usable" : "not recommended";

  const exactCount = firstPage.length < 250;

  return {
    retailer: identity.retailer,
    retailerDisplayName: displayName,
    baseUrl: origin,
    discovery: {
      method: "api",
      api: {
        endpoint: `${origin}/products.json`,
        method: "GET",
        paginationParam: "page",
        pageSize: 250,
        productUrlTemplate: `${origin}/products/{handle}`,
      },
    },
    productPage: {
      hasJsonLd: allHaveJsonLd,
      jsonLdNotes: allHaveJsonLd
        ? "Shopify server-rendered JSON-LD (sampled via plain fetch)."
        : "No product JSON-LD found on sampled pages; OG fallback " +
          (anyOpenGraph ? "available." : "also missing."),
      hasOpenGraph: anyOpenGraph,
    },
    requestConfig: {
      requiresJsRendering: false,
      delayBetweenRequestsMs: 2000,
      blocksHeadlessBrowsers: false,
      notes: "Shopify store — config generated deterministically from /products.json (no AI).",
    },
    dataQuality: {
      jsonLdCompleteness,
      estimatedProductCount: firstPage.length,
      sampleProductUrls: samples.map((s) => s.url),
      missingFields,
      overallRecommendation,
    },
    generatedBy: exactCount ? "shopify-fast-path" : "shopify-fast-path (count is first page only, 250+)",
  };
}
