import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Pool } from "pg";
import { Stagehand } from "@browserbasehq/stagehand";
import { getStagehandModel } from "./stagehandModel.js";
import { getStagehandBrowserOptions } from "./stagehandConfig.js";
import type { Config } from "./schemas/config.js";
import {
  inferCurrencyFromHostname,
  normalizePriceTriple,
} from "./currencyToUsd.js";
import { ensureFreshFxRates } from "./fxRates.js";
import { getShopifyStoreCurrency } from "./shopifyStore.js";
import { resolveBrand } from "./brandName.js";
import {
  extractJsonLdBlocks,
  findProductInJsonLd,
  extractMetaContent,
  normalizeOffers,
} from "./htmlExtract.js";
import { classifyItem, normalizeGender, type Gender } from "./classify.js";
import { mapShopifyProductFields, type ShopifyProduct } from "./shopifyIngest.js";

// ---------------------------------------------------------------------------
// Logging — same swappable pattern as explore.ts / crawl.ts
// ---------------------------------------------------------------------------

let _logFn: (msg: string) => void = console.log;

function log(...args: unknown[]): void {
  _logFn(args.map(String).join(" "));
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const USER_AGENT =
  "CrawlerConfigBot/1.0 (product-scraper; +https://github.com/example/crawler-config)";

const FETCH_TIMEOUT_MS = 20_000;
const IMAGE_TIMEOUT_MS = 30_000;
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 2000;

const UPLOAD_NAV_TIMEOUT_MS = parseInt(process.env.UPLOAD_NAV_TIMEOUT_MS ?? String(FETCH_TIMEOUT_MS), 10);
const UPLOAD_BROWSER_REFRESH_EVERY_N = parseInt(process.env.UPLOAD_BROWSER_REFRESH_EVERY_N ?? "0", 10);
const UPLOAD_BROWSER_REFRESH_AFTER_CONSECUTIVE_FAILURES = parseInt(
  process.env.UPLOAD_BROWSER_REFRESH_AFTER_CONSECUTIVE_FAILURES ?? "4",
  10,
);

const CATEGORY_MAP: Record<string, string[]> = {
  tops: ["t-shirt", "tee", "shirt", "tank", "polo", "camisole", "hoodie", "long sleeve", "blouse", "henley", "crew", "sweater", "pullover", "sweatshirt", "knit", "jersey"],
  bottoms: ["jeans", "trousers", "chinos", "joggers", "skirt", "shorts", "pants", "leggings", "culottes"],
  outerwear: ["jacket", "blazer", "bomber", "coat", "vest", "fleece", "raincoat", "cardigan", "parka", "windbreaker", "quarter-zip", "quarter zip", "full-zip", "full zip", "zip"],
  shoes: ["sneakers", "boots", "loafers", "sandals", "flats", "heels", "mules", "slides", "oxfords"],
  accessories: ["bag", "belt", "hat", "sunglasses", "scarf", "scarves", "watch", "jewelry", "wallet", "coaster", "garment bag", "cap", "beanie", "tote", "backpack", "pouch"],
};

const COLOR_DICTIONARY = new Set([
  "black", "white", "red", "blue", "navy", "green", "grey", "gray", "beige",
  "tan", "olive", "burgundy", "cream", "brown", "pink", "purple", "orange",
  "yellow", "teal", "coral", "maroon", "ivory", "charcoal", "khaki",
  "indigo", "lavender", "mint", "sage", "rust", "sand", "slate", "wine",
  "camel", "oat", "heather", "stone", "denim", "chambray", "ecru",
]);

const SIZE_PATTERNS = [
  /^(XXS|XS|S|M|L|XL|XXL|XXXL|2XL|3XL|4XL)$/i,
  /^(\d{1,2})$/,
  /^(\d{2}[Wx]\d{2}[L]?)$/i,
  /^(One Size|OS|OSFA)$/i,
];

// ---------------------------------------------------------------------------
// R2 client (lazy singleton)
// ---------------------------------------------------------------------------

let _r2Client: S3Client | null = null;

function getR2Client(): S3Client {
  if (!_r2Client) {
    _r2Client = new S3Client({
      region: "auto",
      endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID!,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
      },
    });
  }
  return _r2Client;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ClothingItemInput {
  name: string | null;
  description: string | null;
  brand: string | null;
  category: string | null;
  subcategory: string | null;
  price: number | null;
  currency: string;
  salePrice: number | null;
  compareAtPrice: number | null;
  /** Stock from the source listing/markup: null unknown, false = sold out. */
  inStock: boolean | null;
  imageUrl: string | null;
  images: string[];
  colors: string[];
  sizes: string[];
  tags: string[];
  gender: string | null;
  productType: string | null;
  isClothing: boolean;
  classificationConfidence: number | null;
  sourceUrl: string;
  metadata: Record<string, unknown> | null;
  retailer: string;
  externalId: string | null;
  manufacturerCode: string | null;
  /** True when `currency` came from a hostname/USD fallback rather than the
   *  page markup — lets the loop upgrade it from the store's /cart.json. */
  currencyInferred?: boolean;
}

export interface UploadProgress {
  uploaded: number;
  skipped: number;
  failed: number;
  total: number;
  currentUrl: string;
}

export interface UploadResult {
  retailer: string;
  uploadedAt: string;
  uploaded: number;
  skipped: number;
  failed: number;
  total: number;
}

export interface UploadItemOutcome {
  url: string;
  status: "uploaded" | "skipped" | "failed";
  error?: string;
  externalId?: string;
  itemName?: string;
  imageCount?: number;
  uploadedToR2?: boolean;
  upsertedToDb?: boolean;
  metadata?: Record<string, unknown>;
}

export interface UploadRunOptions {
  resumeUploadedUrls?: Iterable<string>;
  onItemResult?: (result: UploadItemOutcome) => void | Promise<void>;
}

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchWithRetry(
  url: string,
  timeoutMs = FETCH_TIMEOUT_MS,
): Promise<Response> {
  // Track the last failure so the final throw carries a real cause (HTTP
  // status or network error) instead of an opaque "after N attempts" — the
  // detail surfaces in scrape_errors and the worker issue feed, and lets
  // classifyError tag rate-limiting (429) distinctly from a genuine bug.
  let lastReason = "no response";
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": USER_AGENT },
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (res.ok) return res;
      if (res.status === 429 || res.status >= 500) {
        lastReason = `HTTP ${res.status}`;
        // Honor Retry-After when the site provides it (seconds or HTTP date),
        // capped so a hostile header can't stall the worker for minutes.
        const retryAfterMs = parseRetryAfter(res.headers.get("retry-after"));
        const backoff = retryAfterMs ?? BACKOFF_BASE_MS * 2 ** (attempt - 1);
        log(`    HTTP ${res.status} on attempt ${attempt}/${MAX_RETRIES}, retrying in ${backoff}ms...`);
        await delay(backoff);
        continue;
      }
      return res;
    } catch (err) {
      clearTimeout(timeout);
      lastReason = err instanceof Error ? err.message : String(err);
      if (attempt === MAX_RETRIES) throw err;
      const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1);
      log(`    Fetch error attempt ${attempt}/${MAX_RETRIES}: ${lastReason}, retrying in ${backoff}ms...`);
      await delay(backoff);
    }
  }
  throw new Error(`Failed to fetch ${url} after ${MAX_RETRIES} attempts (last: ${lastReason})`);
}

/**
 * Parse a Retry-After header into milliseconds. Accepts a delta-seconds value
 * or an HTTP-date. Returns null when absent/unparseable; clamps to [0, 30s] so
 * a single hostile response can't park the worker.
 */
function parseRetryAfter(header: string | null): number | null {
  if (!header) return null;
  const seconds = Number(header.trim());
  let ms: number | null = null;
  if (Number.isFinite(seconds)) {
    ms = seconds * 1000;
  } else {
    const when = Date.parse(header);
    if (!Number.isNaN(when)) ms = when - Date.now();
  }
  if (ms === null || !Number.isFinite(ms)) return null;
  return Math.max(0, Math.min(ms, 30_000));
}

// ---------------------------------------------------------------------------
// HTML parsing helpers — moved to ./htmlExtract.ts (dependency-free, shared with
// priceProbe.ts). Imported above: extractJsonLdBlocks, findProductInJsonLd,
// extractMetaContent, normalizeOffers.
// ---------------------------------------------------------------------------

function extractTitle(html: string): string | null {
  const m = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m ? m[1].trim() : null;
}

// ---------------------------------------------------------------------------
// Layer 1: JSON-LD extraction
// ---------------------------------------------------------------------------

function extractFromJsonLd(product: Record<string, unknown>): Partial<ClothingItemInput> {
  const result: Partial<ClothingItemInput> = {};

  result.name = typeof product.name === "string" ? product.name : null;
  result.description = typeof product.description === "string" ? product.description : null;

  // Brand
  if (product.brand) {
    if (typeof product.brand === "string") {
      result.brand = product.brand;
    } else if (typeof product.brand === "object" && product.brand !== null) {
      result.brand = String((product.brand as Record<string, unknown>).name ?? "");
    }
  }

  // Category
  if (typeof product.category === "string") {
    result.category = product.category;
  }

  // Offers -> price, currency, sizes, colors
  const offers = normalizeOffers(product.offers);
  if (offers.length > 0) {
    const firstWithPrice = offers.find((o) => o.price != null);
    if (firstWithPrice) {
      const p = parseFloat(String(firstWithPrice.price));
      if (!isNaN(p)) result.price = p;
      const pc = firstWithPrice.priceCurrency;
      if (pc != null && String(pc).trim() !== "") {
        result.currency = String(pc).trim();
      }
    }
    // Fall back to lowPrice/highPrice
    if (result.price == null && product.offers && typeof product.offers === "object" && !Array.isArray(product.offers)) {
      const aggOffer = product.offers as Record<string, unknown>;
      const lowPrice = parseFloat(String(aggOffer.lowPrice ?? ""));
      if (!isNaN(lowPrice)) {
        result.price = lowPrice;
        const pc = aggOffer.priceCurrency;
        if (pc != null && String(pc).trim() !== "") {
          result.currency = String(pc).trim();
        }
      }
    }

    // Availability from schema.org offers ("http://schema.org/InStock",
    // "OutOfStock", "SoldOut", "Discontinued"…): in stock when ANY offer says
    // so; sold out only when availability is present and NONE do.
    let sawAvailability = false;
    let anyInStock = false;
    for (const offer of offers) {
      const availability = String(offer.availability ?? "").toLowerCase();
      if (!availability) continue;
      sawAvailability = true;
      if (availability.includes("instock") || availability.includes("limitedavailability") || availability.includes("preorder")) {
        anyInStock = true;
      }
    }
    if (sawAvailability) result.inStock = anyInStock;

    // Extract sizes and colors from offer variant names
    const sizes = new Set<string>();
    const colors = new Set<string>();
    for (const offer of offers) {
      const optionName = String(offer.name ?? offer.sku ?? "");
      for (const part of optionName.split(/[\s\/\-,]+/)) {
        const trimmed = part.trim();
        if (!trimmed) continue;
        if (SIZE_PATTERNS.some((p) => p.test(trimmed))) {
          sizes.add(trimmed.toUpperCase());
        }
        if (COLOR_DICTIONARY.has(trimmed.toLowerCase())) {
          colors.add(trimmed.toLowerCase());
        }
      }
    }
    if (sizes.size > 0) result.sizes = Array.from(sizes);
    if (colors.size > 0) result.colors = Array.from(colors);
  }

  // Images
  const images = normalizeImages(product.image ?? product.images);
  if (images.length > 0) result.images = images;

  // SKU / MPN / GTIN -> externalId, manufacturerCode
  if (typeof product.sku === "string" && product.sku) {
    result.externalId = product.sku;
    result.manufacturerCode = product.sku;
  }
  if (typeof product.mpn === "string" && product.mpn) {
    result.manufacturerCode = product.mpn;
  }
  if (typeof product.gtin === "string" && product.gtin) {
    if (!result.externalId) result.externalId = product.gtin;
  }
  if (typeof product.gtin13 === "string" && product.gtin13) {
    if (!result.externalId) result.externalId = product.gtin13;
  }

  // Color at product level
  if (typeof product.color === "string" && product.color) {
    if (!result.colors || result.colors.length === 0) {
      result.colors = [product.color.toLowerCase()];
    }
  }

  result.metadata = product;
  return result;
}

function normalizeImages(img: unknown): string[] {
  if (!img) return [];
  if (typeof img === "string") return [img];
  if (Array.isArray(img)) {
    return img
      .map((i) => {
        if (typeof i === "string") return i;
        if (typeof i === "object" && i !== null) {
          const obj = i as Record<string, unknown>;
          return String(obj.url ?? obj.contentUrl ?? obj["@id"] ?? "");
        }
        return "";
      })
      .filter(Boolean);
  }
  if (typeof img === "object" && img !== null) {
    const obj = img as Record<string, unknown>;
    const url = String(obj.url ?? obj.contentUrl ?? "");
    return url ? [url] : [];
  }
  return [];
}

// ---------------------------------------------------------------------------
// Layer 2: Open Graph extraction
// ---------------------------------------------------------------------------

function extractFromOg(html: string): Partial<ClothingItemInput> {
  const result: Partial<ClothingItemInput> = {};

  const ogTitle = extractMetaContent(html, "og:title");
  if (ogTitle) result.name = ogTitle;

  const ogDesc = extractMetaContent(html, "og:description");
  if (ogDesc) result.description = ogDesc;

  const ogImage = extractMetaContent(html, "og:image");
  if (ogImage) result.images = [ogImage];

  const ogPrice = extractMetaContent(html, "og:price:amount") ?? extractMetaContent(html, "product:price:amount");
  if (ogPrice) {
    const p = parseFloat(ogPrice);
    if (!isNaN(p)) result.price = p;
  }

  // Best-effort sale signal: Facebook product tags expose the pre-sale price as
  // `product:original_price:amount`. Only treat it as a sale when it exceeds the
  // current price (the invariant is re-checked post-FX in normalizeItemPriceToUsd).
  const ogOriginal = extractMetaContent(html, "product:original_price:amount");
  if (ogOriginal && result.price != null) {
    const op = parseFloat(ogOriginal);
    if (!isNaN(op) && op > result.price) {
      result.compareAtPrice = op;
      result.salePrice = result.price;
    }
  }

  const ogCurrency = extractMetaContent(html, "og:price:currency") ?? extractMetaContent(html, "product:price:currency");
  if (ogCurrency) result.currency = ogCurrency;

  const ogBrand = extractMetaContent(html, "product:brand") ?? extractMetaContent(html, "og:brand");
  if (ogBrand) result.brand = ogBrand;

  const ogColor = extractMetaContent(html, "product:color");
  if (ogColor) result.colors = [ogColor.toLowerCase()];

  const ogSize = extractMetaContent(html, "product:size");
  if (ogSize) result.sizes = [ogSize];

  return result;
}

// ---------------------------------------------------------------------------
// Layer 3: HTML meta/selector fallback
// ---------------------------------------------------------------------------

function extractFromHtml(html: string, config: Config): Partial<ClothingItemInput> {
  const result: Partial<ClothingItemInput> = {};

  // Title tag (strip " | Brand" or " - Brand" suffix)
  const title = extractTitle(html);
  if (title) {
    result.name = title.replace(/\s*[|\-–—]\s*[^|\-–—]+$/, "").trim();
  }

  // Meta description
  const metaDesc = extractMetaContent(html, "description");
  if (metaDesc) result.description = metaDesc;

  // Meta keywords -> tags
  const keywords = extractMetaContent(html, "keywords");
  if (keywords) {
    result.tags = keywords.split(",").map((k) => k.trim().toLowerCase()).filter(Boolean);
  }

  // Price from itemprop or data attributes via regex
  const priceMatch = html.match(/itemprop\s*=\s*["']price["'][^>]*content\s*=\s*["']([^"']+)["']/i)
    ?? html.match(/content\s*=\s*["']([^"']+)["'][^>]*itemprop\s*=\s*["']price["']/i);
  if (priceMatch) {
    const p = parseFloat(priceMatch[1]);
    if (!isNaN(p)) result.price = p;
  }

  const currencyMatch = html.match(/itemprop\s*=\s*["']priceCurrency["'][^>]*content\s*=\s*["']([^"']+)["']/i)
    ?? html.match(/content\s*=\s*["']([^"']+)["'][^>]*itemprop\s*=\s*["']priceCurrency["']/i);
  if (currencyMatch) result.currency = currencyMatch[1];

  // Additional images from config selector (regex approximation for non-browser mode)
  if (config.productPage.additionalImageSelector) {
    const imgUrls = extractImageUrlsFromHtml(html);
    if (imgUrls.length > 0) result.images = imgUrls;
  }

  return result;
}

function extractImageUrlsFromHtml(html: string): string[] {
  const urls: string[] = [];
  const re = /<img[^>]*src\s*=\s*["']([^"']+)["'][^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    const src = m[1];
    if (src && !src.includes("data:") && !src.includes("svg") && !src.includes("icon") && !src.includes("logo")) {
      urls.push(src);
    }
  }
  // Also check srcset
  const srcsetRe = /<img[^>]*srcset\s*=\s*["']([^"']+)["'][^>]*>/gi;
  while ((m = srcsetRe.exec(html)) !== null) {
    const firstUrl = m[1].split(",")[0]?.split(/\s+/)[0];
    if (firstUrl && !firstUrl.includes("data:")) {
      urls.push(firstUrl);
    }
  }
  return [...new Set(urls)];
}

// ---------------------------------------------------------------------------
// Layer 4: Config-driven inference
// ---------------------------------------------------------------------------

function inferFromConfig(
  partial: Partial<ClothingItemInput>,
  url: string,
  config: Config,
  html: string,
): void {
  const breadcrumbText = extractBreadcrumbText(html);

  // Category / subcategory inference
  if (!partial.category) {
    const inferred = inferCategory(breadcrumbText + " " + (partial.name ?? ""));
    if (inferred) {
      partial.category = inferred.category;
      if (!partial.subcategory) partial.subcategory = inferred.subcategory;
    }
  }

  // Heuristic classification — resolves gender (controlled vocab), productType,
  // and clothing-vs-non-clothing from all signals at once. A per-retailer
  // gender pattern in the config (if any) is authoritative over the generic
  // inference; everything else comes from the shared classifier. Non-wearables
  // (wallets, candles, camping gear…) get isClothing=false so the backend hides
  // them — see src/classify.ts.
  const classification = classifyItem({
    name: partial.name,
    category: partial.category,
    subcategory: partial.subcategory,
    tags: partial.tags,
    sourceUrl: url,
    breadcrumbs: breadcrumbText ? [breadcrumbText] : null,
    retailer: partial.retailer ?? config.retailer,
  });
  partial.gender = genderFromConfig(url, config) ?? classification.gender;
  partial.productType = classification.productType;
  partial.isClothing = classification.isClothing;
  partial.classificationConfidence = classification.confidence;

  // Colors from product name if still empty
  if (!partial.colors || partial.colors.length === 0) {
    partial.colors = extractColorsFromText(partial.name ?? "");
  }

  // Sizes from offers/options if still empty (scan HTML for common size option patterns)
  if (!partial.sizes || partial.sizes.length === 0) {
    partial.sizes = extractSizesFromHtml(html);
  }

  // Tags generation
  if (!partial.tags || partial.tags.length === 0) {
    partial.tags = generateTags(partial);
  }

  // Brand fallback
  if (!partial.brand) {
    partial.brand = config.retailerDisplayName;
  }

  // ExternalId fallback: URL slug
  if (!partial.externalId) {
    partial.externalId = extractSlugFromUrl(url);
  }
}

function extractBreadcrumbText(html: string): string {
  // Look for BreadcrumbList in JSON-LD
  const blocks = extractJsonLdBlocks(html);
  for (const block of blocks) {
    const items = Array.isArray(block["@graph"]) ? (block["@graph"] as Record<string, unknown>[]) : [block];
    for (const item of items) {
      const type = String(item["@type"] ?? "").toLowerCase();
      if (type.includes("breadcrumblist") && Array.isArray(item.itemListElement)) {
        return (item.itemListElement as Record<string, unknown>[])
          .map((el) => String(el.name ?? ""))
          .filter(Boolean)
          .join(" ");
      }
    }
  }
  return "";
}

function inferCategory(text: string): { category: string; subcategory: string | null } | null {
  const lower = text.toLowerCase();
  for (const [category, keywords] of Object.entries(CATEGORY_MAP)) {
    for (const keyword of keywords) {
      if (lower.includes(keyword)) {
        return { category, subcategory: keyword };
      }
    }
  }
  return null;
}

/** Per-retailer gender hint from the config's URL patterns, normalized to the
 * controlled vocab. Authoritative when present (covers non-English path words
 * the generic classifier wouldn't know); otherwise the classifier decides. */
function genderFromConfig(url: string, config: Config): Gender | null {
  if (!config.productPage.genderInUrl?.detected) return null;
  const lowerUrl = url.toLowerCase();
  for (const [gender, pattern] of Object.entries(config.productPage.genderInUrl.patterns)) {
    if (pattern && lowerUrl.includes(pattern.toLowerCase())) {
      return normalizeGender(gender);
    }
  }
  return null;
}

function extractColorsFromText(text: string): string[] {
  const words = text.toLowerCase().split(/[\s\/\-,|]+/);
  const found: string[] = [];
  for (const word of words) {
    if (COLOR_DICTIONARY.has(word)) found.push(word);
  }
  return found;
}

function extractSizesFromHtml(html: string): string[] {
  // Look for common size option patterns in HTML
  const sizes = new Set<string>();

  // Shopify-style option values
  const optionRe = /data-value\s*=\s*["']([^"']+)["']/gi;
  let m: RegExpExecArray | null;
  while ((m = optionRe.exec(html)) !== null) {
    if (SIZE_PATTERNS.some((p) => p.test(m![1].trim()))) {
      sizes.add(m[1].trim().toUpperCase());
    }
  }

  // Select option values
  const selectRe = /<option[^>]*value\s*=\s*["']([^"']+)["'][^>]*>/gi;
  while ((m = selectRe.exec(html)) !== null) {
    const val = m[1].trim();
    if (SIZE_PATTERNS.some((p) => p.test(val))) {
      sizes.add(val.toUpperCase());
    }
  }

  return Array.from(sizes);
}

function generateTags(partial: Partial<ClothingItemInput>): string[] {
  const tags = new Set<string>();
  if (partial.category) tags.add(partial.category);
  if (partial.subcategory) tags.add(partial.subcategory);
  if (partial.brand) tags.add(partial.brand.toLowerCase());

  // Material keywords from name/description
  const text = ((partial.name ?? "") + " " + (partial.description ?? "")).toLowerCase();
  const materials = ["cotton", "linen", "wool", "polyester", "denim", "leather", "silk", "cashmere", "fleece", "nylon", "hemp", "organic", "recycled", "merino"];
  for (const mat of materials) {
    if (text.includes(mat)) tags.add(mat);
  }

  // Style keywords
  const styles = ["casual", "formal", "streetwear", "athletic", "vintage", "classic", "modern", "slim", "relaxed", "performance"];
  for (const style of styles) {
    if (text.includes(style)) tags.add(style);
  }

  return Array.from(tags);
}

function extractSlugFromUrl(url: string): string {
  try {
    const pathname = new URL(url).pathname;
    const segments = pathname.split("/").filter(Boolean);
    return segments[segments.length - 1] ?? pathname;
  } catch {
    return url;
  }
}

// ---------------------------------------------------------------------------
// Multi-layer product extraction
// ---------------------------------------------------------------------------

function extractProductData(html: string, url: string, config: Config): ClothingItemInput {
  const result: ClothingItemInput = {
    name: null,
    description: null,
    brand: null,
    category: null,
    subcategory: null,
    price: null,
    currency: "",
    salePrice: null,
    compareAtPrice: null,
    inStock: null,
    imageUrl: null,
    images: [],
    colors: [],
    sizes: [],
    tags: [],
    gender: null,
    productType: null,
    isClothing: true,
    classificationConfidence: null,
    sourceUrl: url,
    metadata: null,
    retailer: config.retailer,
    externalId: null,
    manufacturerCode: null,
  };

  // Layer 1: JSON-LD (primary)
  const jsonLdBlocks = extractJsonLdBlocks(html);
  const product = findProductInJsonLd(jsonLdBlocks);
  if (product) {
    const jsonLdData = extractFromJsonLd(product);
    mergeInto(result, jsonLdData);
    log("    [L1] JSON-LD: found product data");
  } else {
    log("    [L1] JSON-LD: no product data found");
  }

  // Layer 2: Open Graph (supplement)
  const ogData = extractFromOg(html);
  mergeInto(result, ogData, true);
  if (ogData.name || ogData.price != null) {
    log("    [L2] OG tags: supplemented missing fields");
  }

  // Layer 3: HTML meta/selectors (fallback)
  const htmlData = extractFromHtml(html, config);
  mergeInto(result, htmlData, true);

  // Layer 4: Config-driven inference
  inferFromConfig(result, url, config, html);

  // Currency: if still missing after all layers, infer from hostname or USD
  finalizeCurrencyHint(result, url);

  // Set imageUrl from first image
  if (!result.imageUrl && result.images.length > 0) {
    result.imageUrl = result.images[0];
  }

  return result;
}

function finalizeCurrencyHint(result: ClothingItemInput, url: string): void {
  if (!result.currency?.trim()) {
    result.currency = inferCurrencyFromHostname(url) ?? "USD";
    result.currencyInferred = true;
  }
}

function mergeInto(target: ClothingItemInput, source: Partial<ClothingItemInput>, onlyMissing = false): void {
  for (const [key, value] of Object.entries(source)) {
    if (value == null) continue;
    const k = key as keyof ClothingItemInput;

    if (onlyMissing) {
      const current = target[k];
      if (current != null && current !== "" && !(Array.isArray(current) && current.length === 0)) {
        continue;
      }
    }

    (target as unknown as Record<string, unknown>)[k] = value;
  }
}

// ---------------------------------------------------------------------------
// Gallery rescue (Shopify)
// ---------------------------------------------------------------------------

// The per-page extractor only recovers the single featured image from Shopify
// product pages (their JSON-LD / og:image `image` is scalar), so model-shot
// brands land ONE on-model photo and no product-only fallback — which makes the
// people-photo scan hide the whole product. The canonical `/products/<handle>.json`
// lists the full ordered gallery (model + laydown + detail), recovered below.
const MAX_GALLERY_IMAGES = 12;

function looksShopify(html: string): boolean {
  return (
    html.includes("cdn.shopify.com") ||
    html.includes("/cdn/shop/") ||
    html.includes("Shopify.theme") ||
    html.includes("window.Shopify")
  );
}

// Full ordered image gallery from a Shopify product JSON endpoint. Returns []
// for non-Shopify/odd URLs or on any failure — the caller keeps whatever it
// already extracted, so this is purely additive.
async function fetchShopifyGalleryImages(url: string): Promise<string[]> {
  let jsonUrl: string;
  try {
    const u = new URL(url);
    const handle = u.pathname.match(/\/products\/([^/?#]+)/);
    if (!handle) return [];
    jsonUrl = `${u.origin}/products/${handle[1]}.json`;
  } catch {
    return [];
  }
  try {
    const res = await fetchWithRetry(jsonUrl);
    if (!res.ok) return [];
    const data = (await res.json()) as { product?: { images?: Array<{ src?: unknown }> } };
    const srcs = (data.product?.images ?? [])
      .map((img) => (typeof img?.src === "string" ? img.src : ""))
      .filter(Boolean);
    return srcs.slice(0, MAX_GALLERY_IMAGES);
  } catch {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Shopify-native ingestion (structured /products.json instead of HTML scraping)
// ---------------------------------------------------------------------------

/** True when this config should ingest products straight from the Shopify
 *  product JSON (full gallery + variant price + compare_at_price) rather than
 *  scraping each HTML product page. */
function isShopifyNativeConfig(config: Config): boolean {
  const d = config.discovery;
  if (d.method !== "api") return false;
  return d.api.shopifyNative === true || d.api.endpoint.endsWith("/products.json");
}

/** Fetch the full Shopify product object for a `/products/<handle>` URL.
 *  Returns null on any failure (caller treats it like a failed page fetch). */
async function fetchShopifyProductJson(url: string): Promise<ShopifyProduct | null> {
  let jsonUrl: string;
  try {
    const u = new URL(url);
    const handle = u.pathname.match(/\/products\/([^/?#]+)/);
    if (!handle) return null;
    jsonUrl = `${u.origin}/products/${handle[1]}.json`;
  } catch {
    return null;
  }
  try {
    const res = await fetchWithRetry(jsonUrl);
    if (!res.ok) return null;
    const data = (await res.json()) as { product?: ShopifyProduct };
    return data.product ?? null;
  } catch {
    return null;
  }
}

/** Build a ClothingItemInput from a Shopify product object, then run the shared
 *  classifier + currency hint (reusing the per-page pipeline's downstream). */
function buildShopifyNativeItem(product: ShopifyProduct, url: string, config: Config): ClothingItemInput {
  let origin: string;
  try {
    origin = new URL(url).origin;
  } catch {
    origin = config.baseUrl;
  }
  const f = mapShopifyProductFields(product, origin);

  // Keep the structured variants/options/tags in metadata but drop the bulky
  // blobs (description + image list already live in their own columns).
  const { body_html: _bodyHtml, images: _images, ...metaRest } = product;

  const item: ClothingItemInput = {
    name: f.name,
    description: f.description,
    brand: f.brand,
    category: f.category,
    subcategory: null,
    price: f.price,
    currency: "",
    salePrice: f.salePrice,
    compareAtPrice: f.compareAtPrice,
    inStock: f.inStock,
    imageUrl: f.images[0] ?? null,
    images: f.images,
    colors: f.colors,
    sizes: f.sizes,
    tags: f.tags,
    gender: null,
    productType: null,
    isClothing: true,
    classificationConfidence: null,
    sourceUrl: f.sourceUrl,
    metadata: metaRest as Record<string, unknown>,
    retailer: config.retailer,
    // externalId = variant SKU || null; the upload loop falls back to the URL
    // slug (handle) when null, matching the per-page path so re-crawls update
    // existing rows in place instead of inserting duplicates.
    // Leave externalId null: the upload loop resolves the key (preserve an
    // existing row's key via sourceUrl, else the URL handle) so re-crawls update
    // in place. Keep the SKU as the manufacturer code.
    externalId: null,
    manufacturerCode: f.externalId,
  };

  // Reuse the shared classifier (gender/productType/isClothing) + category/colors
  // fill. No HTML to scan, so pass "" — sizes/colors are already set from the
  // Shopify options, so inferFromConfig won't overwrite them.
  inferFromConfig(item, url, config, "");
  finalizeCurrencyHint(item, url);
  if (!item.imageUrl && item.images.length > 0) item.imageUrl = item.images[0];
  return item;
}

// ---------------------------------------------------------------------------
// Image upload to R2
// ---------------------------------------------------------------------------

const _imageCache = new Map<string, string>();

// Returns the public R2 URL on success, or `null` when the image can't be
// fetched/stored (after fetchWithRetry's own retries). Callers MUST treat null
// as "this image is unavailable" and drop it — never fall back to the source
// URL, which would hotlink-break in the app and skip background-removal.
async function uploadImageToR2(
  imageUrl: string,
  retailer: string,
  externalId: string,
  index: number,
): Promise<string | null> {
  // Check cache for deduplication
  if (_imageCache.has(imageUrl)) {
    return _imageCache.get(imageUrl)!;
  }

  try {
    // Resolve relative URLs
    let resolvedUrl = imageUrl;
    if (imageUrl.startsWith("//")) resolvedUrl = "https:" + imageUrl;

    const res = await fetchWithRetry(resolvedUrl, IMAGE_TIMEOUT_MS);
    if (!res.ok) {
      log(`    Image fetch failed (HTTP ${res.status}): ${imageUrl}`);
      return null;
    }

    const buffer = Buffer.from(await res.arrayBuffer());
    const contentType = res.headers.get("content-type") ?? guessContentType(imageUrl);
    const ext = extensionFromContentType(contentType);
    const key = `products/${retailer}/${externalId}/${index}.${ext}`;

    await getR2Client().send(
      new PutObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: key,
        Body: buffer,
        ContentType: contentType,
      }),
    );

    const publicUrl = `${process.env.R2_PUBLIC_URL}/${key}`;
    _imageCache.set(imageUrl, publicUrl);
    return publicUrl;
  } catch (err) {
    log(`    Image upload failed: ${(err as Error).message}`);
    return null;
  }
}

function guessContentType(url: string): string {
  const lower = url.toLowerCase();
  if (lower.includes(".png")) return "image/png";
  if (lower.includes(".webp")) return "image/webp";
  if (lower.includes(".gif")) return "image/gif";
  if (lower.includes(".avif")) return "image/avif";
  return "image/jpeg";
}

function extensionFromContentType(ct: string): string {
  if (ct.includes("png")) return "png";
  if (ct.includes("webp")) return "webp";
  if (ct.includes("gif")) return "gif";
  if (ct.includes("avif")) return "avif";
  return "jpg";
}

// ---------------------------------------------------------------------------
// USD normalization (before DB)
// ---------------------------------------------------------------------------

function normalizeItemPriceToUsd(item: ClothingItemInput, sourceUrl: string): void {
  if (item.price == null) return;

  const originalPrice = item.price;
  const rawCurrency = item.currency;

  // Shared normalizer: resolve real currency, strip the home-market VAT/GST
  // baked into EU/UK/etc. consumer prices (a US shopper buying as an export
  // isn't charged it), FX all three price fields with the same rate, then
  // re-assert the sale invariant (compareAtPrice > price else both null).
  const n = normalizePriceTriple(sourceUrl, rawCurrency, originalPrice, item.salePrice, item.compareAtPrice);
  const { iso, notes, vatStripped: vat, unknownCurrency } = n;

  if (iso !== "USD" || notes.length > 0 || vat > 0) {
    const vatNote = vat > 0 ? ` less ${Math.round(vat * 100)}% VAT` : "";
    log(
      `    FX: ${originalPrice} ${iso}${vatNote} → ~$${n.price} USD${notes.length ? ` (${notes.join("; ")})` : ""}`,
    );
  } else if (unknownCurrency) {
    log(`    FX: no rate for "${iso}" — leaving nominal amount as USD`);
  }

  item.price = n.price;
  item.currency = "USD";
  item.salePrice = n.salePrice;
  item.compareAtPrice = n.compareAtPrice;

  const shouldAddMeta = iso !== "USD" || notes.length > 0 || unknownCurrency || vat > 0;
  if (shouldAddMeta) {
    const prev =
      item.metadata && typeof item.metadata === "object" && !Array.isArray(item.metadata)
        ? { ...(item.metadata as Record<string, unknown>) }
        : {};
    const fxParts = [...notes];
    if (vat > 0) fxParts.push(`stripped ${Math.round(vat * 100)}% home VAT before FX`);
    if (unknownCurrency) fxParts.push(`no USD rate for ${iso}; amount stored as nominal USD`);
    const fxNote = fxParts.filter(Boolean).join("; ");
    item.metadata = {
      ...prev,
      originalPrice,
      originalCurrency: iso,
      ...(vat > 0 ? { vatRateStripped: vat } : {}),
      ...(fxNote ? { fxNote } : {}),
    };
  }
}

// ---------------------------------------------------------------------------
// Database upsert
// ---------------------------------------------------------------------------

/** Compare two string arrays element-wise (order-sensitive). */
function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

async function upsertClothingItem(item: ClothingItemInput, pool: Pool): Promise<void> {
  // Detect whether a re-crawl actually changed this item's images. Only then do
  // we reset the downstream processing (nobg/embed/person) — a blanket reset on
  // every weekly sweep would re-process the whole catalog. retailer+externalId is
  // the unique key (externalId is set by the caller before upsert).
  let existingId: string | null = null;
  let imagesChanged = false;
  if (item.externalId != null) {
    const ex = await pool.query<{ id: string; imageUrl: string | null; images: string[] | null }>(
      'SELECT id, "imageUrl", images FROM "ClothingItem" WHERE retailer = $1 AND "externalId" = $2',
      [item.retailer, item.externalId],
    );
    const prev = ex.rows[0];
    if (prev) {
      existingId = prev.id;
      imagesChanged = prev.imageUrl !== item.imageUrl || !arraysEqual(prev.images ?? [], item.images);
    }
  }

  const query = `
    INSERT INTO "ClothingItem" (
      "id", "name", "description", "brand", "category", "subcategory",
      "price", "currency", "imageUrl", "images", "colors", "sizes",
      "tags", "gender", "sourceUrl", "metadata", "retailer",
      "externalId", "manufacturerCode", "productType", "isClothing",
      "classificationConfidence", "classifiedAt", "lastVerifiedAt", "active",
      "createdAt", "updatedAt",
      "salePrice", "compareAtPrice", "inStock"
    ) VALUES (
      gen_random_uuid(), $1, $2, $3, $4, $5,
      $6, $7, $8, $9, $10, $11,
      $12, $13, $14, $15, $16,
      $17, $18, $19, $20,
      $21, NOW(), NOW(), true,
      NOW(), NOW(),
      $22, $23, $25
    )
    ON CONFLICT ("retailer", "externalId") DO UPDATE SET
      "name" = EXCLUDED."name",
      "description" = EXCLUDED."description",
      "brand" = EXCLUDED."brand",
      "category" = EXCLUDED."category",
      "subcategory" = EXCLUDED."subcategory",
      "price" = EXCLUDED."price",
      "salePrice" = EXCLUDED."salePrice",
      "compareAtPrice" = EXCLUDED."compareAtPrice",
      -- Keep the previous stock verdict when this crawl learned nothing
      -- (non-Shopify page without offer availability).
      "inStock" = COALESCE(EXCLUDED."inStock", "ClothingItem"."inStock"),
      "currency" = EXCLUDED."currency",
      "imageUrl" = EXCLUDED."imageUrl",
      "images" = EXCLUDED."images",
      "colors" = EXCLUDED."colors",
      "sizes" = EXCLUDED."sizes",
      "tags" = EXCLUDED."tags",
      "gender" = EXCLUDED."gender",
      "sourceUrl" = EXCLUDED."sourceUrl",
      "metadata" = EXCLUDED."metadata",
      "manufacturerCode" = EXCLUDED."manufacturerCode",
      "productType" = EXCLUDED."productType",
      "isClothing" = EXCLUDED."isClothing",
      "classificationConfidence" = EXCLUDED."classificationConfidence",
      "classifiedAt" = NOW(),
      "lastVerifiedAt" = NOW(),
      "updatedAt" = NOW(),
      -- When a re-crawl changed the images ($24 = imagesChanged), null the
      -- processing flags so the new gallery is re-nobg/re-embedded/re-person-
      -- scanned; unchanged photos keep their flags → zero reprocessing.
      "hasNobg" = CASE WHEN $24::boolean THEN NULL ELSE "ClothingItem"."hasNobg" END,
      "personScannedAt" = CASE WHEN $24::boolean THEN NULL ELSE "ClothingItem"."personScannedAt" END,
      "personScanConfidence" = CASE WHEN $24::boolean THEN NULL ELSE "ClothingItem"."personScanConfidence" END
  `;

  const values = [
    item.name,
    item.description,
    item.brand,
    item.category,
    item.subcategory,
    item.price,
    item.currency,
    item.imageUrl,
    item.images,
    item.colors,
    item.sizes,
    item.tags,
    item.gender,
    item.sourceUrl,
    item.metadata ? JSON.stringify(item.metadata) : null,
    item.retailer,
    item.externalId,
    item.manufacturerCode,
    item.productType,
    item.isClothing,
    item.classificationConfidence,
    item.salePrice,
    item.compareAtPrice,
    imagesChanged,
    item.inStock,
  ];

  await pool.query(query, values);

  // Drop the stale embedding when the images changed so it re-embeds against the
  // new primary (the SET clause already nulled hasNobg to re-trigger nobg→embed).
  // Separate statement: ItemEmbedding is its own table, keyed by itemId.
  if (existingId && imagesChanged) {
    await pool.query('DELETE FROM "ItemEmbedding" WHERE "itemId" = $1', [existingId]);
  }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function validateItem(item: ClothingItemInput): string[] {
  const missing: string[] = [];
  if (!item.name) missing.push("name");
  if (!item.brand) missing.push("brand");
  if (!item.category) missing.push("category");
  if (item.price == null) missing.push("price");
  if (!item.imageUrl && item.images.length === 0) missing.push("image");
  return missing;
}

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------

async function createUploadStagehand(): Promise<Stagehand> {
  const sh = new Stagehand({
    ...getStagehandBrowserOptions(),
    model: getStagehandModel(),
    localBrowserLaunchOptions: { headless: true },
  });
  await sh.init();
  return sh;
}

async function closeUploadStagehand(sh: Stagehand | null): Promise<void> {
  if (!sh) return;
  try {
    await sh.close();
  } catch {
    // ignore
  }
}

export async function uploadRetailer(
  config: Config,
  urls: string[],
  onLog?: (msg: string) => void,
  onProgress?: (progress: UploadProgress) => void,
  options?: UploadRunOptions,
): Promise<UploadResult> {
  if (onLog) _logFn = onLog;

  // Clear image cache for this run
  _imageCache.clear();

  const delayMs = Math.max(config.requestConfig.delayBetweenRequestsMs, 1000);
  const needsBrowser = config.requestConfig.requiresJsRendering;

  log(`\nStarting product upload for ${config.retailerDisplayName}`);
  log(`  URLs to process: ${urls.length}`);
  log(`  Delay between requests: ${delayMs}ms`);
  log(`  Requires browser: ${needsBrowser}`);

  const dbUrl = (process.env.DATABASE_URL ?? "").replace(/^["']+|["']+$/g, "");
  const pool = new Pool({
    connectionString: dbUrl,
    max: 3,
    idleTimeoutMillis: 30_000,
  });

  let stagehand: Stagehand | null = null;

  const progress: UploadProgress = {
    uploaded: 0,
    skipped: 0,
    failed: 0,
    total: urls.length,
    currentUrl: "",
  };

  try {
    // Test DB connection
    await pool.query("SELECT 1");
    log("  Database connection OK");

    // Freshen FX rates (KV-cached daily; falls back to the hardcoded table).
    await ensureFreshFxRates();

    // Initialize browser if needed
    if (needsBrowser) {
      log("  Initializing headless browser...");
      stagehand = await createUploadStagehand();
      log("  Browser ready");
    }

    let consecutiveFailures = 0;
    let successesSinceBrowserRefresh = 0;
    const resumeUploaded = new Set(options?.resumeUploadedUrls ?? []);
    const shopifyNative = isShopifyNativeConfig(config);
    if (shopifyNative) log("  Mode: Shopify-native ingestion (/products/<handle>.json)");

    for (let i = 0; i < urls.length; i++) {
      const url = urls[i];
      progress.currentUrl = url;

      if (i > 0) await delay(delayMs);

      log(`\n  [${i + 1}/${urls.length}] ${url}`);

      if (resumeUploaded.has(url)) {
        log("    SKIPPED: already uploaded successfully for this crawl checkpoint");
        progress.skipped++;
        onProgress?.(progress);
        if (options?.onItemResult) {
          await options.onItemResult({
            url,
            status: "skipped",
            metadata: { reason: "already_uploaded_for_crawl_checkpoint" },
          });
        }
        continue;
      }

      for (let attempt = 0; attempt < 2; attempt++) {
        let externalId: string | undefined;
        let itemName: string | undefined;
        let imageCount = 0;
        let uploadedToR2 = false;
        let upsertedToDb = false;
        try {
          // Build the product item: Shopify-native (structured /products.json)
          // when the config supports it, else fetch the HTML page + 4-layer extract.
          let item: ClothingItemInput;
          if (shopifyNative) {
            const product = await fetchShopifyProductJson(url);
            if (!product) {
              log(`    Shopify product JSON unavailable — skipping`);
              progress.failed++;
              consecutiveFailures++;
              onProgress?.(progress);
              if (options?.onItemResult) {
                await options.onItemResult({
                  url,
                  status: "failed",
                  error: "shopify_product_json_unavailable",
                  uploadedToR2,
                  upsertedToDb,
                  metadata: {},
                });
              }
              break;
            }
            item = buildShopifyNativeItem(product, url, config);
          } else {
            // Fetch page HTML
            let html: string;
            if (stagehand) {
              const page = stagehand.context.pages()[0];
              await page.goto(url, {
                waitUntil: "domcontentloaded",
                timeoutMs: UPLOAD_NAV_TIMEOUT_MS,
              });
              html = await page.evaluate(() => document.documentElement.outerHTML);
            } else {
              const res = await fetchWithRetry(url);
              if (!res.ok) {
                log(`    HTTP ${res.status} — skipping`);
                progress.failed++;
                consecutiveFailures++;
                onProgress?.(progress);
                if (options?.onItemResult) {
                  await options.onItemResult({
                    url,
                    status: "failed",
                    error: `HTTP ${res.status}`,
                    uploadedToR2,
                    upsertedToDb,
                    metadata: { httpStatus: res.status },
                  });
                }
                break;
              }
              html = await res.text();
            }

            // Extract product data (all 4 layers)
            item = extractProductData(html, url, config);

            // Gallery rescue: Shopify product pages only expose the featured image
            // via JSON-LD/OG, so we under-capture to a single (often on-model) photo.
            // Pull the full gallery from the product JSON so each product keeps its
            // laydown/product-only shots — letting the people-photo scan STRIP model
            // images instead of hiding the whole product.
            if (item.images.length < 2 && looksShopify(html)) {
              const gallery = await fetchShopifyGalleryImages(url);
              if (gallery.length > item.images.length) {
                log(`    [gallery] Shopify .json recovered ${gallery.length} images (was ${item.images.length})`);
                item.images = gallery;
                item.imageUrl = gallery[0];
              }
            }
          }

          // Currency upgrade: when no currency was found in the markup (we only
          // guessed from the hostname — wrong for GBP/EUR stores on .com hosts,
          // e.g. Drake's), ask the store's own /cart.json. Shopify answers with
          // its checkout currency; non-Shopify sites 404 and we keep the guess.
          if (item.currencyInferred && item.price != null) {
            const storeCurrency = await getShopifyStoreCurrency(url);
            if (storeCurrency && storeCurrency !== item.currency) {
              log(`    Currency: ${item.currency} (hostname guess) → ${storeCurrency} (store cart.json)`);
              item.currency = storeCurrency;
              item.currencyInferred = false;
            }
          }

          // Brand identity: keep the scraped brand when it's a real brand name (it is
          // for single brands AND for multi-brand stockists), but replace it with the
          // crawl target's discovered name when it's a distributor/operating company,
          // a gender/category word, or a region/collection variant (Danton →
          // "Bshop Co.,Ltd", Mads Nørgaard → "Women", Drake's → "Drakes - Archive").
          // The raw scraped value remains in metadata for traceability. Per-retailer
          // unify/clean-name overrides (e.g. J.Press) live in brandName.ts.
          item.brand = resolveBrand(item.brand, config.retailer, config.retailerDisplayName) ?? item.brand;

          // Validate required fields
          const missingFields = validateItem(item);
          if (missingFields.length > 0) {
            log(`    SKIPPED: missing required fields: ${missingFields.join(", ")}`);
            progress.skipped++;
            consecutiveFailures = 0;
            onProgress?.(progress);
            if (options?.onItemResult) {
              await options.onItemResult({
                url,
                status: "skipped",
                itemName: item.name ?? undefined,
                externalId: item.externalId ?? undefined,
                uploadedToR2,
                upsertedToDb,
                metadata: { reason: "missing_required_fields", missingFields },
              });
            }
            break;
          }

          // Resolve the upsert/R2 key. For Shopify-native, preserve any existing
          // row's externalId (an earlier per-page crawl may have keyed it on a
          // SKU) by matching on sourceUrl (== url), so the re-crawl updates in
          // place and reuses the existing R2 keys instead of inserting a duplicate.
          // New items fall back to the URL slug (handle).
          if (shopifyNative) {
            // Match the existing row by its product handle (robust to locale
            // prefixes / query strings in either URL) so we reuse its key.
            const ex = await pool.query<{ externalId: string }>(
              `SELECT "externalId" FROM "ClothingItem"
               WHERE retailer = $1
                 AND substring("sourceUrl" from '/products/([^/?#]+)') = $2
                 AND "externalId" IS NOT NULL
               LIMIT 1`,
              [config.retailer, extractSlugFromUrl(url)],
            );
            externalId = ex.rows[0]?.externalId ?? item.externalId ?? extractSlugFromUrl(url);
          } else {
            externalId = item.externalId ?? extractSlugFromUrl(url);
          }

          // Upload images to R2
          let r2Images: string[] = [];
          const allImages = item.images.length > 0 ? item.images : (item.imageUrl ? [item.imageUrl] : []);
          itemName = item.name ?? undefined;
          imageCount = allImages.length;

          // Fingerprint skip: when this row was already uploaded from EXACTLY the
          // same source image URLs (recorded as metadata.sourceImages on every
          // upsert), reuse its existing R2 URLs and move zero image bytes. This is
          // what makes routine re-crawls price/metadata-cheap — images transfer
          // only when the retailer actually changed the gallery.
          let imagesReused = false;
          const prevRow = await pool.query<{
            imageUrl: string | null;
            images: string[] | null;
            sourceImages: unknown;
          }>(
            'SELECT "imageUrl", images, metadata->\'sourceImages\' AS "sourceImages" FROM "ClothingItem" WHERE retailer = $1 AND "externalId" = $2',
            [config.retailer, externalId],
          );
          const prev = prevRow.rows[0];
          const prevSourceImages = Array.isArray(prev?.sourceImages)
            ? (prev.sourceImages as unknown[]).filter((s): s is string => typeof s === "string")
            : null;
          if (
            prev &&
            prevSourceImages &&
            prevSourceImages.length > 0 &&
            (prev.images?.length ?? 0) > 0 &&
            arraysEqual(prevSourceImages, allImages)
          ) {
            r2Images = prev.images!;
            imagesReused = true;
            log(`    Images unchanged (${allImages.length} source URLs match) — reusing existing R2 gallery`);
          } else {
            // Upload a product's images to R2 concurrently — they're distinct CDN
            // assets (not storefront hits), so parallelizing is polite and ~Nx
            // faster than the old sequential loop. Order preserved via the index.
            const eid = externalId;
            const r2Results = await Promise.all(
              allImages.map((img, imgIdx) => uploadImageToR2(img, config.retailer, eid, imgIdx)),
            );
            for (const r2Url of r2Results) {
              if (r2Url) r2Images.push(r2Url);
            }
          }
          uploadedToR2 = r2Images.length > 0;

          // Every image failed to upload to R2 (after fetchWithRetry's own retries).
          // Don't persist a row pointing at un-uploaded source URLs: they hotlink-break
          // in the app, never get background-removal (so they can't reach the feed), and
          // would be silently mislabeled as a successful upload. Skip the product the same
          // way we skip missing required fields — a later crawl re-attempts the images.
          if (r2Images.length === 0) {
            log(`    SKIPPED: all ${allImages.length} image(s) failed to upload to R2`);
            progress.skipped++;
            consecutiveFailures = 0;
            onProgress?.(progress);
            if (options?.onItemResult) {
              await options.onItemResult({
                url,
                status: "skipped",
                itemName: item.name ?? undefined,
                externalId,
                imageCount: allImages.length,
                uploadedToR2: false,
                upsertedToDb: false,
                metadata: { reason: "images_upload_failed", attemptedImages: allImages.length },
              });
            }
            break;
          }

          item.imageUrl = r2Images[0];
          item.images = r2Images;

          // Record the source-gallery fingerprint for the next crawl's skip check
          // (and for the price-refresh sweep's gallery-change detection).
          item.metadata = {
            ...(item.metadata && typeof item.metadata === "object" && !Array.isArray(item.metadata)
              ? (item.metadata as Record<string, unknown>)
              : {}),
            sourceImages: allImages,
          };

          // Ensure externalId is set for upsert
          item.externalId = externalId;

          normalizeItemPriceToUsd(item, url);

          // Upsert into database
          await upsertClothingItem(item, pool);
          upsertedToDb = true;

          log(`    OK: "${item.name}" | ${item.category}/${item.subcategory ?? "?"} | $${item.price} | ${r2Images.length} images`);
          progress.uploaded++;
          consecutiveFailures = 0;
          successesSinceBrowserRefresh++;

          if (
            needsBrowser &&
            stagehand &&
            UPLOAD_BROWSER_REFRESH_EVERY_N > 0 &&
            successesSinceBrowserRefresh >= UPLOAD_BROWSER_REFRESH_EVERY_N
          ) {
            log(
              `    Browser session recycled (reason: proactive after ${successesSinceBrowserRefresh} successful uploads)`,
            );
            await closeUploadStagehand(stagehand);
            stagehand = await createUploadStagehand();
            successesSinceBrowserRefresh = 0;
          }

          onProgress?.(progress);
          if (options?.onItemResult) {
            await options.onItemResult({
              url,
              status: "uploaded",
              itemName: item.name ?? undefined,
              externalId,
              imageCount: r2Images.length,
              uploadedToR2,
              upsertedToDb,
              metadata: {
                category: item.category,
                subcategory: item.subcategory,
                price: item.price,
                imagesReused,
              },
            });
          }
          break;
        } catch (err) {
          const e = err instanceof Error ? err : new Error(String(err));
          log(`    FAILED: ${e.message}`);
          if (e.stack) log(`    ${e.stack.split("\n").slice(0, 4).join("\n    ")}`);
          consecutiveFailures++;

          if (
            needsBrowser &&
            stagehand &&
            UPLOAD_BROWSER_REFRESH_AFTER_CONSECUTIVE_FAILURES > 0 &&
            consecutiveFailures >= UPLOAD_BROWSER_REFRESH_AFTER_CONSECUTIVE_FAILURES &&
            attempt === 0
          ) {
            log(
              `    Browser session recycled (reason: consecutive_failures=${consecutiveFailures}) — retrying URL once`,
            );
            await closeUploadStagehand(stagehand);
            stagehand = await createUploadStagehand();
            consecutiveFailures = 0;
            continue;
          }

          progress.failed++;
          onProgress?.(progress);
          if (options?.onItemResult) {
            await options.onItemResult({
              url,
              status: "failed",
              error: e.message,
              externalId,
              itemName,
              imageCount,
              uploadedToR2,
              upsertedToDb,
            });
          }
          break;
        }
      }
    }
  } finally {
    await pool.end();
    if (stagehand) await stagehand.close();
    _logFn = console.log;
  }

  const result: UploadResult = {
    retailer: config.retailer,
    uploadedAt: new Date().toISOString(),
    uploaded: progress.uploaded,
    skipped: progress.skipped,
    failed: progress.failed,
    total: progress.total,
  };

  log(`\n  Upload complete: ${result.uploaded} uploaded, ${result.skipped} skipped, ${result.failed} failed (${result.total} total)`);

  return result;
}
