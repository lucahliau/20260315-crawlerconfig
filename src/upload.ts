import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Pool } from "pg";
import { Stagehand } from "@browserbasehq/stagehand";
import type { Config } from "./schemas/config.js";
import {
  convertToUsd,
  inferCurrencyFromHostname,
  resolveCurrencyForUsd,
  roundUsdPrice,
} from "./currencyToUsd.js";

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
  imageUrl: string | null;
  images: string[];
  colors: string[];
  sizes: string[];
  tags: string[];
  gender: string | null;
  sourceUrl: string;
  metadata: Record<string, unknown> | null;
  retailer: string;
  externalId: string | null;
  manufacturerCode: string | null;
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
        const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1);
        log(`    HTTP ${res.status} on attempt ${attempt}/${MAX_RETRIES}, retrying in ${backoff}ms...`);
        await delay(backoff);
        continue;
      }
      return res;
    } catch (err) {
      clearTimeout(timeout);
      if (attempt === MAX_RETRIES) throw err;
      const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1);
      log(`    Fetch error attempt ${attempt}/${MAX_RETRIES}: ${(err as Error).message}, retrying in ${backoff}ms...`);
      await delay(backoff);
    }
  }
  throw new Error(`Failed to fetch ${url} after ${MAX_RETRIES} attempts`);
}

// ---------------------------------------------------------------------------
// HTML parsing helpers (regex-based, no DOM dependency)
// ---------------------------------------------------------------------------

function extractJsonLdBlocks(html: string): Record<string, unknown>[] {
  const blocks: Record<string, unknown>[] = [];
  const re = /<script[^>]*type\s*=\s*["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match: RegExpExecArray | null;
  while ((match = re.exec(html)) !== null) {
    try {
      const parsed = JSON.parse(match[1].trim());
      blocks.push(parsed);
    } catch { /* malformed JSON-LD */ }
  }
  return blocks;
}

function findProductInJsonLd(blocks: Record<string, unknown>[]): Record<string, unknown> | null {
  for (const block of blocks) {
    // Direct Product object
    const type = String(block["@type"] ?? "").toLowerCase();
    if (type.includes("product")) return block;

    // @graph array
    if (Array.isArray(block["@graph"])) {
      for (const item of block["@graph"] as Record<string, unknown>[]) {
        const itemType = String(item["@type"] ?? "").toLowerCase();
        if (itemType.includes("product")) return item;
        // Handle array types like ["Product", "ItemPage"]
        if (Array.isArray(item["@type"])) {
          const types = (item["@type"] as string[]).map((t) => t.toLowerCase());
          if (types.some((t) => t.includes("product"))) return item;
        }
      }
    }
  }
  return null;
}

function extractMetaContent(html: string, property: string): string | null {
  // Match both property="..." and name="..."
  const re = new RegExp(
    `<meta[^>]*(?:property|name)\\s*=\\s*["']${escapeRegex(property)}["'][^>]*content\\s*=\\s*["']([^"']*)["']` +
    `|<meta[^>]*content\\s*=\\s*["']([^"']*)["'][^>]*(?:property|name)\\s*=\\s*["']${escapeRegex(property)}["']`,
    "i",
  );
  const m = html.match(re);
  if (m) return m[1] ?? m[2] ?? null;
  return null;
}

function extractTitle(html: string): string | null {
  const m = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m ? m[1].trim() : null;
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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

function normalizeOffers(offers: unknown): Record<string, unknown>[] {
  if (!offers) return [];
  if (Array.isArray(offers)) return offers as Record<string, unknown>[];
  if (typeof offers === "object" && offers !== null) {
    const obj = offers as Record<string, unknown>;
    // AggregateOffer with offers array inside
    if (Array.isArray(obj.offers)) return obj.offers as Record<string, unknown>[];
    return [obj];
  }
  return [];
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
  // Category / subcategory inference
  if (!partial.category) {
    // Try breadcrumbs from JSON-LD BreadcrumbList
    const breadcrumbText = extractBreadcrumbText(html);
    const inferred = inferCategory(breadcrumbText + " " + (partial.name ?? ""));
    if (inferred) {
      partial.category = inferred.category;
      if (!partial.subcategory) partial.subcategory = inferred.subcategory;
    }
  }

  // Gender inference
  if (!partial.gender) {
    partial.gender = inferGender(url, partial.name ?? "", config, html);
  }

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

function inferGender(url: string, name: string, config: Config, _html: string): string | null {
  // Check config patterns
  if (config.productPage.genderInUrl?.detected) {
    const patterns = config.productPage.genderInUrl.patterns;
    const lowerUrl = url.toLowerCase();
    for (const [gender, pattern] of Object.entries(patterns)) {
      if (lowerUrl.includes(pattern.toLowerCase())) {
        return gender === "male" ? "men" : gender === "female" ? "women" : gender;
      }
    }
  }

  // Check URL and name for gender keywords
  const combined = (url + " " + name).toLowerCase();
  if (/\bwomens?\b|\bfemale\b|\bladies\b/.test(combined)) return "women";
  if (/\bmens?\b|\bmale\b/.test(combined)) return "men";
  if (/\bkids?\b|\bchildren\b|\bboys?\b|\bgirls?\b/.test(combined)) return "kids";
  if (/\bunisex\b/.test(combined)) return "unisex";

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
    imageUrl: null,
    images: [],
    colors: [],
    sizes: [],
    tags: [],
    gender: null,
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
// Image upload to R2
// ---------------------------------------------------------------------------

const _imageCache = new Map<string, string>();

async function uploadImageToR2(
  imageUrl: string,
  retailer: string,
  externalId: string,
  index: number,
): Promise<string> {
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
      return imageUrl;
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
    return imageUrl;
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
  const { iso, notes } = resolveCurrencyForUsd(sourceUrl, originalPrice, rawCurrency);
  const { usd, unknownCurrency } = convertToUsd(originalPrice, iso);
  const rounded = roundUsdPrice(usd);

  if (iso !== "USD" || notes.length > 0) {
    log(
      `    FX: ${originalPrice} ${iso} → ~$${rounded} USD${notes.length ? ` (${notes.join("; ")})` : ""}`,
    );
  } else if (unknownCurrency) {
    log(`    FX: no rate for "${iso}" — leaving nominal amount as USD`);
  }

  item.price = rounded;
  item.currency = "USD";

  const shouldAddMeta = iso !== "USD" || notes.length > 0 || unknownCurrency;
  if (shouldAddMeta) {
    const prev =
      item.metadata && typeof item.metadata === "object" && !Array.isArray(item.metadata)
        ? { ...(item.metadata as Record<string, unknown>) }
        : {};
    const fxParts = [...notes];
    if (unknownCurrency) fxParts.push(`no USD rate for ${iso}; amount stored as nominal USD`);
    const fxNote = fxParts.filter(Boolean).join("; ");
    item.metadata = {
      ...prev,
      originalPrice,
      originalCurrency: iso,
      ...(fxNote ? { fxNote } : {}),
    };
  }
}

// ---------------------------------------------------------------------------
// Database upsert
// ---------------------------------------------------------------------------

async function upsertClothingItem(item: ClothingItemInput, pool: Pool): Promise<void> {
  const query = `
    INSERT INTO "ClothingItem" (
      "id", "name", "description", "brand", "category", "subcategory",
      "price", "currency", "imageUrl", "images", "colors", "sizes",
      "tags", "gender", "sourceUrl", "metadata", "retailer",
      "externalId", "manufacturerCode", "lastVerifiedAt", "active",
      "createdAt", "updatedAt"
    ) VALUES (
      gen_random_uuid(), $1, $2, $3, $4, $5,
      $6, $7, $8, $9, $10, $11,
      $12, $13, $14, $15, $16,
      $17, $18, NOW(), true,
      NOW(), NOW()
    )
    ON CONFLICT ("retailer", "externalId") DO UPDATE SET
      "name" = EXCLUDED."name",
      "description" = EXCLUDED."description",
      "brand" = EXCLUDED."brand",
      "category" = EXCLUDED."category",
      "subcategory" = EXCLUDED."subcategory",
      "price" = EXCLUDED."price",
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
      "lastVerifiedAt" = NOW(),
      "updatedAt" = NOW()
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
  ];

  await pool.query(query, values);
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
    env: (process.env.STAGEHAND_ENV as "LOCAL" | "BROWSERBASE") || "LOCAL",
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

    // Initialize browser if needed
    if (needsBrowser) {
      log("  Initializing headless browser...");
      stagehand = await createUploadStagehand();
      log("  Browser ready");
    }

    let consecutiveFailures = 0;
    let successesSinceBrowserRefresh = 0;

    for (let i = 0; i < urls.length; i++) {
      const url = urls[i];
      progress.currentUrl = url;

      if (i > 0) await delay(delayMs);

      log(`\n  [${i + 1}/${urls.length}] ${url}`);

      for (let attempt = 0; attempt < 2; attempt++) {
        try {
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
              break;
            }
            html = await res.text();
          }

          // Extract product data (all 4 layers)
          const item = extractProductData(html, url, config);

          // Validate required fields
          const missingFields = validateItem(item);
          if (missingFields.length > 0) {
            log(`    SKIPPED: missing required fields: ${missingFields.join(", ")}`);
            progress.skipped++;
            consecutiveFailures = 0;
            onProgress?.(progress);
            break;
          }

          // Upload images to R2
          const externalId = item.externalId ?? extractSlugFromUrl(url);
          const r2Images: string[] = [];
          const allImages = item.images.length > 0 ? item.images : (item.imageUrl ? [item.imageUrl] : []);

          for (let imgIdx = 0; imgIdx < allImages.length; imgIdx++) {
            const r2Url = await uploadImageToR2(allImages[imgIdx], config.retailer, externalId, imgIdx);
            r2Images.push(r2Url);
          }

          if (r2Images.length > 0) {
            item.imageUrl = r2Images[0];
            item.images = r2Images;
          }

          // Ensure externalId is set for upsert
          item.externalId = externalId;

          normalizeItemPriceToUsd(item, url);

          // Upsert into database
          await upsertClothingItem(item, pool);

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
