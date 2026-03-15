import { Stagehand } from "@browserbasehq/stagehand";
import { z } from "zod";
import fs from "node:fs";
import path from "node:path";

// ---------------------------------------------------------------------------
// Logging — swappable sink so the server can capture output via SSE
// ---------------------------------------------------------------------------

let _logFn: (msg: string) => void = console.log;

function log(...args: unknown[]): void {
  _logFn(args.map(String).join(" "));
}

function setLogFn(fn: (msg: string) => void): void {
  _logFn = fn;
}

function resetLogFn(): void {
  _logFn = console.log;
}

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

function parseArgs(): { url: string } {
  const args = process.argv.slice(2);
  const urlIndex = args.indexOf("--url");

  if (urlIndex === -1 || urlIndex + 1 >= args.length) {
    console.error("Usage: npm start -- --url <retailer-url>");
    console.error("Example: npm start -- --url https://www.asos.com");
    process.exit(1);
  }

  const url = args[urlIndex + 1];

  try {
    new URL(url);
  } catch {
    console.error(`Invalid URL: ${url}`);
    process.exit(1);
  }

  return { url };
}

function extractRetailerSlug(url: string): string {
  const hostname = new URL(url).hostname;
  return hostname.replace(/^www\./, "").split(".")[0];
}

function extractDisplayName(url: string): string {
  const slug = extractRetailerSlug(url);
  return slug.charAt(0).toUpperCase() + slug.slice(1);
}

// ---------------------------------------------------------------------------
// Retry helper — rate-limit aware with exponential backoff
// ---------------------------------------------------------------------------

async function withRetry<T>(
  fn: () => Promise<T>,
  retries = 2,
  delayMs = 3000,
): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    if (retries <= 0) throw err;

    const msg = (err as Error).message ?? String(err);
    const isRateLimit = /rate.?limit/i.test(msg);

    // If rate-limited, wait 60s instead of the default delay
    const actualDelay = isRateLimit ? Math.max(delayMs, 60_000) : delayMs;

    log(`  ${isRateLimit ? "Rate limited" : "Error"} — retrying after ${actualDelay / 1000}s...`);
    await new Promise((r) => setTimeout(r, actualDelay));

    // Exponential backoff: double the delay for the next retry
    return withRetry(fn, retries - 1, actualDelay * 2);
  }
}

// ---------------------------------------------------------------------------
// Cookie / modal dismissal — tracked so we only pay for one LLM call
// ---------------------------------------------------------------------------

const _overlaysDismissed = new Set<string>();

async function dismissOverlays(
  stagehand: Stagehand,
  page: StagehandPage,
): Promise<void> {
  // Derive a domain key so we only dismiss once per domain per session
  let domain: string;
  try {
    domain = new URL(page.url()).hostname;
  } catch {
    domain = "__unknown__";
  }

  if (_overlaysDismissed.has(domain)) return;

  // First try a quick DOM-based dismissal (zero LLM tokens)
  const dismissed = await page.evaluate(() => {
    // Common cookie / overlay accept button selectors
    const selectors = [
      '[id*="cookie"] button[class*="accept"]',
      '[id*="cookie"] button[class*="agree"]',
      '[class*="cookie"] button[class*="accept"]',
      '[class*="cookie"] button[class*="agree"]',
      'button[id*="accept-cookie"]',
      'button[id*="acceptCookie"]',
      'button[data-testid*="cookie-accept"]',
      'button[data-testid*="accept-cookies"]',
      '[class*="consent"] button:first-of-type',
      '[id*="consent"] button[class*="accept"]',
      '[class*="overlay"] button[class*="close"]',
      '[class*="modal"] button[class*="close"]',
      '[aria-label="Close"]',
      'button[class*="dismiss"]',
    ];
    let found = false;
    for (const sel of selectors) {
      const btn = document.querySelector<HTMLElement>(sel);
      if (btn && btn.offsetParent !== null) {
        btn.click();
        found = true;
      }
    }
    return found;
  });

  if (dismissed) {
    log("  Dismissed overlays via DOM selectors.");
    _overlaysDismissed.add(domain);
    // Small wait for animations
    await page.waitForTimeout(500);
    return;
  }

  // Fallback: use Stagehand LLM (costs tokens, but only once per domain)
  try {
    await stagehand.act(
      "If there is a cookie consent banner, popup overlay, or country/region selector visible, accept cookies and dismiss it. If nothing is visible, do nothing.",
    );
    _overlaysDismissed.add(domain);
  } catch {
    // Non-fatal — mark as done so we don't retry
    _overlaysDismissed.add(domain);
  }
}

// ---------------------------------------------------------------------------
// Partial config output
// ---------------------------------------------------------------------------

function writePartialConfig(
  configsDir: string,
  retailer: string,
  partialData: Record<string, unknown>,
): void {
  const outPath = path.join(configsDir, `${retailer}.json`);
  fs.writeFileSync(outPath, JSON.stringify(partialData, null, 2) + "\n");
  log(`Config written to: ${outPath}`);
}

// ---------------------------------------------------------------------------
// Pure-TypeScript URL pattern derivation (replaces LLM call)
// ---------------------------------------------------------------------------

function deriveProductUrlPattern(urls: string[]): {
  pattern: string;
  matching: string[];
  nonMatching: string[];
} {
  // Common product URL indicators
  const productIndicators = [
    /\/p\/[^/]+/,            // /p/product-slug
    /\/product[s]?\/[^/]+/,  // /product/slug or /products/slug
    /\/item[s]?\/[^/]+/,     // /item/slug
    /\/dp\/[A-Z0-9]+/,       // Amazon-style /dp/ASIN
    /\/[^/]+-\d{5,}/,        // slug-with-numeric-id
    /\/\d{5,}/,              // bare numeric ID
    /\/prd\//,               // /prd/ prefix
    /\/pd\//,                // /pd/ prefix
    /\/[^/]+\.html$/,        // slug.html
  ];

  // Try to find a common path structure among the URLs
  const parsed = urls.map((u) => {
    try {
      return new URL(u);
    } catch {
      return null;
    }
  }).filter(Boolean) as URL[];

  if (parsed.length === 0) {
    return { pattern: ".*", matching: urls, nonMatching: [] };
  }

  // Check known product indicators first
  for (const indicator of productIndicators) {
    const matches = urls.filter((u) => indicator.test(u));
    if (matches.length >= Math.ceil(urls.length * 0.5)) {
      return {
        pattern: indicator.source,
        matching: matches,
        nonMatching: urls.filter((u) => !indicator.test(u)),
      };
    }
  }

  // Fallback: find the longest common path prefix, then build a pattern
  const pathSegments = parsed.map((u) =>
    u.pathname.split("/").filter(Boolean),
  );

  // Find common prefix segments
  const minLen = Math.min(...pathSegments.map((s) => s.length));
  let commonPrefixLen = 0;
  for (let i = 0; i < minLen; i++) {
    const seg = pathSegments[0][i];
    if (pathSegments.every((s) => s[i] === seg)) {
      commonPrefixLen = i + 1;
    } else {
      break;
    }
  }

  if (commonPrefixLen > 0) {
    const prefix = "/" + pathSegments[0].slice(0, commonPrefixLen).join("/");
    const pattern = prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "/[^/]+";
    const re = new RegExp(pattern);
    return {
      pattern,
      matching: urls.filter((u) => re.test(u)),
      nonMatching: urls.filter((u) => !re.test(u)),
    };
  }

  // Last resort: match any URL with 3+ path segments (likely a product page)
  const depthPattern = "^https?://[^/]+/[^/]+/[^/]+/[^/]+";
  const re = new RegExp(depthPattern);
  return {
    pattern: depthPattern,
    matching: urls.filter((u) => re.test(u)),
    nonMatching: urls.filter((u) => !re.test(u)),
  };
}

// ---------------------------------------------------------------------------
// DOM-based extraction helpers (zero LLM tokens)
// ---------------------------------------------------------------------------

interface JsonLdData {
  found: boolean;
  fieldsPresent: string[];
  rawJsonLd: Record<string, unknown> | null;
  notes: string;
}

async function extractJsonLdFromPage(page: StagehandPage): Promise<JsonLdData> {
  return page.evaluate(() => {
    const scripts = document.querySelectorAll(
      'script[type="application/ld+json"]',
    );
    for (const script of scripts) {
      try {
        const data = JSON.parse(script.textContent ?? "");
        // Handle @graph arrays
        const items: Record<string, unknown>[] = Array.isArray(data["@graph"])
          ? data["@graph"]
          : [data];

        for (const item of items) {
          const type = (item["@type"] ?? "").toString().toLowerCase();
          if (type === "product" || type.includes("product")) {
            const fields: string[] = [];
            if (item.name) fields.push("name");
            if (item.brand) fields.push("brand");
            if (item.description) fields.push("description");
            if (item.offers || item.price) fields.push("price");
            if (item.image || item.images) fields.push("images");
            if (item.sku) fields.push("sku");
            if (item.mpn) fields.push("mpn");
            if (item.color) fields.push("color");
            if (item.category) fields.push("category");
            if (item.gtin || item.gtin13 || item.gtin12) fields.push("gtin");

            const notes: string[] = [];
            if (item.offers && typeof item.offers === "object") {
              const offers = Array.isArray(item.offers) ? item.offers : [item.offers];
              if (offers.length > 1) notes.push(`${offers.length} offer variants`);
            }
            if (item.image && Array.isArray(item.image)) {
              notes.push(`${item.image.length} images in array`);
            }

            return {
              found: true,
              fieldsPresent: fields,
              rawJsonLd: item as Record<string, unknown>,
              notes: notes.join(". ") || "Standard schema.org Product.",
            };
          }
        }
      } catch {
        // malformed JSON-LD, skip
      }
    }
    return { found: false, fieldsPresent: [], rawJsonLd: null, notes: "No JSON-LD Product data found." };
  });
}

interface OgData {
  found: boolean;
  tagsPresent: string[];
}

async function extractOgTagsFromPage(page: StagehandPage): Promise<OgData> {
  return page.evaluate(() => {
    const metas = document.querySelectorAll('meta[property^="og:"]');
    const tags: string[] = [];
    for (const meta of metas) {
      const prop = meta.getAttribute("property");
      if (prop) tags.push(prop);
    }
    return { found: tags.length > 0, tagsPresent: tags };
  });
}

interface BreadcrumbData {
  hasBreadcrumbs: boolean;
  selector: string | null;
}

async function extractBreadcrumbsFromPage(
  page: StagehandPage,
): Promise<BreadcrumbData> {
  return page.evaluate(() => {
    // Check common breadcrumb patterns
    const selectors = [
      'nav[aria-label*="breadcrumb"] a',
      'nav[aria-label*="Breadcrumb"] a',
      '[class*="breadcrumb"] a',
      '[data-testid*="breadcrumb"] a',
      'ol[class*="breadcrumb"] a',
      '.breadcrumbs a',
      '#breadcrumbs a',
    ];
    for (const sel of selectors) {
      const els = document.querySelectorAll(sel);
      if (els.length >= 2) {
        return { hasBreadcrumbs: true, selector: sel };
      }
    }
    // Check for BreadcrumbList JSON-LD
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const script of scripts) {
      try {
        const data = JSON.parse(script.textContent ?? "");
        const items = Array.isArray(data["@graph"]) ? data["@graph"] : [data];
        for (const item of items) {
          if ((item["@type"] ?? "").toString().includes("BreadcrumbList")) {
            return { hasBreadcrumbs: true, selector: null };
          }
        }
      } catch { /* skip */ }
    }
    return { hasBreadcrumbs: false, selector: null };
  });
}

async function isProductPage(page: StagehandPage): Promise<{
  isProduct: boolean;
  evidence: string;
}> {
  return page.evaluate(() => {
    const evidence: string[] = [];

    // Check JSON-LD
    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const script of scripts) {
      try {
        const data = JSON.parse(script.textContent ?? "");
        const items = Array.isArray(data["@graph"]) ? data["@graph"] : [data];
        for (const item of items) {
          if ((item["@type"] ?? "").toString().toLowerCase().includes("product")) {
            evidence.push("JSON-LD Product found");
          }
        }
      } catch { /* skip */ }
    }

    // Check OG type
    const ogType = document.querySelector('meta[property="og:type"]');
    if (ogType?.getAttribute("content")?.includes("product")) {
      evidence.push("og:type=product");
    }

    // Check for price elements
    const priceSelectors = [
      '[class*="price"]',
      '[data-testid*="price"]',
      '[itemprop="price"]',
      '[class*="Price"]',
    ];
    for (const sel of priceSelectors) {
      if (document.querySelector(sel)) {
        evidence.push(`Price element: ${sel}`);
        break;
      }
    }

    // Check for add-to-cart / buy button
    const cartSelectors = [
      'button[class*="add-to-cart"]',
      'button[class*="addToCart"]',
      'button[class*="add-to-bag"]',
      'button[class*="addToBag"]',
      'button[data-testid*="add-to"]',
      '[id*="add-to-cart"]',
      '[id*="addToCart"]',
    ];
    for (const sel of cartSelectors) {
      if (document.querySelector(sel)) {
        evidence.push(`Cart button: ${sel}`);
        break;
      }
    }

    return {
      isProduct: evidence.length >= 2,
      evidence: evidence.join("; ") || "No product indicators found",
    };
  });
}

async function getVisibleProductCount(page: StagehandPage): Promise<number> {
  return page.evaluate(() => {
    // Try common product card selectors
    const selectors = [
      '[data-testid*="product"]',
      '[class*="product-card"]',
      '[class*="productCard"]',
      '[class*="product-tile"]',
      '[class*="productTile"]',
      '[class*="product-item"]',
      '[class*="productItem"]',
      'article[class*="product"]',
      '[data-auto-id*="product"]',
      'li[class*="product"]',
    ];
    for (const sel of selectors) {
      const els = document.querySelectorAll(sel);
      if (els.length >= 3) return els.length;
    }
    return 0;
  });
}

async function extractTotalProductCount(page: StagehandPage): Promise<{
  totalProducts: number | null;
  countText: string | null;
}> {
  return page.evaluate(() => {
    // Look for "X results", "X items", "Showing 1-48 of X" patterns
    const body = document.body.innerText;
    const patterns = [
      /(\d[\d,]+)\s*(?:results|items|products)/i,
      /of\s+(\d[\d,]+)/i,
      /showing\s+\d+\s*[-–]\s*\d+\s+of\s+(\d[\d,]+)/i,
      /(\d[\d,]+)\s+styles/i,
    ];
    for (const pat of patterns) {
      const m = body.match(pat);
      if (m) {
        const num = parseInt(m[1].replace(/,/g, ""), 10);
        if (num > 0 && num < 10_000_000) {
          return { totalProducts: num, countText: m[0] };
        }
      }
    }
    return { totalProducts: null, countText: null };
  });
}

// ---------------------------------------------------------------------------
// Extract <loc> URLs from sitemap XML via DOM (zero LLM tokens)
// ---------------------------------------------------------------------------

async function extractSitemapUrls(page: StagehandPage): Promise<string[]> {
  return page.evaluate(() => {
    // The browser renders XML sitemaps into a DOM — <loc> tags become elements
    const locs = document.querySelectorAll("loc");
    const urls: string[] = [];
    for (const loc of locs) {
      const text = loc.textContent?.trim();
      if (text) urls.push(text);
    }
    // Fallback: if the browser rendered it as plain text, regex-extract
    if (urls.length === 0) {
      const raw = document.body?.innerText ?? document.documentElement.outerHTML;
      const matches = raw.match(/https?:\/\/[^\s<>"']+/g);
      if (matches) {
        for (const m of matches) urls.push(m.replace(/[<>\s]+$/, ""));
      }
    }
    return urls.slice(0, 50); // cap at 50 to keep things reasonable
  });
}

// ---------------------------------------------------------------------------
// Sitemap check (Phase 1)
// ---------------------------------------------------------------------------

interface SitemapResult {
  sitemapUrl: string;
  productUrlPattern: string;
  sampleProductUrls: string[];
}

type StagehandPage = ReturnType<Stagehand["context"]["pages"]>[number];

async function checkSitemap(
  baseUrl: string,
  page: StagehandPage,
  stagehand: Stagehand,
): Promise<SitemapResult | null> {
  // Step A: Fetch robots.txt
  log("Phase 1: Checking robots.txt for sitemap...");
  try {
    await withRetry(() => page.goto(baseUrl + "/robots.txt"));
  } catch (err) {
    log("  Could not load robots.txt:", (err as Error).message);
    return null;
  }

  const robotsText = await page.evaluate(() => document.body.innerText);
  const sitemapLines = robotsText
    .split("\n")
    .filter((line: string) => /^sitemap:\s*/i.test(line.trim()));

  if (sitemapLines.length === 0) {
    log("  No Sitemap directives found in robots.txt.");
    return null;
  }

  const sitemapUrls = sitemapLines.map((line: string) =>
    line.replace(/^sitemap:\s*/i, "").trim(),
  );
  log(`  Found ${sitemapUrls.length} sitemap URL(s): ${sitemapUrls.join(", ")}`);

  // Step B: Find the product sitemap
  let productSitemapUrl: string | null = null;

  for (const smUrl of sitemapUrls) {
    log(`  Checking sitemap: ${smUrl}`);
    try {
      await withRetry(() => page.goto(smUrl));
    } catch {
      log(`  Could not load sitemap: ${smUrl}, skipping.`);
      continue;
    }

    const sitemapHtml = await page.evaluate(
      () => document.documentElement.outerHTML,
    );

    const isSitemapIndex =
      sitemapHtml.includes("<sitemapindex") ||
      sitemapHtml.includes("sitemapindex>");

    if (isSitemapIndex) {
      log("  Sitemap is an index. Looking for product sub-sitemap...");

      // DOM extraction instead of LLM call
      const subSitemapUrls = await extractSitemapUrls(page);
      log(`  Found ${subSitemapUrls.length} sub-sitemaps in index.`);

      const productSm = subSitemapUrls.find(
        (u) =>
          /product/i.test(u) ||
          /item/i.test(u) ||
          /catalog/i.test(u) ||
          /merch/i.test(u),
      );

      if (productSm) {
        log(`  Found product sub-sitemap: ${productSm}`);
        productSitemapUrl = productSm;
        break;
      }

      // Fallback: first sub-sitemap
      const fallback = subSitemapUrls[0];
      if (fallback) {
        log(
          `  No product-specific sub-sitemap found. Trying first sub-sitemap: ${fallback}`,
        );
        productSitemapUrl = fallback;
        break;
      }
    } else {
      // Not an index — this sitemap itself might contain product URLs
      productSitemapUrl = smUrl;
      break;
    }
  }

  if (!productSitemapUrl) {
    log("  Could not find a usable sitemap.");
    return null;
  }

  // Navigate to the product sitemap if we haven't already
  if (page.url() !== productSitemapUrl) {
    try {
      await withRetry(() => page.goto(productSitemapUrl!));
    } catch {
      log(`  Could not load product sitemap: ${productSitemapUrl}`);
      return null;
    }
  }

  // Step C: Extract sample product URLs (DOM-based, zero tokens)
  log("  Extracting sample URLs from sitemap...");

  const allSitemapUrls = await extractSitemapUrls(page);

  if (allSitemapUrls.length === 0) {
    log("  No URLs found in sitemap.");
    return null;
  }

  log(`  Found ${allSitemapUrls.length} URLs in sitemap.`);

  // Derive product URL pattern using TypeScript (zero tokens)
  const patternResult = deriveProductUrlPattern(allSitemapUrls.slice(0, 20));

  if (!patternResult.pattern || patternResult.matching.length === 0) {
    log("  Could not identify a product URL pattern from sitemap URLs.");
    return null;
  }

  log(`  Product URL pattern: ${patternResult.pattern}`);
  log(`  Matching URLs: ${patternResult.matching.length}, Non-matching: ${patternResult.nonMatching.length}`);

  // Step D: Verify 2-3 URLs are actual product pages (DOM-based checks)
  log("  Verifying sample product URLs...");
  const urlsToVerify = patternResult.matching.slice(0, 3);
  const verifiedUrls: string[] = [];

  for (const productUrl of urlsToVerify) {
    log(`  Checking: ${productUrl}`);
    try {
      await withRetry(() => page.goto(productUrl));
    } catch {
      log(`    Could not load, skipping.`);
      continue;
    }

    await dismissOverlays(stagehand, page);

    // DOM-based product page verification (zero tokens)
    const verification = await isProductPage(page);

    if (verification.isProduct) {
      log(`    Verified as product page: ${verification.evidence}`);
      verifiedUrls.push(productUrl);
    } else {
      log(`    Not a product page: ${verification.evidence}`);
    }
  }

  if (verifiedUrls.length === 0) {
    log("  None of the sample URLs were verified as product pages.");
    return null;
  }

  log(
    `  Sitemap check succeeded: ${verifiedUrls.length}/${urlsToVerify.length} URLs verified.`,
  );

  return {
    sitemapUrl: productSitemapUrl,
    productUrlPattern: patternResult.pattern,
    sampleProductUrls: verifiedUrls,
  };
}

// ---------------------------------------------------------------------------
// Category & Pagination exploration (Phase 2)
// ---------------------------------------------------------------------------

interface CategoryResult {
  method: "categoryPagination" | "infiniteScroll";
  startUrls: string[];
  paginationConfig?: {
    paginationParam: string;
    pageStartsAt: number;
    pageIncrement: number;
    maxPages: number;
  };
  scrollConfig?: {
    scrollPauseMs: number;
    maxScrolls: number;
    xhrPattern?: string;
  };
  productLinkExtraction: {
    selector: string;
    urlAttribute: string;
    productUrlPattern: string;
    urlIsRelative: boolean;
  };
  estimatedProductCount: number;
}

async function exploreCategories(
  baseUrl: string,
  page: StagehandPage,
  stagehand: Stagehand,
): Promise<CategoryResult | null> {
  // Step A: Navigate to homepage
  log("Phase 2: Exploring categories and pagination...");
  log("  Navigating to homepage...");

  try {
    await withRetry(() => page.goto(baseUrl));
  } catch (err) {
    log("  Could not load homepage:", (err as Error).message);
    return null;
  }

  await dismissOverlays(stagehand, page);

  // Step B: Extract category URLs from navigation
  // This genuinely needs the LLM — nav structures are too varied for selectors
  log("  Observing navigation structure...");

  try {
    await stagehand.act(
      "Open the main navigation menu or hover over a top-level category to reveal sub-categories. If the menu is already visible, do nothing.",
    );
  } catch {
    log("  Could not open navigation menu (may already be visible).");
  }

  log("  Extracting category URLs...");

  const categoryData = await stagehand.extract(
    "Extract all product category links from the navigation menu on this page. For each category, provide the name, URL, gender (men/women/unisex/unknown), and category type (e.g. Tops, Bottoms, Shoes, Dresses, Outerwear, Accessories). Skip non-product links like Sale, Gift Cards, About, Help, etc.",
    z.object({
      categories: z.array(
        z.object({
          name: z.string(),
          url: z.string(),
          gender: z
            .enum(["men", "women", "unisex", "unknown"])
            .optional(),
          type: z.string().optional(),
        }),
      ),
    }),
  );

  if (categoryData.categories.length === 0) {
    log("  No category URLs found in navigation.");
    return null;
  }

  // Resolve relative URLs and deduplicate
  const resolvedCategories = categoryData.categories.map((cat) => ({
    ...cat,
    url: new URL(cat.url, baseUrl).toString(),
  }));

  // Pick a manageable set: up to 10 categories covering both genders
  const startUrls = resolvedCategories.slice(0, 10).map((c) => c.url);
  log(`  Found ${resolvedCategories.length} categories. Using ${startUrls.length} as start URLs.`);
  for (const cat of resolvedCategories.slice(0, 10)) {
    log(`    ${cat.gender ?? "?"} / ${cat.type ?? cat.name}: ${cat.url}`);
  }

  // Step C: Navigate to first category and detect pagination
  const firstCategoryUrl = startUrls[0];
  log(`\n  Navigating to first category: ${firstCategoryUrl}`);

  try {
    await withRetry(() => page.goto(firstCategoryUrl));
  } catch {
    log("  Could not load category page.");
    return null;
  }

  await dismissOverlays(stagehand, page);

  // Pagination detection — still needs LLM for the variety of implementations
  log("  Detecting pagination type...");

  const paginationInfo = await stagehand.extract(
    "Analyze the pagination on this product listing page. Determine: (1) what type of pagination is used — numbered pages, next/prev buttons, load more button, or infinite scroll / none visible, (2) if there are page links, what URL parameter is used for pagination (e.g. 'page', 'p', 'offset'), (3) what the starting page number is.",
    z.object({
      paginationType: z
        .enum(["numbered_pages", "next_prev", "load_more", "infinite_scroll", "none_visible"])
        .describe("The pagination mechanism detected"),
      paginationParam: z
        .string()
        .nullable()
        .describe("Query parameter name used for pagination, e.g. 'page'"),
      startingPage: z
        .number()
        .nullable()
        .describe("The starting page number, usually 1"),
      nextPageUrl: z
        .string()
        .nullable()
        .describe("URL of the next page link if visible"),
    }),
  );

  log(`  Pagination type: ${paginationInfo.paginationType}`);

  let method: "categoryPagination" | "infiniteScroll";
  let paginationConfig: CategoryResult["paginationConfig"];
  let scrollConfig: CategoryResult["scrollConfig"];

  const usesPagination =
    paginationInfo.paginationType === "numbered_pages" ||
    paginationInfo.paginationType === "next_prev";

  if (usesPagination) {
    method = "categoryPagination";

    // Determine page increment by checking page 2 URL
    let pageIncrement = 1;
    const param = paginationInfo.paginationParam ?? "page";
    const startPage = paginationInfo.startingPage ?? 1;

    if (paginationInfo.nextPageUrl) {
      try {
        const nextUrl = new URL(paginationInfo.nextPageUrl, baseUrl);
        const nextVal = parseInt(nextUrl.searchParams.get(param) ?? "", 10);
        if (!isNaN(nextVal) && nextVal > startPage) {
          pageIncrement = nextVal - startPage;
        }
      } catch {
        // keep default increment
      }
    }

    // Verify pagination works by comparing product names on page 1 vs 2
    log("  Verifying pagination — checking page 1 products...");

    // DOM-based: grab first few product link texts
    const page1Names = await page.evaluate(() => {
      const selectors = [
        '[data-testid*="product"] a',
        '[class*="product-card"] a',
        '[class*="productCard"] a',
        'article[class*="product"] a',
        '[class*="product-tile"] a',
      ];
      for (const sel of selectors) {
        const els = document.querySelectorAll(sel);
        if (els.length >= 2) {
          return Array.from(els).slice(0, 3).map(
            (el) => (el as HTMLElement).textContent?.trim() ?? "",
          );
        }
      }
      return [] as string[];
    });

    const page2Url = paginationInfo.nextPageUrl
      ? new URL(paginationInfo.nextPageUrl, baseUrl).toString()
      : `${firstCategoryUrl}${firstCategoryUrl.includes("?") ? "&" : "?"}${param}=${startPage + pageIncrement}`;

    log(`  Navigating to page 2: ${page2Url}`);
    try {
      await withRetry(() => page.goto(page2Url));
      await dismissOverlays(stagehand, page);

      const page2Names = await page.evaluate(() => {
        const selectors = [
          '[data-testid*="product"] a',
          '[class*="product-card"] a',
          '[class*="productCard"] a',
          'article[class*="product"] a',
          '[class*="product-tile"] a',
        ];
        for (const sel of selectors) {
          const els = document.querySelectorAll(sel);
          if (els.length >= 2) {
            return Array.from(els).slice(0, 3).map(
              (el) => (el as HTMLElement).textContent?.trim() ?? "",
            );
          }
        }
        return [] as string[];
      });

      const isDifferent =
        page2Names.length > 0 &&
        page1Names.length > 0 &&
        page2Names[0] !== page1Names[0];

      if (isDifferent) {
        log("  Page 2 shows different products — pagination confirmed.");
      } else {
        log("  Warning: Page 2 products may be the same as page 1.");
      }
    } catch {
      log("  Could not verify page 2, proceeding with detected pagination.");
    }

    // Navigate back to first category for product link extraction
    await withRetry(() => page.goto(firstCategoryUrl));
    await dismissOverlays(stagehand, page);

    paginationConfig = {
      paginationParam: param,
      pageStartsAt: startPage,
      pageIncrement,
      maxPages: 50,
    };

    log(`  Pagination config: param=${param}, startsAt=${startPage}, increment=${pageIncrement}`);
  } else {
    // Infinite scroll or load-more
    method = "infiniteScroll";
    log("  Testing infinite scroll...");

    // DOM-based product count (zero tokens)
    const beforeCount = await getVisibleProductCount(page);

    // Scroll to bottom
    await page.evaluate(() =>
      window.scrollTo(0, document.body.scrollHeight),
    );
    await page.waitForTimeout(2500);

    const afterCount = await getVisibleProductCount(page);

    if (afterCount > beforeCount) {
      log(`  Infinite scroll confirmed: ${beforeCount} → ${afterCount} products after scroll.`);
    } else {
      log(`  No new products after scroll (${beforeCount} → ${afterCount}). Using infinite scroll config as fallback.`);
    }

    // Scroll back to top for product link extraction
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    scrollConfig = {
      scrollPauseMs: 2000,
      maxScrolls: 100,
    };
  }

  // Step D: Extract product link selector — still needs LLM
  log("\n  Extracting product link selector...");

  const linkData = await stagehand.extract(
    "Find all product links on this listing page. Identify a single CSS selector that matches all product link elements (the <a> tags that link to individual product pages). Also determine if the URLs are relative or absolute, and provide a few example hrefs.",
    z.object({
      commonSelector: z
        .string()
        .describe("A CSS selector matching all product link <a> elements"),
      urlIsRelative: z.boolean(),
      sampleHrefs: z
        .array(z.string())
        .describe("A few example href values from the product links"),
    }),
  );

  log(`  Product link selector: ${linkData.commonSelector}`);
  log(`  URLs are relative: ${linkData.urlIsRelative}`);
  log(`  Sample hrefs: ${linkData.sampleHrefs.slice(0, 3).join(", ")}`);

  // Derive product URL pattern using TypeScript (zero tokens)
  const urlPatternResult = deriveProductUrlPattern(
    linkData.sampleHrefs.slice(0, 10).map((h) => {
      try { return new URL(h, baseUrl).toString(); } catch { return h; }
    }),
  );

  log(`  Product URL pattern: ${urlPatternResult.pattern}`);

  // Test selector on a second category page
  if (startUrls.length >= 2) {
    const secondCategoryUrl = startUrls[1];
    log(`\n  Testing selector on second category: ${secondCategoryUrl}`);

    try {
      await withRetry(() => page.goto(secondCategoryUrl));
      await dismissOverlays(stagehand, page);

      // DOM-based selector test (zero tokens)
      const selectorTestResult = await page.evaluate((selector: string) => {
        const els = document.querySelectorAll(selector);
        return { count: els.length, works: els.length >= 3 };
      }, linkData.commonSelector);

      if (selectorTestResult.works) {
        log(`  Selector confirmed on second category page (${selectorTestResult.count} matches).`);
      } else {
        log(`  Selector only matched ${selectorTestResult.count} elements on second page.`);
        // Fall back to LLM only if the DOM check fails
        const secondPageLinks = await stagehand.extract(
          `Find product links on this listing page. Does the CSS selector "${linkData.commonSelector}" match product links here? What CSS selector would you use?`,
          z.object({
            selectorWorks: z.boolean(),
            alternativeSelector: z.string().nullable(),
          }),
        );
        if (secondPageLinks.alternativeSelector) {
          log(`  Using alternative selector: ${secondPageLinks.alternativeSelector}`);
          linkData.commonSelector = secondPageLinks.alternativeSelector;
        }
      }
    } catch {
      log("  Could not verify selector on second page, proceeding.");
    }
  }

  // Step E: Estimate product count (DOM-based, zero tokens)
  log("\n  Estimating product count...");

  try {
    await withRetry(() => page.goto(firstCategoryUrl));
    await dismissOverlays(stagehand, page);
  } catch {
    // non-fatal
  }

  const countResult = await extractTotalProductCount(page);
  const estimatedProductCount = countResult.totalProducts ?? 0;
  if (estimatedProductCount > 0) {
    log(`  Estimated products in this category: ${estimatedProductCount} (${countResult.countText})`);
  } else {
    log("  Could not determine product count.");
  }

  log("\n  Phase 2 complete.");

  return {
    method,
    startUrls,
    paginationConfig,
    scrollConfig,
    productLinkExtraction: {
      selector: linkData.commonSelector,
      urlAttribute: "href",
      productUrlPattern: urlPatternResult.pattern,
      urlIsRelative: linkData.urlIsRelative,
    },
    estimatedProductCount,
  };
}

// ---------------------------------------------------------------------------
// Product page analysis (Phase 3) — mostly DOM-based now
// ---------------------------------------------------------------------------

interface ProductPageResult {
  hasJsonLd: boolean;
  jsonLdNotes: string;
  hasOpenGraph: boolean;
  additionalImageSelector?: string;
  genderInUrl?: { detected: boolean; patterns: Record<string, string> };
  categoryInUrl?: { detected: boolean; breadcrumbSelector?: string };
  requiresJsRendering: boolean;
  sampleProductUrls: string[];
  missingFields: string[];
  jsonLdCompleteness: "high" | "medium" | "low";
}

const ALL_JSONLD_FIELDS = [
  "name",
  "brand",
  "description",
  "price",
  "images",
  "sku",
  "mpn",
  "color",
  "category",
];

async function analyzeProductPages(
  productUrls: string[],
  page: StagehandPage,
  stagehand: Stagehand,
): Promise<ProductPageResult> {
  log("Phase 3: Analyzing product pages...");

  const urlsToCheck = productUrls.slice(0, 3);
  let jsonLdFoundCount = 0;
  let openGraphFoundCount = 0;
  let requiresJsRendering = false;
  const allFieldsFound = new Set<string>();
  const allNotes: string[] = [];
  let additionalImageSelector: string | undefined;
  const genderPatterns: Record<string, string> = {};
  let breadcrumbSelector: string | undefined;
  let breadcrumbDetected = false;
  const verifiedUrls: string[] = [];

  for (const productUrl of urlsToCheck) {
    log(`\n  Analyzing: ${productUrl}`);

    try {
      await withRetry(() => page.goto(productUrl));
    } catch {
      log("    Could not load, skipping.");
      continue;
    }

    await dismissOverlays(stagehand, page);
    verifiedUrls.push(productUrl);

    // 1. JSON-LD check (DOM-based, zero tokens)
    const jsonLdResult = await extractJsonLdFromPage(page);

    if (jsonLdResult.found) {
      jsonLdFoundCount++;
      for (const f of jsonLdResult.fieldsPresent) allFieldsFound.add(f.toLowerCase());
      if (jsonLdResult.notes) allNotes.push(jsonLdResult.notes);
      log(`    JSON-LD found. Fields: ${jsonLdResult.fieldsPresent.join(", ")}`);
    } else {
      log("    No JSON-LD Product data found.");
    }

    // 2. Open Graph check (DOM-based, zero tokens)
    const ogResult = await extractOgTagsFromPage(page);

    if (ogResult.found) {
      openGraphFoundCount++;
      log(`    Open Graph found. Tags: ${ogResult.tagsPresent.join(", ")}`);
    } else {
      log("    No Open Graph tags found.");
    }

    // 3. Image gallery — only use LLM if JSON-LD is missing image data
    if (jsonLdResult.found && !jsonLdResult.fieldsPresent.includes("images")) {
      // Try DOM-based image gallery detection first
      const imageGallerySelector = await page.evaluate(() => {
        const selectors = [
          '[class*="gallery"] img',
          '[class*="carousel"] img',
          '[class*="thumbnail"] img',
          '[data-testid*="gallery"] img',
          '[data-testid*="image"] img',
          '[class*="product-image"] img',
          '[class*="productImage"] img',
          'picture img[class*="product"]',
        ];
        for (const sel of selectors) {
          const els = document.querySelectorAll(sel);
          if (els.length >= 2) return sel;
        }
        return null;
      });

      if (imageGallerySelector) {
        additionalImageSelector = imageGallerySelector;
        log(`    Image gallery selector (DOM): ${additionalImageSelector}`);
      } else {
        // Fall back to LLM only for the image gallery selector
        const imageSelector = await stagehand.extract(
          "What CSS selector would match all product gallery/additional images on this page? Look for img elements in a gallery, carousel, or thumbnail strip.",
          z.object({
            selector: z.string().nullable(),
          }),
        );
        if (imageSelector.selector) {
          additionalImageSelector = imageSelector.selector;
          log(`    Image gallery selector (LLM): ${additionalImageSelector}`);
        }
      }
    }

    // 4. Gender in URL (pure string matching, zero tokens)
    const lowerUrl = productUrl.toLowerCase();
    if (/\/men[\/\?]|\/mens[\/\?]|\/male/i.test(lowerUrl)) {
      genderPatterns["male"] = lowerUrl.match(/(\/men\w*\/)/i)?.[1] ?? "/men/";
    }
    if (/\/women[\/\?]|\/womens[\/\?]|\/female/i.test(lowerUrl)) {
      genderPatterns["female"] = lowerUrl.match(/(\/women\w*\/)/i)?.[1] ?? "/women/";
    }

    // 5. Breadcrumbs (DOM-based, zero tokens)
    if (!breadcrumbDetected) {
      const breadcrumbResult = await extractBreadcrumbsFromPage(page);

      if (breadcrumbResult.hasBreadcrumbs) {
        breadcrumbDetected = true;
        breadcrumbSelector = breadcrumbResult.selector ?? undefined;
        log(`    Breadcrumbs found: ${breadcrumbSelector ?? "(via JSON-LD)"}`);
      }
    }

    // 6. JS rendering check — is JSON-LD in raw HTML source? (DOM-based, zero tokens)
    if (jsonLdResult.found) {
      const rawHtml = await page.evaluate(
        () => document.documentElement.outerHTML,
      );
      const jsonLdInSource = rawHtml.includes("application/ld+json");
      if (!jsonLdInSource) {
        requiresJsRendering = true;
        log("    JSON-LD is injected by JavaScript (not in raw HTML).");
      } else {
        log("    JSON-LD is in raw HTML source.");
      }
    }
  }

  // Aggregate results
  const hasJsonLd = jsonLdFoundCount > 0;
  const hasOpenGraph = openGraphFoundCount > 0;

  const missingFields = ALL_JSONLD_FIELDS.filter(
    (f) => !allFieldsFound.has(f),
  );

  let jsonLdCompleteness: "high" | "medium" | "low";
  if (!hasJsonLd) {
    jsonLdCompleteness = "low";
  } else if (allFieldsFound.size >= 6) {
    jsonLdCompleteness = "high";
  } else if (allFieldsFound.size >= 3) {
    jsonLdCompleteness = "medium";
  } else {
    jsonLdCompleteness = "low";
  }

  const genderDetected = Object.keys(genderPatterns).length > 0;

  const jsonLdNotes = hasJsonLd
    ? allNotes.join(" ") || "Standard schema.org Product."
    : "No JSON-LD Product data found on any checked page.";

  log(`\n  Phase 3 summary:`);
  log(`    JSON-LD: ${hasJsonLd ? "yes" : "no"} (${jsonLdFoundCount}/${urlsToCheck.length} pages)`);
  log(`    Completeness: ${jsonLdCompleteness}`);
  log(`    Open Graph: ${hasOpenGraph ? "yes" : "no"}`);
  log(`    Missing fields: ${missingFields.join(", ") || "none"}`);
  log(`    Requires JS rendering: ${requiresJsRendering}`);

  return {
    hasJsonLd,
    jsonLdNotes,
    hasOpenGraph,
    additionalImageSelector,
    genderInUrl: {
      detected: genderDetected,
      patterns: genderPatterns,
    },
    categoryInUrl: {
      detected: breadcrumbDetected,
      breadcrumbSelector,
    },
    requiresJsRendering,
    sampleProductUrls: verifiedUrls,
    missingFields,
    jsonLdCompleteness,
  };
}

// ---------------------------------------------------------------------------
// Extract product URLs from listing page (for category discovery path)
// ---------------------------------------------------------------------------

async function extractProductUrlsFromListing(
  startUrl: string,
  baseUrl: string,
  page: StagehandPage,
  stagehand: Stagehand,
): Promise<string[]> {
  log(`  Extracting product URLs from listing: ${startUrl}`);
  try {
    await withRetry(() => page.goto(startUrl));
  } catch {
    log("    Could not load listing page.");
    return [];
  }

  await dismissOverlays(stagehand, page);

  // Try DOM-based extraction first
  const domLinks = await page.evaluate((base: string) => {
    const selectors = [
      '[data-testid*="product"] a[href]',
      '[class*="product-card"] a[href]',
      '[class*="productCard"] a[href]',
      'article[class*="product"] a[href]',
      '[class*="product-tile"] a[href]',
      '[class*="product-item"] a[href]',
    ];
    for (const sel of selectors) {
      const els = document.querySelectorAll<HTMLAnchorElement>(sel);
      if (els.length >= 3) {
        const hrefs = new Set<string>();
        for (const el of els) {
          try {
            hrefs.add(new URL(el.href, base).toString());
          } catch { /* skip */ }
          if (hrefs.size >= 5) break;
        }
        return Array.from(hrefs);
      }
    }
    return [] as string[];
  }, baseUrl);

  if (domLinks.length >= 3) {
    log(`    Found ${domLinks.length} product URLs via DOM.`);
    return domLinks;
  }

  // Fall back to LLM
  const links = await stagehand.extract(
    "Extract the href values of the first 5 product links on this listing page. These are links to individual product detail pages.",
    z.object({
      hrefs: z.array(z.string()),
    }),
  );

  return links.hrefs.map((href) => new URL(href, baseUrl).toString());
}

// ---------------------------------------------------------------------------
// Verification checklist (Step 6)
// ---------------------------------------------------------------------------

interface VerificationContext {
  discoveryMethod: string;
  startUrls?: string[];
  selectorTestedOnMultiplePages: boolean;
  productUrlPattern?: string;
  productPageUrls: string[];
  jsonLdPageCount: number;
  totalPagesChecked: number;
}

function runVerificationChecklist(ctx: VerificationContext): void {
  log("\n--- Verification Checklist ---");

  // Check 1: startUrls visited (only for category methods)
  if (ctx.startUrls && ctx.startUrls.length > 0) {
    log(`  [PASS] ${ctx.startUrls.length} category start URL(s) collected`);
  } else if (ctx.discoveryMethod !== "sitemap") {
    log("  [FAIL] No category start URLs collected");
  } else {
    log("  [SKIP] Start URLs not applicable for sitemap method");
  }

  // Check 2: Selector tested on 2+ pages
  if (ctx.selectorTestedOnMultiplePages) {
    log("  [PASS] Product link selector tested on at least 2 pages");
  } else if (ctx.discoveryMethod === "sitemap") {
    log("  [SKIP] Product link selector not applicable for sitemap method");
  } else {
    log("  [FAIL] Product link selector only tested on 1 page");
  }

  // Check 3: Product URL pattern matches real product pages
  if (ctx.productUrlPattern && ctx.productPageUrls.length > 0) {
    let matchCount = 0;
    try {
      const re = new RegExp(ctx.productUrlPattern);
      for (const url of ctx.productPageUrls) {
        if (re.test(url)) matchCount++;
      }
    } catch {
      log(`  [WARN] Could not compile product URL pattern as regex: ${ctx.productUrlPattern}`);
    }
    if (matchCount > 0) {
      log(`  [PASS] Product URL pattern matches ${matchCount}/${ctx.productPageUrls.length} verified URLs`);
    } else {
      log("  [FAIL] Product URL pattern doesn't match any verified URLs");
    }
  }

  // Check 4: JSON-LD confirmed on 2-3 pages
  if (ctx.jsonLdPageCount >= 2) {
    log(`  [PASS] JSON-LD confirmed on ${ctx.jsonLdPageCount}/${ctx.totalPagesChecked} product pages`);
  } else if (ctx.jsonLdPageCount === 1) {
    log(`  [WARN] JSON-LD only confirmed on 1/${ctx.totalPagesChecked} product pages`);
  } else {
    log(`  [FAIL] JSON-LD not found on any product page`);
  }

  // Check 5: Sample URLs verified
  if (ctx.productPageUrls.length >= 2) {
    log(`  [PASS] ${ctx.productPageUrls.length} sample product URLs verified with data`);
  } else if (ctx.productPageUrls.length === 1) {
    log("  [WARN] Only 1 sample product URL verified");
  } else {
    log("  [FAIL] No sample product URLs verified");
  }

  log("-----------------------------\n");
}

// ---------------------------------------------------------------------------
// Exported exploration function — callable from both CLI and server
// ---------------------------------------------------------------------------

export async function exploreRetailer(
  url: string,
  onLog?: (msg: string) => void,
): Promise<Record<string, unknown>> {
  if (onLog) setLogFn(onLog);

  // Reset overlay tracking for each new exploration
  _overlaysDismissed.clear();

  try {
    const baseUrl = new URL(url).origin;
    const retailer = extractRetailerSlug(url);
    const displayName = extractDisplayName(url);

    log(`\n--- Crawler Config Generator ---`);
    log(`URL:      ${url}`);
    log(`Base URL: ${baseUrl}`);
    log(`Retailer: ${retailer} (${displayName})`);
    log("");

    const configsDir = path.join(process.cwd(), "configs");
    fs.mkdirSync(configsDir, { recursive: true });

    log("Initializing Stagehand (LOCAL, headed browser)...");

    const stagehand = new Stagehand({
      env: "LOCAL",
      localBrowserLaunchOptions: { headless: false },
      model: "anthropic/claude-sonnet-4-6",
    });

    await stagehand.init();
    log("Stagehand initialized.\n");

    const page = stagehand.context.pages()[0];
    let blocksHeadlessBrowsers = false;

    const sitemapResult = await checkSitemap(baseUrl, page, stagehand);

    let categoryResult: CategoryResult | null = null;

    if (!sitemapResult) {
      log("\nNo usable sitemap found. Starting category exploration...\n");

      try {
        categoryResult = await exploreCategories(baseUrl, page, stagehand);
      } catch (err) {
        const message = (err as Error).message ?? "";
        if (/403|captcha|blocked|forbidden/i.test(message)) {
          blocksHeadlessBrowsers = true;
          log("  Site appears to block headless browsers.");
        } else {
          log("  Category exploration failed:", message);
        }
      }
    }

    if (!sitemapResult && !categoryResult) {
      log("\nBoth sitemap and category exploration failed. Writing minimal config...\n");

      const minimalConfig: Record<string, unknown> = {
        retailer,
        retailerDisplayName: displayName,
        baseUrl,
        discovery: {
          method: null,
          notes: "Both sitemap check and category exploration failed. Manual investigation needed.",
        },
        requestConfig: {
          blocksHeadlessBrowsers,
          notes: blocksHeadlessBrowsers
            ? "Site blocks headless browsers (403/CAPTCHA detected)."
            : "",
        },
      };

      writePartialConfig(configsDir, retailer, minimalConfig);
      await stagehand.close();
      log("Done.");
      return minimalConfig;
    }

    let productUrlsForAnalysis: string[] = [];

    if (sitemapResult) {
      productUrlsForAnalysis = sitemapResult.sampleProductUrls;
    } else if (categoryResult) {
      const extracted = await extractProductUrlsFromListing(
        categoryResult.startUrls[0],
        baseUrl,
        page,
        stagehand,
      );
      productUrlsForAnalysis = extracted.slice(0, 5);
    }

    let productPageResult: ProductPageResult | null = null;

    if (productUrlsForAnalysis.length > 0) {
      try {
        productPageResult = await analyzeProductPages(
          productUrlsForAnalysis,
          page,
          stagehand,
        );
      } catch (err) {
        log("  Product page analysis failed:", (err as Error).message);
      }
    } else {
      log("  No product URLs available for Phase 3 analysis.");
    }

    const discoveryMethod = sitemapResult ? "sitemap" : categoryResult!.method;
    const productUrlPattern = sitemapResult
      ? sitemapResult.productUrlPattern
      : categoryResult?.productLinkExtraction.productUrlPattern;

    runVerificationChecklist({
      discoveryMethod,
      startUrls: categoryResult?.startUrls,
      selectorTestedOnMultiplePages: !!categoryResult && (categoryResult.startUrls.length >= 2),
      productUrlPattern,
      productPageUrls: productPageResult?.sampleProductUrls ?? [],
      jsonLdPageCount: productPageResult?.hasJsonLd
        ? productPageResult.sampleProductUrls.length
        : 0,
      totalPagesChecked: productPageResult?.sampleProductUrls.length ?? 0,
    });

    log("Assembling final config...");

    let discovery: Record<string, unknown>;

    if (sitemapResult) {
      discovery = {
        method: "sitemap",
        sitemap: {
          url: sitemapResult.sitemapUrl,
          productUrlPattern: sitemapResult.productUrlPattern,
        },
      };
    } else {
      discovery = { method: categoryResult!.method };
      if (categoryResult!.method === "categoryPagination" && categoryResult!.paginationConfig) {
        discovery.categoryPagination = {
          startUrls: categoryResult!.startUrls,
          ...categoryResult!.paginationConfig,
        };
      } else if (categoryResult!.method === "infiniteScroll" && categoryResult!.scrollConfig) {
        discovery.infiniteScroll = {
          startUrls: categoryResult!.startUrls,
          ...categoryResult!.scrollConfig,
        };
      }
    }

    const productLinkExtraction =
      categoryResult && discoveryMethod !== "sitemap"
        ? categoryResult.productLinkExtraction
        : undefined;

    const productPage = productPageResult
      ? {
          hasJsonLd: productPageResult.hasJsonLd,
          jsonLdNotes: productPageResult.jsonLdNotes,
          hasOpenGraph: productPageResult.hasOpenGraph,
          additionalImageSelector: productPageResult.additionalImageSelector,
          genderInUrl: productPageResult.genderInUrl,
          categoryInUrl: productPageResult.categoryInUrl,
        }
      : {
          hasJsonLd: false,
          jsonLdNotes: "Product page analysis did not complete.",
          hasOpenGraph: false,
        };

    const requestNotes: string[] = [];
    if (blocksHeadlessBrowsers) requestNotes.push("Site blocks headless browsers.");
    const requestConfig = {
      requiresJsRendering: productPageResult?.requiresJsRendering ?? true,
      delayBetweenRequestsMs: 2000,
      blocksHeadlessBrowsers,
      notes: requestNotes.join(" ") || "No special requirements detected.",
    };

    let overallRecommendation: "recommended" | "usable" | "not recommended";
    const completeness = productPageResult?.jsonLdCompleteness ?? "low";
    if (completeness === "high" && !blocksHeadlessBrowsers) {
      overallRecommendation = "recommended";
    } else if (completeness === "medium" || (completeness === "high" && blocksHeadlessBrowsers)) {
      overallRecommendation = "usable";
    } else if (!productPageResult?.hasJsonLd) {
      overallRecommendation = "not recommended";
    } else {
      overallRecommendation = "usable";
    }

    const estimatedProductCount = categoryResult?.estimatedProductCount ?? 0;

    const dataQuality = {
      jsonLdCompleteness: completeness,
      estimatedProductCount,
      sampleProductUrls: productPageResult?.sampleProductUrls ?? [],
      missingFields: productPageResult?.missingFields ?? [],
      overallRecommendation,
    };

    const finalConfig: Record<string, unknown> = {
      retailer,
      retailerDisplayName: displayName,
      baseUrl,
      discovery,
      ...(productLinkExtraction ? { productLinkExtraction } : {}),
      productPage,
      requestConfig,
      dataQuality,
    };

    try {
      const { validateConfig } = await import("./schemas/config.js");
      validateConfig(finalConfig);
      log("Config validated against schema successfully.");
    } catch (err) {
      log("Config validation warning — schema mismatch (writing anyway):");
      const zodErr = err as { errors?: Array<{ path: (string | number)[]; message: string }> };
      if (zodErr.errors) {
        for (const issue of zodErr.errors) {
          log(`  ${issue.path.join(".")}: ${issue.message}`);
        }
      }
    }

    writePartialConfig(configsDir, retailer, finalConfig);

    log("\n--- Summary ---");
    log(`  Retailer:         ${displayName} (${retailer})`);
    log(`  Discovery method: ${discoveryMethod}`);
    log(`  JSON-LD:          ${productPage.hasJsonLd ? "yes" : "no"} (${completeness})`);
    log(`  JS rendering:     ${requestConfig.requiresJsRendering ? "required" : "not required"}`);
    log(`  Recommendation:   ${overallRecommendation}`);
    log(`  Output:           configs/${retailer}.json`);
    log("----------------\n");

    await stagehand.close();
    log("Done.");

    return finalConfig;
  } finally {
    resetLogFn();
  }
}

// ---------------------------------------------------------------------------
// CLI entrypoint
// ---------------------------------------------------------------------------

async function main() {
  const { url } = parseArgs();
  await exploreRetailer(url, console.log);
}

const isCLI = process.argv[1]?.includes("explore");
if (isCLI) {
  main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  });
}