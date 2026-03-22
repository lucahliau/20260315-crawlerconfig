import "dotenv/config";
import { Stagehand, type StagehandMetrics } from "@browserbasehq/stagehand";
import { z } from "zod";
import fs from "node:fs";
import path from "node:path";
import { addToMasterList } from "./discoverBrands.js";
import { writeJsonAtomic } from "./jsonFs.js";
import { retailerSlugFromUrl } from "./retailerSlug.js";
import { estimateUsdFromStagehandMetrics } from "./pricing.js";
import { getStagehandModel } from "./stagehandModel.js";

export interface ExploreRetailerResult {
  config: Record<string, unknown>;
  metrics: StagehandMetrics | null;
  /** Rough USD from Stagehand token totals (same model as `model` option). */
  estimatedUsd: number;
}

async function shutdownStagehand(stagehand: Stagehand | null): Promise<StagehandMetrics | null> {
  if (!stagehand) return null;
  try {
    const m = await stagehand.metrics;
    await stagehand.close();
    return m;
  } catch {
    try {
      await stagehand.close();
    } catch {
      // ignore
    }
    return null;
  }
}

// ---------------------------------------------------------------------------
// Logging — per-session context instead of global mutable state
// ---------------------------------------------------------------------------

interface SessionContext {
  logFn: (msg: string) => void;
  overlaysDismissed: Set<string>;
  abortController: AbortController;
}

function createSession(onLog?: (msg: string) => void): SessionContext {
  return {
    logFn: onLog ?? console.log,
    overlaysDismissed: new Set(),
    abortController: new AbortController(),
  };
}

function log(session: SessionContext, ...args: unknown[]): void {
  session.logFn(args.map(String).join(" "));
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

function extractDisplayName(url: string): string {
  const slug = retailerSlugFromUrl(url);
  return slug.charAt(0).toUpperCase() + slug.slice(1);
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Maximum time (ms) for the entire exploration before we abort
const OVERALL_TIMEOUT_MS = parseInt(process.env.EXPLORE_TIMEOUT_MS ?? "300000", 10); // 5 min

// Maximum time (ms) for a single page.goto call
const NAV_TIMEOUT_MS = 30_000;

// ---------------------------------------------------------------------------
// Retry helper — rate-limit aware with exponential backoff
// ---------------------------------------------------------------------------

async function withRetry<T>(
  fn: () => Promise<T>,
  retries = 3,
  delayMs = 4000,
  session?: SessionContext,
): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    if (retries <= 0) throw err;

    const msg = (err as Error).message ?? String(err);
    const isRateLimit = /rate.?limit/i.test(msg);
    const isTimeout = /timeout|timed?\s*out|net::ERR_/i.test(msg);
    const isNavError = /navigation|frame was detached|execution context/i.test(msg);

    // If rate-limited, wait 60s; if timeout/nav error, shorter retry
    let actualDelay: number;
    if (isRateLimit) {
      actualDelay = Math.max(delayMs, 60_000);
    } else if (isTimeout || isNavError) {
      actualDelay = Math.min(delayMs, 5000);
    } else {
      actualDelay = delayMs;
    }

    if (session) {
      log(session, `  ${isRateLimit ? "Rate limited" : isTimeout ? "Timeout" : "Error"} — retrying after ${actualDelay / 1000}s... (${retries} left)`);
    }
    await new Promise((r) => setTimeout(r, actualDelay));

    // Exponential backoff: double the delay for the next retry
    return withRetry(fn, retries - 1, actualDelay * 2, session);
  }
}

// ---------------------------------------------------------------------------
// Safe navigation helper — wraps goto with timeout + waitUntil
// ---------------------------------------------------------------------------

type StagehandPage = ReturnType<Stagehand["context"]["pages"]>[number];

async function safeGoto(
  page: StagehandPage,
  url: string,
  session: SessionContext,
  options?: { waitUntil?: "load" | "domcontentloaded" | "networkidle"; timeout?: number },
): Promise<void> {
  const waitUntil = options?.waitUntil ?? "domcontentloaded";
  const timeout = options?.timeout ?? NAV_TIMEOUT_MS;

  await withRetry(
    async () => {
      await page.goto(url, { waitUntil, timeout } as any);
    },
    2,
    3000,
    session,
  );

  // Give SPAs a moment to hydrate after DOM is ready
  await page.waitForTimeout(1500);
}

// ---------------------------------------------------------------------------
// Cookie / modal dismissal — tracked per session, not globally
// ---------------------------------------------------------------------------

async function dismissOverlays(
  stagehand: Stagehand,
  page: StagehandPage,
  session: SessionContext,
): Promise<void> {
  let domain: string;
  try {
    domain = new URL(page.url()).hostname;
  } catch {
    domain = "__unknown__";
  }

  if (session.overlaysDismissed.has(domain)) return;

  // First try a quick DOM-based dismissal (zero LLM tokens)
  const dismissed = await page.evaluate(() => {
    const selectors = [
      // Cookie consent
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
      // Generic close/dismiss
      '[class*="overlay"] button[class*="close"]',
      '[class*="modal"] button[class*="close"]',
      '[class*="popup"] button[class*="close"]',
      '[class*="dialog"] button[class*="close"]',
      '[aria-label="Close"]',
      '[aria-label="close"]',
      '[aria-label="Dismiss"]',
      'button[class*="dismiss"]',
      'button[class*="Dismiss"]',
      // Newsletter popups
      '[class*="newsletter"] button[class*="close"]',
      '[class*="signup"] button[class*="close"]',
      '[class*="email-capture"] button[class*="close"]',
      // "No thanks" / "Maybe later" text in buttons
      'button[class*="close-modal"]',
      'button[class*="closeModal"]',
      '[data-dismiss="modal"]',
      // Specific patterns for large retailers
      'button[data-testid="dialog-close-button"]',
      'button[data-testid="modal-close"]',
      '[class*="promo-close"]',
      '[class*="banner-close"]',
    ];
    let found = false;
    for (const sel of selectors) {
      try {
        const btn = document.querySelector<HTMLElement>(sel);
        if (btn && btn.offsetParent !== null) {
          btn.click();
          found = true;
        }
      } catch { /* selector might be invalid on some pages */ }
    }

    // Also try clicking buttons with common dismiss text
    const allButtons = document.querySelectorAll("button, a[role='button'], [role='button']");
    const dismissTexts = ["no thanks", "maybe later", "close", "accept all", "accept cookies", "got it", "i agree", "dismiss"];
    for (const btn of allButtons) {
      const text = (btn as HTMLElement).textContent?.trim().toLowerCase() ?? "";
      if (text.length < 30 && dismissTexts.some((t) => text.includes(t))) {
        const el = btn as HTMLElement;
        if (el.offsetParent !== null) {
          el.click();
          found = true;
          break;
        }
      }
    }

    return found;
  });

  if (dismissed) {
    log(session, "  Dismissed overlays via DOM selectors.");
    session.overlaysDismissed.add(domain);
    await page.waitForTimeout(800);
    return;
  }

  // Fallback: use Stagehand LLM (costs tokens, but only once per domain)
  try {
    await stagehand.act(
      "If there is a cookie consent banner, popup overlay, newsletter signup, country/region selector, or any modal dialog visible, dismiss or accept it. If nothing is visible, do nothing.",
    );
    session.overlaysDismissed.add(domain);
  } catch {
    // Non-fatal — mark as done so we don't retry
    session.overlaysDismissed.add(domain);
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
  writeJsonAtomic(outPath, partialData);
}

function explorationArtifactsPath(retailer: string): string {
  const dir =
    process.env.EXPLORATION_ARTIFACTS_DIR ?? path.join(process.cwd(), "exploration-artifacts");
  return path.join(dir, `${retailer}.json`);
}

function writeExplorationArtifacts(retailer: string, artifact: Record<string, unknown>): void {
  const outPath = explorationArtifactsPath(retailer);
  writeJsonAtomic(outPath, {
    retailer,
    updatedAt: new Date().toISOString(),
    ...artifact,
  });
}

// ---------------------------------------------------------------------------
// Pure-TypeScript URL pattern derivation (replaces LLM call)
// ---------------------------------------------------------------------------

function deriveProductUrlPattern(urls: string[]): {
  pattern: string;
  matching: string[];
  nonMatching: string[];
} {
  // Common product URL indicators — ordered by specificity
  const productIndicators: { re: RegExp; name: string }[] = [
    { re: /\/dp\/[A-Z0-9]+/, name: "amazon-dp" },               // Amazon /dp/ASIN
    { re: /\/p\/[^/?#]+/, name: "p-slug" },                      // /p/product-slug
    { re: /\/product[s]?\/[^/?#]+/, name: "products-slug" },     // /products/slug
    { re: /\/item[s]?\/[^/?#]+/, name: "items-slug" },           // /items/slug
    { re: /\/prd\/[^/?#]+/, name: "prd-slug" },                  // /prd/slug
    { re: /\/pd\/[^/?#]+/, name: "pd-slug" },                    // /pd/slug
    { re: /\/t\/[^/?#]+\/[^/?#]+/, name: "t-slug" },             // Nike-style /t/Name/ID
    { re: /\/shop\/[^/?#]+\/[^/?#]+/, name: "shop-slug" },       // /shop/cat/product
    { re: /\/buy\/[^/?#]+/, name: "buy-slug" },                  // /buy/product
    { re: /\/i\/[^/?#]+/, name: "i-slug" },                      // /i/product
    { re: /\/[^/?#]+-\d{5,}/, name: "slug-numericid" },          // slug-with-numeric-id
    { re: /\/\d{5,}(?:[/?#]|$)/, name: "bare-numericid" },       // bare numeric ID
    { re: /\/[^/?#]+\.html(?:[?#]|$)/, name: "html-ext" },       // slug.html
    { re: /\/collection[s]?\/[^/?#]+\/product[s]?\//, name: "collection-product" }, // Shopify-style
    { re: /\/[^/?#]+\/[^/?#]+\.asp/, name: "asp-ext" },          // legacy .asp pages
  ];

  const parsed = urls.map((u) => {
    try { return new URL(u); } catch { return null; }
  }).filter(Boolean) as URL[];

  if (parsed.length === 0) {
    return { pattern: ".*", matching: urls, nonMatching: [] };
  }

  // Check known product indicators first
  for (const { re } of productIndicators) {
    const matches = urls.filter((u) => re.test(u));
    if (matches.length >= Math.max(2, Math.ceil(urls.length * 0.3))) {
      return {
        pattern: re.source,
        matching: matches,
        nonMatching: urls.filter((u) => !re.test(u)),
      };
    }
  }

  // Heuristic: find the most common path depth and prefix structure
  const pathSegments = parsed.map((u) =>
    u.pathname.split("/").filter(Boolean),
  );

  // Group URLs by their path prefix (first N-1 segments)
  const prefixGroups = new Map<string, string[]>();
  for (let i = 0; i < pathSegments.length; i++) {
    const segs = pathSegments[i];
    if (segs.length >= 2) {
      const prefix = "/" + segs.slice(0, -1).join("/");
      if (!prefixGroups.has(prefix)) prefixGroups.set(prefix, []);
      prefixGroups.get(prefix)!.push(urls[i]);
    }
  }

  // Find the largest prefix group
  let bestPrefix = "";
  let bestGroup: string[] = [];
  for (const [prefix, group] of prefixGroups) {
    if (group.length > bestGroup.length) {
      bestPrefix = prefix;
      bestGroup = group;
    }
  }

  if (bestGroup.length >= Math.max(2, Math.ceil(urls.length * 0.3))) {
    const pattern = bestPrefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "/[^/?#]+";
    const re = new RegExp(pattern);
    return {
      pattern,
      matching: urls.filter((u) => re.test(u)),
      nonMatching: urls.filter((u) => !re.test(u)),
    };
  }

  // Find common prefix segments (original approach as further fallback)
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
    const pattern = prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "/[^/?#]+";
    const re = new RegExp(pattern);
    return {
      pattern,
      matching: urls.filter((u) => re.test(u)),
      nonMatching: urls.filter((u) => !re.test(u)),
    };
  }

  // Last resort: match any URL with 2+ path segments (less aggressive than 3+)
  const depthPattern = "^https?://[^/]+/[^/]+/[^/?#]+";
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
      '[class*="Price"]',
      '[data-testid*="price"]',
      '[data-testid*="Price"]',
      '[itemprop="price"]',
      '[class*="cost"]',
      '[class*="amount"]',
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
      'button[class*="AddToCart"]',
      'button[class*="add-to-bag"]',
      'button[class*="addToBag"]',
      'button[class*="AddToBag"]',
      'button[data-testid*="add-to"]',
      '[id*="add-to-cart"]',
      '[id*="addToCart"]',
      'button[class*="buy-now"]',
      'button[class*="buyNow"]',
      'button[class*="add_to_cart"]',
      'button[name="add"]',
      'form[action*="/cart"] button[type="submit"]',
    ];
    for (const sel of cartSelectors) {
      if (document.querySelector(sel)) {
        evidence.push(`Cart button: ${sel}`);
        break;
      }
    }

    // Also check for add-to-cart text in buttons
    if (evidence.length < 2) {
      const buttons = document.querySelectorAll("button");
      const cartTexts = ["add to cart", "add to bag", "buy now", "add to basket"];
      for (const btn of buttons) {
        const text = btn.textContent?.trim().toLowerCase() ?? "";
        if (cartTexts.some((t) => text.includes(t))) {
          evidence.push(`Cart button text: "${text.slice(0, 30)}"`);
          break;
        }
      }
    }

    // Weaker signal: product-related meta or schema
    if (evidence.length < 2) {
      if (document.querySelector('[itemtype*="schema.org/Product"]')) {
        evidence.push("Microdata Product schema");
      }
    }

    return {
      isProduct: evidence.length >= 2,
      evidence: evidence.join("; ") || "No product indicators found",
    };
  });
}

// Expanded set of product card selectors covering more sites
const PRODUCT_CARD_SELECTORS = [
  '[data-testid*="product"]',
  '[data-testid*="Product"]',
  '[class*="product-card"]',
  '[class*="productCard"]',
  '[class*="ProductCard"]',
  '[class*="product-tile"]',
  '[class*="productTile"]',
  '[class*="ProductTile"]',
  '[class*="product-item"]',
  '[class*="productItem"]',
  '[class*="ProductItem"]',
  '[class*="product-grid"] > *',
  '[class*="productGrid"] > *',
  '[class*="ProductGrid"] > *',
  'article[class*="product"]',
  'article[class*="Product"]',
  '[data-auto-id*="product"]',
  'li[class*="product"]',
  'li[class*="Product"]',
  '[class*="plp-"] li',
  '[class*="grid-item"]',
  '[class*="collection-product"]',
  '[class*="search-result"]',
  '[class*="product_card"]',
  '[class*="product_tile"]',
  '[class*="productList"] > *',
  '[data-component="product-card"]',
  '[data-component="ProductCard"]',
];

async function getVisibleProductCount(page: StagehandPage): Promise<number> {
  return page.evaluate((selectors: string[]) => {
    for (const sel of selectors) {
      try {
        const els = document.querySelectorAll(sel);
        if (els.length >= 3) return els.length;
      } catch { /* invalid selector, skip */ }
    }
    return 0;
  }, PRODUCT_CARD_SELECTORS);
}

async function extractTotalProductCount(page: StagehandPage): Promise<{
  totalProducts: number | null;
  countText: string | null;
}> {
  return page.evaluate(() => {
    const body = document.body.innerText;
    const patterns = [
      /showing\s+\d+\s*[-–]\s*\d+\s+of\s+(\d[\d,]+)/i,
      /(\d[\d,]+)\s*(?:results|items|products|styles)/i,
      /of\s+(\d[\d,]+)\s*(?:results|items|products|styles)?/i,
      /\((\d[\d,]+)\s*(?:results|items|products|styles)?\)/i,
      /total[:\s]+(\d[\d,]+)/i,
      /found\s+(\d[\d,]+)/i,
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

async function extractSitemapUrls(page: StagehandPage, maxUrls = 200): Promise<string[]> {
  return page.evaluate((limit: number) => {
    // The browser renders XML sitemaps into a DOM — <loc> tags become elements
    const locs = document.querySelectorAll("loc");
    const urls: string[] = [];
    for (const loc of locs) {
      const text = loc.textContent?.trim();
      if (text) urls.push(text);
      if (urls.length >= limit) break;
    }
    // Fallback: if the browser rendered it as plain text, regex-extract
    if (urls.length === 0) {
      const raw = document.body?.innerText ?? document.documentElement.outerHTML;
      const matches = raw.match(/https?:\/\/[^\s<>"']+/g);
      if (matches) {
        for (const m of matches) {
          urls.push(m.replace(/[<>\s]+$/, ""));
          if (urls.length >= limit) break;
        }
      }
    }
    return urls;
  }, maxUrls);
}

// ---------------------------------------------------------------------------
// Sitemap check (Phase 1) — improved: tries multiple sources, handles nesting
// ---------------------------------------------------------------------------

interface SitemapResult {
  sitemapUrl: string;
  productUrlPattern: string;
  sampleProductUrls: string[];
}

async function checkSitemap(
  baseUrl: string,
  page: StagehandPage,
  stagehand: Stagehand,
  session: SessionContext,
): Promise<SitemapResult | null> {
  log(session, "Phase 1: Checking for sitemaps...");

  const sitemapCandidates: string[] = [];

  // Step A: Check robots.txt for Sitemap directives
  log(session, "  Checking robots.txt...");
  try {
    await safeGoto(page, baseUrl + "/robots.txt", session);
    const robotsText = await page.evaluate(() => document.body.innerText);
    const sitemapLines = robotsText
      .split("\n")
      .filter((line: string) => /^sitemap:\s*/i.test(line.trim()));

    for (const line of sitemapLines) {
      const url = line.replace(/^sitemap:\s*/i, "").trim();
      if (url) sitemapCandidates.push(url);
    }

    if (sitemapCandidates.length > 0) {
      log(session, `  Found ${sitemapCandidates.length} sitemap(s) in robots.txt`);
    } else {
      log(session, "  No Sitemap directives in robots.txt.");
    }
  } catch (err) {
    log(session, "  Could not load robots.txt:", (err as Error).message);
  }

  // Step A2: Also try common sitemap paths directly (many sites don't list in robots.txt)
  const commonPaths = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemaps/sitemap.xml",
    "/sitemap/sitemap-index.xml",
    "/sitemap_products.xml",
    "/product-sitemap.xml",
  ];

  for (const p of commonPaths) {
    const candidate = baseUrl + p;
    if (!sitemapCandidates.includes(candidate)) {
      sitemapCandidates.push(candidate);
    }
  }

  // Step B: Walk through candidates, resolving sitemap indexes recursively (max 2 levels)
  let productSitemapUrl: string | null = null;
  let productUrls: string[] = [];

  for (const smUrl of sitemapCandidates) {
    log(session, `  Trying sitemap: ${smUrl}`);

    try {
      await safeGoto(page, smUrl, session, { timeout: 15000 });
    } catch {
      log(session, `    Could not load, skipping.`);
      continue;
    }

    // Check if this URL actually returned XML/sitemap content
    const contentCheck = await page.evaluate(() => {
      const html = document.documentElement.outerHTML;
      return {
        hasSitemapTag: html.includes("<sitemapindex") || html.includes("<urlset"),
        hasLocTags: document.querySelectorAll("loc").length > 0,
        bodyLength: (document.body?.innerText ?? "").length,
      };
    });

    if (!contentCheck.hasSitemapTag && !contentCheck.hasLocTags && contentCheck.bodyLength < 100) {
      log(session, "    Not a valid sitemap, skipping.");
      continue;
    }

    const isSitemapIndex =
      (await page.evaluate(() => document.documentElement.outerHTML)).includes("<sitemapindex");

    if (isSitemapIndex) {
      log(session, "    Sitemap index detected. Looking for product sub-sitemap...");

      const subSitemapUrls = await extractSitemapUrls(page, 100);
      log(session, `    Found ${subSitemapUrls.length} sub-sitemaps.`);

      if (subSitemapUrls.length === 0) continue;

      // Prioritize product-related sub-sitemaps
      const productSm = subSitemapUrls.find(
        (u) =>
          /product/i.test(u) ||
          /item/i.test(u) ||
          /catalog/i.test(u) ||
          /merch/i.test(u) ||
          /good/i.test(u),
      );

      const targetSm = productSm ?? subSitemapUrls[0];
      log(session, `    Trying sub-sitemap: ${targetSm}`);

      try {
        await safeGoto(page, targetSm, session, { timeout: 15000 });
      } catch {
        log(session, `    Could not load sub-sitemap.`);
        continue;
      }

      // Handle nested sitemap index (index → index → urls)
      const isNestedIndex =
        (await page.evaluate(() => document.documentElement.outerHTML)).includes("<sitemapindex");

      if (isNestedIndex) {
        log(session, "    Nested sitemap index detected. Going one level deeper...");
        const nestedUrls = await extractSitemapUrls(page, 50);
        if (nestedUrls.length > 0) {
          const nestedTarget = nestedUrls.find((u) => /product/i.test(u)) ?? nestedUrls[0];
          try {
            await safeGoto(page, nestedTarget, session, { timeout: 15000 });
            productSitemapUrl = nestedTarget;
          } catch {
            log(session, "    Could not load nested sub-sitemap.");
            continue;
          }
        } else {
          continue;
        }
      } else {
        productSitemapUrl = targetSm;
      }
    } else {
      // Direct URL list sitemap
      productSitemapUrl = smUrl;
    }

    // We have a sitemap with URLs — extract them
    if (productSitemapUrl) {
      productUrls = await extractSitemapUrls(page, 200);
      if (productUrls.length > 0) {
        log(session, `    Extracted ${productUrls.length} URLs from sitemap.`);
        break;
      } else {
        log(session, "    No URLs found in this sitemap, trying next...");
        productSitemapUrl = null;
      }
    }
  }

  if (!productSitemapUrl || productUrls.length === 0) {
    log(session, "  No usable sitemap found.");
    return null;
  }

  // Step C: Derive product URL pattern from a larger sample
  log(session, "  Deriving product URL pattern...");

  // Use more URLs for pattern derivation (up to 50) for better accuracy
  const patternResult = deriveProductUrlPattern(productUrls.slice(0, 50));

  if (!patternResult.pattern || patternResult.matching.length === 0) {
    log(session, "  Could not identify a product URL pattern from sitemap URLs.");
    return null;
  }

  log(session, `  Product URL pattern: ${patternResult.pattern}`);
  log(session, `  Matching URLs: ${patternResult.matching.length}, Non-matching: ${patternResult.nonMatching.length}`);

  // Step D: Verify 2-3 URLs are actual product pages
  log(session, "  Verifying sample product URLs...");
  const urlsToVerify = patternResult.matching.slice(0, 4); // try up to 4 to get at least 2
  const verifiedUrls: string[] = [];

  for (const productUrl of urlsToVerify) {
    if (verifiedUrls.length >= 3) break; // enough verified
    log(session, `  Checking: ${productUrl}`);
    try {
      await safeGoto(page, productUrl, session);
    } catch {
      log(session, `    Could not load, skipping.`);
      continue;
    }

    await dismissOverlays(stagehand, page, session);

    const verification = await isProductPage(page);

    if (verification.isProduct) {
      log(session, `    Verified as product page: ${verification.evidence}`);
      verifiedUrls.push(productUrl);
    } else {
      log(session, `    Not a product page: ${verification.evidence}`);
    }
  }

  if (verifiedUrls.length === 0) {
    // If DOM checks fail, try a broader check — the page might use non-standard markup
    log(session, "  DOM verification found 0 product pages — trying broader check...");
    const fallbackUrl = patternResult.matching[0];
    if (fallbackUrl) {
      try {
        await safeGoto(page, fallbackUrl, session);
        await dismissOverlays(stagehand, page, session);

        // Check if the page at least has meaningful content (title, images)
        const hasContent = await page.evaluate(() => {
          const title = document.title;
          const images = document.querySelectorAll("img");
          const h1 = document.querySelector("h1");
          return {
            hasTitle: title.length > 10,
            imageCount: images.length,
            hasH1: !!h1,
            h1Text: h1?.textContent?.trim() ?? "",
          };
        });

        if (hasContent.hasTitle && hasContent.imageCount > 0 && hasContent.hasH1) {
          log(session, `    Looks like a content page (h1: "${hasContent.h1Text.slice(0, 50)}"). Accepting with low confidence.`);
          verifiedUrls.push(fallbackUrl);
        }
      } catch {
        // give up
      }
    }
  }

  if (verifiedUrls.length === 0) {
    log(session, "  None of the sample URLs could be verified.");
    return null;
  }

  log(session, `  Sitemap check succeeded: ${verifiedUrls.length}/${urlsToVerify.length} URLs verified.`);

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
  session: SessionContext,
): Promise<CategoryResult | null> {
  log(session, "Phase 2: Exploring categories and pagination...");
  log(session, "  Navigating to homepage...");

  try {
    await safeGoto(page, baseUrl, session, { waitUntil: "load" });
  } catch (err) {
    log(session, "  Could not load homepage:", (err as Error).message);
    return null;
  }

  await dismissOverlays(stagehand, page, session);

  // Step B: Extract category URLs from navigation
  log(session, "  Observing navigation structure...");

  // Try to open the nav, but don't fail if this doesn't work
  try {
    await stagehand.act(
      "Open the main navigation menu or hover over a top-level category to reveal sub-categories. If the menu is already visible, do nothing.",
    );
  } catch {
    log(session, "  Could not open navigation menu (may already be visible).");
  }

  // Small wait for menus to animate open
  await page.waitForTimeout(1000);

  log(session, "  Extracting category URLs...");

  let categoryData: { categories: { name: string; url: string; gender?: string; type?: string }[] };

  try {
    categoryData = await stagehand.extract(
      "Extract all product category links from the navigation menu on this page. For each category, provide the name, URL, gender (men/women/unisex/unknown), and category type (e.g. Tops, Bottoms, Shoes, Dresses, Outerwear, Accessories). Skip non-product links like Sale, Gift Cards, About, Help, etc. Include links from dropdown menus and mega menus if visible.",
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
  } catch (err) {
    log(session, "  LLM category extraction failed:", (err as Error).message);
    // Fallback: try DOM-based nav link extraction
    log(session, "  Falling back to DOM-based nav link extraction...");

    const domCategories = await page.evaluate((base: string) => {
      const navLinks: { name: string; url: string }[] = [];
      const navSelectors = [
        "nav a[href]",
        "[role='navigation'] a[href]",
        "header a[href]",
        '[class*="nav"] a[href]',
        '[class*="menu"] a[href]',
      ];
      const seen = new Set<string>();
      for (const sel of navSelectors) {
        const links = document.querySelectorAll<HTMLAnchorElement>(sel);
        for (const link of links) {
          const text = link.textContent?.trim() ?? "";
          const href = link.href;
          if (!text || text.length < 2 || text.length > 50) continue;
          if (/cart|account|login|sign.?in|help|about|contact|gift/i.test(text)) continue;
          if (!href || href === "#" || href.includes("javascript:")) continue;
          try {
            const resolved = new URL(href, base).toString();
            if (!seen.has(resolved) && resolved.startsWith(base)) {
              seen.add(resolved);
              navLinks.push({ name: text, url: resolved });
            }
          } catch { /* skip */ }
        }
        if (navLinks.length >= 20) break;
      }
      return navLinks;
    }, baseUrl);

    if (domCategories.length === 0) {
      log(session, "  No category URLs found.");
      return null;
    }

    categoryData = { categories: domCategories.map((c) => ({ ...c, gender: "unknown" as const })) };
  }

  if (categoryData.categories.length === 0) {
    log(session, "  No category URLs found in navigation.");
    return null;
  }

  // Resolve relative URLs and deduplicate
  const seen = new Set<string>();
  const resolvedCategories = categoryData.categories
    .map((cat) => {
      try {
        return { ...cat, url: new URL(cat.url, baseUrl).toString() };
      } catch {
        return null;
      }
    })
    .filter((cat): cat is NonNullable<typeof cat> => {
      if (!cat) return false;
      // Deduplicate by path (ignore trailing slash differences)
      const key = cat.url.replace(/\/$/, "");
      if (seen.has(key)) return false;
      seen.add(key);
      // Filter out homepage and non-same-origin links
      try {
        const u = new URL(cat.url);
        const base = new URL(baseUrl);
        if (u.origin !== base.origin) return false;
        if (u.pathname === "/" || u.pathname === "") return false;
      } catch { return false; }
      return true;
    });

  // Pick a diverse set: up to 12 categories, preferring different genders
  const startUrls: string[] = [];
  const usedPaths = new Set<string>();

  // First pass: add one from each gender
  for (const gender of ["men", "women", "unisex", "unknown"]) {
    const cat = resolvedCategories.find((c) => c.gender === gender && !usedPaths.has(c.url));
    if (cat && startUrls.length < 12) {
      startUrls.push(cat.url);
      usedPaths.add(cat.url);
    }
  }

  // Fill remaining slots
  for (const cat of resolvedCategories) {
    if (startUrls.length >= 12) break;
    if (!usedPaths.has(cat.url)) {
      startUrls.push(cat.url);
      usedPaths.add(cat.url);
    }
  }

  log(session, `  Found ${resolvedCategories.length} categories. Using ${startUrls.length} as start URLs.`);
  for (const cat of resolvedCategories.filter((c) => startUrls.includes(c.url))) {
    log(session, `    ${cat.gender ?? "?"} / ${cat.type ?? cat.name}: ${cat.url}`);
  }

  // Step C: Navigate to first category and detect pagination
  const firstCategoryUrl = startUrls[0];
  log(session, `\n  Navigating to first category: ${firstCategoryUrl}`);

  try {
    await safeGoto(page, firstCategoryUrl, session, { waitUntil: "load" });
  } catch {
    log(session, "  Could not load category page.");
    return null;
  }

  await dismissOverlays(stagehand, page, session);

  // Wait for product content to load (SPAs may need extra time)
  await page.waitForTimeout(2000);

  // Step C2: Try DOM-based pagination detection first (zero tokens)
  log(session, "  Detecting pagination type...");

  const domPagination = await page.evaluate(() => {
    // Look for pagination links in the DOM
    const paginationSelectors = [
      'nav[aria-label*="pagination"] a',
      'nav[aria-label*="Pagination"] a',
      '[class*="pagination"] a',
      '[class*="Pagination"] a',
      '[data-testid*="pagination"] a',
      '.pager a',
      '[class*="paging"] a',
      'ul[class*="page"] a',
    ];

    for (const sel of paginationSelectors) {
      const links = document.querySelectorAll<HTMLAnchorElement>(sel);
      if (links.length >= 2) {
        // Found pagination links — try to extract param
        for (const link of links) {
          try {
            const url = new URL(link.href);
            for (const [param, val] of url.searchParams) {
              const num = parseInt(val, 10);
              if (!isNaN(num) && num >= 1 && num <= 1000) {
                return {
                  found: true,
                  type: "numbered_pages" as const,
                  param,
                  startPage: 1,
                  nextUrl: link.href,
                  sampleValue: num,
                };
              }
            }
            // Check path-based pagination like /page/2
            const pathMatch = link.pathname.match(/\/page\/(\d+)/);
            if (pathMatch) {
              return {
                found: true,
                type: "numbered_pages" as const,
                param: "page_path",
                startPage: 1,
                nextUrl: link.href,
                sampleValue: parseInt(pathMatch[1], 10),
              };
            }
          } catch { /* skip */ }
        }
      }
    }

    // Check for "next" button
    const nextSelectors = [
      'a[aria-label*="next" i]',
      'a[aria-label*="Next"]',
      'a[class*="next"]',
      'a[class*="Next"]',
      'button[aria-label*="next" i]',
      '[data-testid*="next"]',
      'a[rel="next"]',
    ];
    for (const sel of nextSelectors) {
      const el = document.querySelector<HTMLAnchorElement>(sel);
      if (el?.href) {
        try {
          const url = new URL(el.href);
          for (const [param, val] of url.searchParams) {
            const num = parseInt(val, 10);
            if (!isNaN(num)) {
              return {
                found: true,
                type: "next_prev" as const,
                param,
                startPage: Math.max(1, num - 1),
                nextUrl: el.href,
                sampleValue: num,
              };
            }
          }
        } catch { /* skip */ }
      }
    }

    // Check for load-more button
    const loadMoreSelectors = [
      'button[class*="load-more"]',
      'button[class*="loadMore"]',
      'button[class*="LoadMore"]',
      'button[class*="show-more"]',
      '[data-testid*="load-more"]',
    ];
    for (const sel of loadMoreSelectors) {
      if (document.querySelector(sel)) {
        return {
          found: true,
          type: "load_more" as const,
          param: null,
          startPage: null,
          nextUrl: null,
          sampleValue: null,
        };
      }
    }

    // Check button text
    const buttons = document.querySelectorAll("button");
    for (const btn of buttons) {
      const text = btn.textContent?.trim().toLowerCase() ?? "";
      if (text.includes("load more") || text.includes("show more") || text.includes("view more")) {
        return {
          found: true,
          type: "load_more" as const,
          param: null,
          startPage: null,
          nextUrl: null,
          sampleValue: null,
        };
      }
    }

    return { found: false, type: "none_visible" as const, param: null, startPage: null, nextUrl: null, sampleValue: null };
  });

  let paginationType: string;
  let paginationParam: string | null = null;
  let startingPage: number | null = null;
  let nextPageUrl: string | null = null;

  if (domPagination.found) {
    paginationType = domPagination.type;
    paginationParam = domPagination.param;
    startingPage = domPagination.startPage;
    nextPageUrl = domPagination.nextUrl;
    log(session, `  Pagination detected via DOM: ${paginationType} (param=${paginationParam})`);
  } else {
    // Fallback to LLM
    log(session, "  No pagination found via DOM, falling back to LLM...");

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

    paginationType = paginationInfo.paginationType;
    paginationParam = paginationInfo.paginationParam;
    startingPage = paginationInfo.startingPage;
    nextPageUrl = paginationInfo.nextPageUrl;
  }

  log(session, `  Pagination type: ${paginationType}`);

  let method: "categoryPagination" | "infiniteScroll";
  let paginationConfig: CategoryResult["paginationConfig"];
  let scrollConfig: CategoryResult["scrollConfig"];

  const usesPagination =
    paginationType === "numbered_pages" ||
    paginationType === "next_prev";

  if (usesPagination) {
    method = "categoryPagination";

    let pageIncrement = 1;
    const param = paginationParam ?? "page";
    const startPage = startingPage ?? 1;

    if (nextPageUrl) {
      try {
        const nextUrl = new URL(nextPageUrl, baseUrl);
        // Handle both query param and path-based pagination
        if (param === "page_path") {
          const match = nextUrl.pathname.match(/\/page\/(\d+)/);
          if (match) {
            pageIncrement = parseInt(match[1], 10) - startPage;
          }
        } else {
          const nextVal = parseInt(nextUrl.searchParams.get(param) ?? "", 10);
          if (!isNaN(nextVal) && nextVal > startPage) {
            pageIncrement = nextVal - startPage;
          }
        }
      } catch {
        // keep default increment
      }
    }

    // Verify pagination by comparing products on page 1 vs 2
    log(session, "  Verifying pagination — checking page 1 vs page 2...");

    const page1Products = await getProductTexts(page);

    const page2Url = nextPageUrl
      ? new URL(nextPageUrl, baseUrl).toString()
      : param === "page_path"
        ? `${firstCategoryUrl.replace(/\/$/, "")}/page/${startPage + pageIncrement}`
        : `${firstCategoryUrl}${firstCategoryUrl.includes("?") ? "&" : "?"}${param}=${startPage + pageIncrement}`;

    log(session, `  Navigating to page 2: ${page2Url}`);
    try {
      await safeGoto(page, page2Url, session);
      await dismissOverlays(stagehand, page, session);
      await page.waitForTimeout(1500);

      const page2Products = await getProductTexts(page);

      const isDifferent =
        page2Products.length > 0 &&
        page1Products.length > 0 &&
        // Compare multiple items, not just the first one
        (page2Products[0] !== page1Products[0] ||
         (page2Products.length > 1 && page1Products.length > 1 && page2Products[1] !== page1Products[1]));

      if (isDifferent) {
        log(session, `  Page 2 shows different products (${page1Products.length} → ${page2Products.length}) — pagination confirmed.`);
      } else if (page2Products.length > 0 && page1Products.length > 0) {
        log(session, "  Warning: Page 2 products appear identical to page 1. Pagination may not work.");
      } else {
        log(session, `  Could not compare pages (p1: ${page1Products.length} items, p2: ${page2Products.length} items).`);
      }
    } catch {
      log(session, "  Could not verify page 2, proceeding with detected pagination.");
    }

    // Navigate back to first category for product link extraction
    await safeGoto(page, firstCategoryUrl, session, { waitUntil: "load" });
    await dismissOverlays(stagehand, page, session);
    await page.waitForTimeout(1500);

    paginationConfig = {
      paginationParam: param,
      pageStartsAt: startPage,
      pageIncrement,
      maxPages: 50,
    };

    log(session, `  Pagination config: param=${param}, startsAt=${startPage}, increment=${pageIncrement}`);
  } else {
    // Infinite scroll or load-more
    method = "infiniteScroll";
    log(session, "  Testing infinite scroll...");

    const beforeCount = await getVisibleProductCount(page);

    // Scroll to bottom
    await page.evaluate(() =>
      window.scrollTo(0, document.body.scrollHeight),
    );
    await page.waitForTimeout(3000);

    // Try scrolling a bit more (some sites need multiple scrolls to trigger)
    await page.evaluate(() =>
      window.scrollTo(0, document.body.scrollHeight),
    );
    await page.waitForTimeout(2000);

    const afterCount = await getVisibleProductCount(page);

    if (afterCount > beforeCount) {
      log(session, `  Infinite scroll confirmed: ${beforeCount} → ${afterCount} products after scroll.`);
    } else {
      log(session, `  No new products after scroll (${beforeCount} → ${afterCount}). Using infinite scroll config as fallback.`);
    }

    // Scroll back to top for product link extraction
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(800);

    scrollConfig = {
      scrollPauseMs: 2000,
      maxScrolls: 100,
    };
  }

  // Step D: Extract product link selector
  log(session, "\n  Extracting product link selector...");

  // Try DOM-based extraction first
  const domSelector = await findProductLinkSelector(page, baseUrl);

  let commonSelector: string;
  let urlIsRelative: boolean;
  let sampleHrefs: string[];

  if (domSelector) {
    commonSelector = domSelector.selector;
    urlIsRelative = domSelector.urlIsRelative;
    sampleHrefs = domSelector.sampleHrefs;
    log(session, `  Product link selector (DOM): ${commonSelector} (${sampleHrefs.length} samples)`);
  } else {
    // Fall back to LLM
    log(session, "  DOM selector search failed, falling back to LLM...");

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

    commonSelector = linkData.commonSelector;
    urlIsRelative = linkData.urlIsRelative;
    sampleHrefs = linkData.sampleHrefs;
  }

  log(session, `  Product link selector: ${commonSelector}`);
  log(session, `  URLs are relative: ${urlIsRelative}`);
  log(session, `  Sample hrefs: ${sampleHrefs.slice(0, 3).join(", ")}`);

  // Derive product URL pattern
  const urlPatternResult = deriveProductUrlPattern(
    sampleHrefs.slice(0, 15).map((h) => {
      try { return new URL(h, baseUrl).toString(); } catch { return h; }
    }),
  );

  log(session, `  Product URL pattern: ${urlPatternResult.pattern}`);

  // Test selector on a second category page
  if (startUrls.length >= 2) {
    const secondCategoryUrl = startUrls[1];
    log(session, `\n  Testing selector on second category: ${secondCategoryUrl}`);

    try {
      await safeGoto(page, secondCategoryUrl, session, { waitUntil: "load" });
      await dismissOverlays(stagehand, page, session);
      await page.waitForTimeout(1500);

      const selectorTestResult = await page.evaluate((selector: string) => {
        try {
          const els = document.querySelectorAll(selector);
          return { count: els.length, works: els.length >= 3, error: null };
        } catch (e) {
          return { count: 0, works: false, error: (e as Error).message };
        }
      }, commonSelector);

      if (selectorTestResult.works) {
        log(session, `  Selector confirmed on second category page (${selectorTestResult.count} matches).`);
      } else if (selectorTestResult.error) {
        log(session, `  Selector is invalid CSS: ${selectorTestResult.error}`);
        // Try DOM-based fallback on this page
        const fallback = await findProductLinkSelector(page, baseUrl);
        if (fallback) {
          log(session, `  Found alternative selector: ${fallback.selector}`);
          commonSelector = fallback.selector;
        }
      } else {
        log(session, `  Selector only matched ${selectorTestResult.count} elements on second page.`);
        // Fall back to LLM
        try {
          const secondPageLinks = await stagehand.extract(
            `Find product links on this listing page. Does the CSS selector "${commonSelector}" match product links here? What CSS selector would you use?`,
            z.object({
              selectorWorks: z.boolean(),
              alternativeSelector: z.string().nullable(),
            }),
          );
          if (secondPageLinks.alternativeSelector) {
            log(session, `  Using alternative selector: ${secondPageLinks.alternativeSelector}`);
            commonSelector = secondPageLinks.alternativeSelector;
          }
        } catch {
          log(session, "  LLM fallback for selector also failed, keeping original.");
        }
      }
    } catch {
      log(session, "  Could not verify selector on second page, proceeding.");
    }
  }

  // Step E: Estimate product count
  log(session, "\n  Estimating product count...");

  try {
    await safeGoto(page, firstCategoryUrl, session);
    await dismissOverlays(stagehand, page, session);
  } catch {
    // non-fatal
  }

  const countResult = await extractTotalProductCount(page);
  const estimatedProductCount = countResult.totalProducts ?? 0;
  if (estimatedProductCount > 0) {
    log(session, `  Estimated products in this category: ${estimatedProductCount} (${countResult.countText})`);
  } else {
    log(session, "  Could not determine product count.");
  }

  log(session, "\n  Phase 2 complete.");

  return {
    method,
    startUrls,
    paginationConfig,
    scrollConfig,
    productLinkExtraction: {
      selector: commonSelector,
      urlAttribute: "href",
      productUrlPattern: urlPatternResult.pattern,
      urlIsRelative,
    },
    estimatedProductCount,
  };
}

// ---------------------------------------------------------------------------
// DOM-based product link selector finder
// ---------------------------------------------------------------------------

async function findProductLinkSelector(
  page: StagehandPage,
  baseUrl: string,
): Promise<{ selector: string; urlIsRelative: boolean; sampleHrefs: string[] } | null> {
  return page.evaluate((args: { base: string; cardSelectors: string[] }) => {
    const { cardSelectors } = args;
    // Strategy 1: Look for product cards with links
    for (const sel of cardSelectors) {
      try {
        const cards = document.querySelectorAll(sel);
        if (cards.length >= 3) {
          // Find links within cards
          const links = new Set<HTMLAnchorElement>();
          for (const card of cards) {
            const a = card.tagName === "A"
              ? card as HTMLAnchorElement
              : card.querySelector<HTMLAnchorElement>("a[href]");
            if (a) links.add(a);
          }

          if (links.size >= 3) {
            // Determine the best selector for these links
            const firstLink = Array.from(links)[0];
            const linkSel = sel.includes(">") ? sel.replace(/>\s*\*/, " a[href]") : sel + " a[href]";

            // Verify the generated selector
            const verify = document.querySelectorAll(linkSel);
            if (verify.length >= 3) {
              const hrefs: string[] = [];
              let hasRelative = false;
              for (const link of links) {
                if (hrefs.length >= 8) break;
                const href = link.getAttribute("href") ?? "";
                if (href && href !== "#") {
                  hrefs.push(href);
                  if (!href.startsWith("http")) hasRelative = true;
                }
              }
              if (hrefs.length >= 3) {
                return { selector: linkSel, urlIsRelative: hasRelative, sampleHrefs: hrefs };
              }
            }
          }
        }
      } catch { /* invalid selector */ }
    }

    // Strategy 2: Look for links that have product-like href patterns
    const allLinks = document.querySelectorAll<HTMLAnchorElement>("a[href]");
    const productLinks: HTMLAnchorElement[] = [];
    const productPatterns = [
      /\/product[s]?\//i,
      /\/p\//,
      /\/item[s]?\//i,
      /\/t\//,
      /\/shop\//i,
      /\/dp\//,
      /\/buy\//i,
    ];

    for (const link of allLinks) {
      const href = link.getAttribute("href") ?? "";
      if (productPatterns.some((p) => p.test(href))) {
        productLinks.push(link);
      }
    }

    if (productLinks.length >= 3) {
      // Try to find a common parent-based selector
      const hrefs = productLinks.slice(0, 8).map((l) => l.getAttribute("href") ?? "");
      const hasRelative = hrefs.some((h) => !h.startsWith("http"));

      // Check if they share a common class
      const classNames = productLinks.map((l) => Array.from(l.classList));
      if (classNames.length > 0 && classNames[0].length > 0) {
        for (const cls of classNames[0]) {
          if (classNames.every((cn) => cn.includes(cls))) {
            const sel = `a.${cls}`;
            const verify = document.querySelectorAll(sel);
            if (verify.length >= 3) {
              return { selector: sel, urlIsRelative: hasRelative, sampleHrefs: hrefs };
            }
          }
        }
      }

      // Fallback: use a generic pattern selector
      for (const pattern of productPatterns) {
        const matching = Array.from(allLinks).filter((l) => pattern.test(l.getAttribute("href") ?? ""));
        if (matching.length >= 3) {
          const hrefSamples = matching.slice(0, 8).map((l) => l.getAttribute("href") ?? "");
          return {
            selector: `a[href*="${pattern.source.replace(/[\\\/\[\]^$.*+?(){}|]/g, "").slice(0, 10)}"]`,
            urlIsRelative: hrefSamples.some((h) => !h.startsWith("http")),
            sampleHrefs: hrefSamples,
          };
        }
      }
    }

    return null;
  }, { base: baseUrl, cardSelectors: PRODUCT_CARD_SELECTORS });
}

// ---------------------------------------------------------------------------
// Helper: extract product text from listing page for comparison
// ---------------------------------------------------------------------------

async function getProductTexts(page: StagehandPage): Promise<string[]> {
  return page.evaluate((selectors: string[]) => {
    // Try product card selectors
    for (const sel of selectors) {
      try {
        const els = document.querySelectorAll(sel);
        if (els.length >= 2) {
          return Array.from(els).slice(0, 5).map(
            (el) => (el as HTMLElement).textContent?.trim().slice(0, 100) ?? "",
          ).filter(Boolean);
        }
      } catch { /* skip */ }
    }

    // Fallback: generic product link text
    const linkSelectors = [
      '[data-testid*="product"] a',
      '[class*="product-card"] a',
      '[class*="productCard"] a',
      'article[class*="product"] a',
      '[class*="product-tile"] a',
      '[class*="ProductCard"] a',
      '[class*="product-item"] a',
    ];
    for (const sel of linkSelectors) {
      try {
        const els = document.querySelectorAll(sel);
        if (els.length >= 2) {
          return Array.from(els).slice(0, 5).map(
            (el) => (el as HTMLElement).textContent?.trim().slice(0, 100) ?? "",
          ).filter(Boolean);
        }
      } catch { /* skip */ }
    }

    return [] as string[];
  }, PRODUCT_CARD_SELECTORS);
}

// ---------------------------------------------------------------------------
// Product page analysis (Phase 3)
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
  session: SessionContext,
): Promise<ProductPageResult> {
  log(session, "Phase 3: Analyzing product pages...");

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
    log(session, `\n  Analyzing: ${productUrl}`);

    try {
      await safeGoto(page, productUrl, session, { waitUntil: "load" });
    } catch {
      log(session, "    Could not load, skipping.");
      continue;
    }

    await dismissOverlays(stagehand, page, session);
    // Wait for dynamic content to render (important for SPAs like Nike)
    await page.waitForTimeout(2000);

    verifiedUrls.push(productUrl);

    // 1. JSON-LD check (DOM-based, zero tokens)
    const jsonLdResult = await extractJsonLdFromPage(page);

    if (jsonLdResult.found) {
      jsonLdFoundCount++;
      for (const f of jsonLdResult.fieldsPresent) allFieldsFound.add(f.toLowerCase());
      if (jsonLdResult.notes) allNotes.push(jsonLdResult.notes);
      log(session, `    JSON-LD found. Fields: ${jsonLdResult.fieldsPresent.join(", ")}`);
    } else {
      log(session, "    No JSON-LD Product data found.");
    }

    // 2. Open Graph check (DOM-based, zero tokens)
    const ogResult = await extractOgTagsFromPage(page);

    if (ogResult.found) {
      openGraphFoundCount++;
      log(session, `    Open Graph found. Tags: ${ogResult.tagsPresent.join(", ")}`);
    } else {
      log(session, "    No Open Graph tags found.");
    }

    // 3. Image gallery — only use LLM if JSON-LD is missing image data
    if (jsonLdResult.found && !jsonLdResult.fieldsPresent.includes("images")) {
      const imageGallerySelector = await page.evaluate(() => {
        const selectors = [
          '[class*="gallery"] img',
          '[class*="Gallery"] img',
          '[class*="carousel"] img',
          '[class*="Carousel"] img',
          '[class*="thumbnail"] img',
          '[data-testid*="gallery"] img',
          '[data-testid*="image"] img',
          '[class*="product-image"] img',
          '[class*="productImage"] img',
          '[class*="ProductImage"] img',
          'picture img[class*="product"]',
          '[class*="pdp-image"] img',
          '[class*="hero-image"] img',
          '[class*="slider"] img',
        ];
        for (const sel of selectors) {
          try {
            const els = document.querySelectorAll(sel);
            if (els.length >= 2) return sel;
          } catch { /* skip */ }
        }
        return null;
      });

      if (imageGallerySelector) {
        additionalImageSelector = imageGallerySelector;
        log(session, `    Image gallery selector (DOM): ${additionalImageSelector}`);
      } else {
        try {
          const imageSelector = await stagehand.extract(
            "What CSS selector would match all product gallery/additional images on this page? Look for img elements in a gallery, carousel, or thumbnail strip.",
            z.object({
              selector: z.string().nullable(),
            }),
          );
          if (imageSelector.selector) {
            additionalImageSelector = imageSelector.selector;
            log(session, `    Image gallery selector (LLM): ${additionalImageSelector}`);
          }
        } catch {
          log(session, "    Could not determine image gallery selector.");
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
        log(session, `    Breadcrumbs found: ${breadcrumbSelector ?? "(via JSON-LD)"}`);
      }
    }

    // 6. JS rendering check — fetch raw HTML source and compare
    // BUG FIX: The old code used document.documentElement.outerHTML which is the
    // RENDERED DOM, not the raw source. It always contains application/ld+json if
    // JSON-LD exists, making the check useless. Instead, we use a fetch() to get
    // the raw HTML source from the server.
    if (jsonLdResult.found && !requiresJsRendering) {
      try {
        const rawSourceCheck = await page.evaluate(async (url: string) => {
          try {
            const resp = await fetch(url, {
              headers: { "Accept": "text/html" },
              credentials: "same-origin",
            });
            if (!resp.ok) return { fetched: false, hasJsonLd: false };
            const text = await resp.text();
            return {
              fetched: true,
              hasJsonLd: text.includes("application/ld+json"),
            };
          } catch {
            return { fetched: false, hasJsonLd: false };
          }
        }, productUrl);

        if (rawSourceCheck.fetched) {
          if (!rawSourceCheck.hasJsonLd) {
            requiresJsRendering = true;
            log(session, "    JSON-LD is injected by JavaScript (not in raw HTML source).");
          } else {
            log(session, "    JSON-LD is present in raw HTML source.");
          }
        } else {
          // fetch() blocked (CORS, etc) — assume JS rendering needed as safe default
          requiresJsRendering = true;
          log(session, "    Could not fetch raw source (CORS?), assuming JS rendering needed.");
        }
      } catch {
        requiresJsRendering = true;
        log(session, "    Raw source check failed, assuming JS rendering needed.");
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

  log(session, `\n  Phase 3 summary:`);
  log(session, `    JSON-LD: ${hasJsonLd ? "yes" : "no"} (${jsonLdFoundCount}/${urlsToCheck.length} pages)`);
  log(session, `    Completeness: ${jsonLdCompleteness}`);
  log(session, `    Open Graph: ${hasOpenGraph ? "yes" : "no"}`);
  log(session, `    Missing fields: ${missingFields.join(", ") || "none"}`);
  log(session, `    Requires JS rendering: ${requiresJsRendering}`);

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
  session: SessionContext,
): Promise<string[]> {
  log(session, `  Extracting product URLs from listing: ${startUrl}`);
  try {
    await safeGoto(page, startUrl, session, { waitUntil: "load" });
  } catch {
    log(session, "    Could not load listing page.");
    return [];
  }

  await dismissOverlays(stagehand, page, session);
  await page.waitForTimeout(2000);

  // Try DOM-based extraction first with expanded selectors
  const domLinks = await page.evaluate((args: { base: string; cardSelectors: string[] }) => {
    const { base, cardSelectors } = args;
    // Strategy 1: product card selectors
    for (const sel of cardSelectors) {
      try {
        const cards = document.querySelectorAll(sel);
        if (cards.length >= 3) {
          const hrefs = new Set<string>();
          for (const card of cards) {
            const a = card.tagName === "A"
              ? card as HTMLAnchorElement
              : card.querySelector<HTMLAnchorElement>("a[href]");
            if (a?.href) {
              try {
                hrefs.add(new URL(a.href, base).toString());
              } catch { /* skip */ }
            }
            if (hrefs.size >= 8) break;
          }
          if (hrefs.size >= 3) return Array.from(hrefs);
        }
      } catch { /* skip */ }
    }

    // Strategy 2: all anchor-based product link selectors
    const linkSelectors = [
      '[data-testid*="product"] a[href]',
      '[class*="product-card"] a[href]',
      '[class*="productCard"] a[href]',
      '[class*="ProductCard"] a[href]',
      'article[class*="product"] a[href]',
      '[class*="product-tile"] a[href]',
      '[class*="product-item"] a[href]',
      '[class*="ProductItem"] a[href]',
      'a[class*="product-link"]',
      'a[class*="productLink"]',
      'a[href*="/product/"]',
      'a[href*="/products/"]',
      'a[href*="/p/"]',
    ];
    for (const sel of linkSelectors) {
      try {
        const els = document.querySelectorAll<HTMLAnchorElement>(sel);
        if (els.length >= 3) {
          const hrefs = new Set<string>();
          for (const el of els) {
            try {
              hrefs.add(new URL(el.href, base).toString());
            } catch { /* skip */ }
            if (hrefs.size >= 8) break;
          }
          if (hrefs.size >= 3) return Array.from(hrefs);
        }
      } catch { /* skip */ }
    }

    return [] as string[];
  }, { base: baseUrl, cardSelectors: PRODUCT_CARD_SELECTORS });

  if (domLinks.length >= 3) {
    log(session, `    Found ${domLinks.length} product URLs via DOM.`);
    return domLinks;
  }

  // Fall back to LLM
  log(session, "    DOM extraction found too few links, falling back to LLM...");
  try {
    const links = await stagehand.extract(
      "Extract the href values of the first 8 product links on this listing page. These are links to individual product detail pages.",
      z.object({
        hrefs: z.array(z.string()),
      }),
    );

    const resolved = links.hrefs.map((href) => {
      try { return new URL(href, baseUrl).toString(); } catch { return null; }
    }).filter(Boolean) as string[];

    log(session, `    Found ${resolved.length} product URLs via LLM.`);
    return resolved;
  } catch (err) {
    log(session, "    LLM extraction also failed:", (err as Error).message);
    return [];
  }
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

function runVerificationChecklist(ctx: VerificationContext, session: SessionContext): void {
  log(session, "\n--- Verification Checklist ---");

  if (ctx.startUrls && ctx.startUrls.length > 0) {
    log(session, `  [PASS] ${ctx.startUrls.length} category start URL(s) collected`);
  } else if (ctx.discoveryMethod !== "sitemap") {
    log(session, "  [FAIL] No category start URLs collected");
  } else {
    log(session, "  [SKIP] Start URLs not applicable for sitemap method");
  }

  if (ctx.selectorTestedOnMultiplePages) {
    log(session, "  [PASS] Product link selector tested on at least 2 pages");
  } else if (ctx.discoveryMethod === "sitemap") {
    log(session, "  [SKIP] Product link selector not applicable for sitemap method");
  } else {
    log(session, "  [FAIL] Product link selector only tested on 1 page");
  }

  if (ctx.productUrlPattern && ctx.productPageUrls.length > 0) {
    let matchCount = 0;
    try {
      const re = new RegExp(ctx.productUrlPattern);
      for (const url of ctx.productPageUrls) {
        if (re.test(url)) matchCount++;
      }
    } catch {
      log(session, `  [WARN] Could not compile product URL pattern as regex: ${ctx.productUrlPattern}`);
    }
    if (matchCount > 0) {
      log(session, `  [PASS] Product URL pattern matches ${matchCount}/${ctx.productPageUrls.length} verified URLs`);
    } else {
      log(session, "  [FAIL] Product URL pattern doesn't match any verified URLs");
    }
  }

  if (ctx.jsonLdPageCount >= 2) {
    log(session, `  [PASS] JSON-LD confirmed on ${ctx.jsonLdPageCount}/${ctx.totalPagesChecked} product pages`);
  } else if (ctx.jsonLdPageCount === 1) {
    log(session, `  [WARN] JSON-LD only confirmed on 1/${ctx.totalPagesChecked} product pages`);
  } else {
    log(session, `  [FAIL] JSON-LD not found on any product page`);
  }

  if (ctx.productPageUrls.length >= 2) {
    log(session, `  [PASS] ${ctx.productPageUrls.length} sample product URLs verified with data`);
  } else if (ctx.productPageUrls.length === 1) {
    log(session, "  [WARN] Only 1 sample product URL verified");
  } else {
    log(session, "  [FAIL] No sample product URLs verified");
  }

  log(session, "-----------------------------\n");
}

function buildCheckpointConfigSitemap(
  retailer: string,
  displayName: string,
  baseUrl: string,
  sitemapResult: SitemapResult,
  blocksHeadless: boolean,
): Record<string, unknown> {
  return {
    retailer,
    retailerDisplayName: displayName,
    baseUrl,
    discovery: {
      method: "sitemap",
      sitemap: {
        url: sitemapResult.sitemapUrl,
        productUrlPattern: sitemapResult.productUrlPattern,
      },
    },
    requestConfig: {
      requiresJsRendering: true,
      delayBetweenRequestsMs: 2000,
      blocksHeadlessBrowsers: blocksHeadless,
      notes: "Partial checkpoint after sitemap (Phase 1); Phase 3+ not yet complete.",
    },
    dataQuality: {
      jsonLdCompleteness: "unknown",
      estimatedProductCount: 0,
      sampleProductUrls: sitemapResult.sampleProductUrls,
      missingFields: [],
      overallRecommendation: "usable",
    },
    explorationCheckpoint: { phase: "post_sitemap", at: new Date().toISOString() },
  };
}

function buildCheckpointConfigCategory(
  retailer: string,
  displayName: string,
  baseUrl: string,
  categoryResult: CategoryResult,
  blocksHeadless: boolean,
): Record<string, unknown> {
  const discovery: Record<string, unknown> = { method: categoryResult.method };
  if (categoryResult.method === "categoryPagination" && categoryResult.paginationConfig) {
    discovery.categoryPagination = {
      startUrls: categoryResult.startUrls,
      ...categoryResult.paginationConfig,
    };
  } else if (categoryResult.method === "infiniteScroll" && categoryResult.scrollConfig) {
    discovery.infiniteScroll = {
      startUrls: categoryResult.startUrls,
      ...categoryResult.scrollConfig,
    };
  }
  return {
    retailer,
    retailerDisplayName: displayName,
    baseUrl,
    discovery,
    productLinkExtraction: categoryResult.productLinkExtraction,
    requestConfig: {
      requiresJsRendering: true,
      delayBetweenRequestsMs: 2000,
      blocksHeadlessBrowsers: blocksHeadless,
      notes: "Partial checkpoint after category/pagination discovery; Phase 3+ not yet complete.",
    },
    dataQuality: {
      jsonLdCompleteness: "unknown",
      estimatedProductCount: categoryResult.estimatedProductCount,
      sampleProductUrls: [],
      missingFields: [],
      overallRecommendation: "usable",
    },
    explorationCheckpoint: { phase: "post_category", at: new Date().toISOString() },
  };
}

function writeDegradedExploration(
  configsDir: string,
  retailer: string,
  displayName: string,
  baseUrl: string,
  err: unknown,
  partial: {
    sitemapResult: SitemapResult | null;
    categoryResult: CategoryResult | null;
    blocksHeadlessBrowsers: boolean;
    productPageResult: ProductPageResult | null;
  },
): void {
  const message = err instanceof Error ? err.message : String(err);
  const degraded: Record<string, unknown> = {
    retailer,
    retailerDisplayName: displayName,
    baseUrl,
    error: { message, at: new Date().toISOString() },
    requestConfig: {
      requiresJsRendering: partial.productPageResult?.requiresJsRendering ?? true,
      delayBetweenRequestsMs: 2000,
      blocksHeadlessBrowsers: partial.blocksHeadlessBrowsers,
      notes: "Best-effort save after exploration error.",
    },
  };

  if (partial.sitemapResult) {
    degraded.discovery = {
      method: "sitemap",
      sitemap: {
        url: partial.sitemapResult.sitemapUrl,
        productUrlPattern: partial.sitemapResult.productUrlPattern,
      },
    };
  } else if (partial.categoryResult) {
    const cr = partial.categoryResult;
    const discovery: Record<string, unknown> = { method: cr.method };
    if (cr.method === "categoryPagination" && cr.paginationConfig) {
      discovery.categoryPagination = { startUrls: cr.startUrls, ...cr.paginationConfig };
    } else if (cr.method === "infiniteScroll" && cr.scrollConfig) {
      discovery.infiniteScroll = { startUrls: cr.startUrls, ...cr.scrollConfig };
    }
    degraded.discovery = discovery;
    degraded.productLinkExtraction = cr.productLinkExtraction;
  }

  if (partial.productPageResult) {
    const p = partial.productPageResult;
    degraded.productPage = {
      hasJsonLd: p.hasJsonLd,
      jsonLdNotes: p.jsonLdNotes,
      hasOpenGraph: p.hasOpenGraph,
      additionalImageSelector: p.additionalImageSelector,
      genderInUrl: p.genderInUrl,
      categoryInUrl: p.categoryInUrl,
    };
    degraded.dataQuality = {
      jsonLdCompleteness: p.jsonLdCompleteness,
      estimatedProductCount: partial.categoryResult?.estimatedProductCount ?? 0,
      sampleProductUrls: p.sampleProductUrls,
      missingFields: p.missingFields,
      overallRecommendation: "usable",
    };
  } else {
    degraded.dataQuality = {
      jsonLdCompleteness: "unknown",
      estimatedProductCount: partial.categoryResult?.estimatedProductCount ?? 0,
      sampleProductUrls: partial.sitemapResult?.sampleProductUrls ?? [],
      missingFields: [],
      overallRecommendation: "usable",
    };
  }

  writePartialConfig(configsDir, retailer, degraded);
  writeExplorationArtifacts(retailer, {
    phase: "error_recovery",
    error: message,
    sitemapSampleProductUrls: partial.sitemapResult?.sampleProductUrls,
    categoryStartUrls: partial.categoryResult?.startUrls,
    productPageSampleUrls: partial.productPageResult?.sampleProductUrls,
  });
}

// ---------------------------------------------------------------------------
// Exported exploration function — callable from both CLI and server
// ---------------------------------------------------------------------------

export async function exploreRetailer(
  url: string,
  onLog?: (msg: string) => void,
): Promise<ExploreRetailerResult> {
  const session = createSession(onLog);

  // Overall timeout to prevent hanging forever
  const timeoutHandle = setTimeout(() => {
    session.abortController.abort();
  }, OVERALL_TIMEOUT_MS);

  let baseUrl = "";
  let retailer = "";
  let displayName = "";
  let configsDir = "";
  let stagehand: Stagehand | null = null;
  let sitemapResult: SitemapResult | null = null;
  let categoryResult: CategoryResult | null = null;
  let productPageResult: ProductPageResult | null = null;
  let blocksHeadlessBrowsers = false;

  try {
    baseUrl = new URL(url).origin;
    retailer = retailerSlugFromUrl(url);
    displayName = extractDisplayName(url);

    log(session, `\n--- Crawler Config Generator ---`);
    log(session, `URL:      ${url}`);
    log(session, `Base URL: ${baseUrl}`);
    log(session, `Retailer: ${retailer} (${displayName})`);
    log(session, "");

    configsDir = process.env.CONFIGS_DIR ?? path.join(process.cwd(), "configs");
    fs.mkdirSync(configsDir, { recursive: true });

    log(session, "Initializing Stagehand (LOCAL, headed browser)...");

    stagehand = new Stagehand({
      env: (process.env.STAGEHAND_ENV as "LOCAL" | "BROWSERBASE") || "LOCAL",
      localBrowserLaunchOptions: {
        headless: false,
        args: [
          "--disable-blink-features=AutomationControlled",
          "--disable-features=IsolateOrigins,site-per-process",
        ],
      },
      model: getStagehandModel(),
    });

    await stagehand.init();
    log(session, "Stagehand initialized.\n");

    const page = stagehand.context.pages()[0];

    // Check for abort before each phase
    if (session.abortController.signal.aborted) {
      throw new Error("Exploration timed out.");
    }

    sitemapResult = await checkSitemap(baseUrl, page, stagehand, session);

    if (sitemapResult) {
      const partial = buildCheckpointConfigSitemap(
        retailer,
        displayName,
        baseUrl,
        sitemapResult,
        blocksHeadlessBrowsers,
      );
      writePartialConfig(configsDir, retailer, partial);
      if (baseUrl) addToMasterList(baseUrl, displayName);
      writeExplorationArtifacts(retailer, {
        phase: "post_sitemap",
        sitemapUrl: sitemapResult.sitemapUrl,
        productUrlPattern: sitemapResult.productUrlPattern,
        sampleProductUrls: sitemapResult.sampleProductUrls,
      });
      log(session, "Checkpoint saved (post-sitemap).\n");
    }

    if (!sitemapResult) {
      if (session.abortController.signal.aborted) {
        throw new Error("Exploration timed out.");
      }

      log(session, "\nNo usable sitemap found. Starting category exploration...\n");

      try {
        categoryResult = await exploreCategories(baseUrl, page, stagehand, session);
      } catch (err) {
        const message = (err as Error).message ?? "";
        if (/403|captcha|blocked|forbidden/i.test(message)) {
          blocksHeadlessBrowsers = true;
          log(session, "  Site appears to block headless browsers.");
        } else if (/timed?\s*out/i.test(message)) {
          log(session, "  Category exploration timed out:", message);
        } else {
          log(session, "  Category exploration failed:", message);
        }
      }
    }

    if (!sitemapResult && categoryResult) {
      const partial = buildCheckpointConfigCategory(
        retailer,
        displayName,
        baseUrl,
        categoryResult,
        blocksHeadlessBrowsers,
      );
      writePartialConfig(configsDir, retailer, partial);
      if (baseUrl) addToMasterList(baseUrl, displayName);
      writeExplorationArtifacts(retailer, {
        phase: "post_category",
        startUrls: categoryResult.startUrls,
        productLinkExtraction: categoryResult.productLinkExtraction,
        estimatedProductCount: categoryResult.estimatedProductCount,
      });
      log(session, "Checkpoint saved (post-category).\n");
    }

    if (!sitemapResult && !categoryResult) {
      log(session, "\nBoth sitemap and category exploration failed. Writing minimal config...\n");

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
      const masterUrl = minimalConfig.baseUrl as string | undefined;
      const masterName = minimalConfig.retailerDisplayName as string | undefined;
      if (masterUrl) addToMasterList(masterUrl, masterName);
      const metrics = await shutdownStagehand(stagehand);
      stagehand = null;
      log(session, "Done.");
      const estimatedUsd = metrics ? estimateUsdFromStagehandMetrics(metrics) : 0;
      log(session, `Estimated Stagehand cost (this run): ~$${estimatedUsd.toFixed(4)}`);
      return { config: minimalConfig, metrics, estimatedUsd };
    }

    if (session.abortController.signal.aborted) {
      throw new Error("Exploration timed out.");
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
        session,
      );
      productUrlsForAnalysis = extracted.slice(0, 5);
    }

    if (productUrlsForAnalysis.length > 0) {
      if (session.abortController.signal.aborted) {
        throw new Error("Exploration timed out.");
      }

      try {
        productPageResult = await analyzeProductPages(
          productUrlsForAnalysis,
          page,
          stagehand,
          session,
        );
      } catch (err) {
        log(session, "  Product page analysis failed:", (err as Error).message);
      }
    } else {
      log(session, "  No product URLs available for Phase 3 analysis.");
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
    }, session);

    log(session, "Assembling final config...");

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
      log(session, "Config validated against schema successfully.");
    } catch (err) {
      log(session, "Config validation warning — schema mismatch (writing anyway):");
      const zodErr = err as { errors?: Array<{ path: (string | number)[]; message: string }> };
      if (zodErr.errors) {
        for (const issue of zodErr.errors) {
          log(session, `  ${issue.path.join(".")}: ${issue.message}`);
        }
      }
    }

    writePartialConfig(configsDir, retailer, finalConfig);
    const masterUrl = finalConfig.baseUrl as string | undefined;
    const masterName = finalConfig.retailerDisplayName as string | undefined;
    if (masterUrl) addToMasterList(masterUrl, masterName);

    log(session, "\n--- Summary ---");
    log(session, `  Retailer:         ${displayName} (${retailer})`);
    log(session, `  Discovery method: ${discoveryMethod}`);
    log(session, `  JSON-LD:          ${productPage.hasJsonLd ? "yes" : "no"} (${completeness})`);
    log(session, `  JS rendering:     ${requestConfig.requiresJsRendering ? "required" : "not required"}`);
    log(session, `  Recommendation:   ${overallRecommendation}`);
    log(session, `  Output:           configs/${retailer}.json`);
    log(session, "----------------\n");

    const metrics = await shutdownStagehand(stagehand);
    stagehand = null;
    log(session, "Done.");
    const estimatedUsd = metrics ? estimateUsdFromStagehandMetrics(metrics) : 0;
    log(session, `Estimated Stagehand cost (this run): ~$${estimatedUsd.toFixed(4)}`);

    return { config: finalConfig, metrics, estimatedUsd };
  } catch (err) {
    if (retailer && configsDir) {
      try {
        writeDegradedExploration(configsDir, retailer, displayName, baseUrl, err, {
          sitemapResult,
          categoryResult,
          blocksHeadlessBrowsers,
          productPageResult,
        });
        log(session, "Wrote degraded config and exploration artifacts after error.");
      } catch {
        // ignore secondary write failures
      }
    }
    await shutdownStagehand(stagehand);
    stagehand = null;
    const msg = err instanceof Error ? err.message : String(err);
    log(session, `Exploration failed: ${msg}`);
    if (err instanceof Error && err.stack) log(session, err.stack);
    throw err;
  } finally {
    clearTimeout(timeoutHandle);
  }
}

// ---------------------------------------------------------------------------
// CLI entrypoint
// ---------------------------------------------------------------------------

async function main() {
  const { url } = parseArgs();
  const { estimatedUsd } = await exploreRetailer(url, console.log);
  console.log(`Estimated cost (this run): ~$${estimatedUsd.toFixed(4)}`);
}

const isCLI = process.argv[1]?.includes("explore");
if (isCLI) {
  main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  });
}