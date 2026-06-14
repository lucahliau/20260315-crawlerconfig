import fs from "node:fs";
import path from "node:path";
import { gunzipSync } from "node:zlib";
import { Stagehand } from "@browserbasehq/stagehand";
import { getStagehandModel } from "./stagehandModel.js";
import { getStagehandBrowserOptions } from "./stagehandConfig.js";
import type { Config } from "./schemas/config.js";
import { writeJsonAtomic } from "./jsonFs.js";

const USER_AGENT =
  "CrawlerConfigBot/1.0 (product-url-discovery; +https://github.com/example/crawler-config)";

const MIN_DELAY_MS = 1000;
const REQUEST_TIMEOUT_MS = 30_000;
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 2000;

// ---------------------------------------------------------------------------
// Logging — same swappable pattern as explore.ts
// ---------------------------------------------------------------------------

let _logFn: (msg: string) => void = console.log;

function log(...args: unknown[]): void {
  _logFn(args.map(String).join(" "));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function effectiveDelay(config: Config): number {
  return Math.max(config.requestConfig.delayBetweenRequestsMs, MIN_DELAY_MS);
}

/**
 * Heuristic filter: drop product URLs that are very unlikely to be apparel
 * (gift cards, art/prints, home & outdoor non-wearables, etc.).
 * Applied to every URL before it enters the crawl result set.
 */
export function isExcludedNonClothingProductUrl(rawUrl: string): boolean {
  let pathQuery: string;
  try {
    const u = new URL(rawUrl);
    pathQuery = `${u.pathname}${u.search}`.toLowerCase();
  } catch {
    return true;
  }

  const patterns: RegExp[] = [
    // Gift / prepaid / digital non-goods
    /gift[-_\s]?card/,
    /gift[-_\s]?cert/,
    /gift[-_\s]?voucher/,
    /\bgiftcards?\b/,
    /e[-_\s]?gift/,
    /\begift\b/,
    /(prepaid|store)[-_\s]?card/,
    // Art / wall decor (not apparel) — avoid broad "print/canvas" (hits screen-print tees, canvas sneakers)
    /\/art(\/|$|-)/,
    /fine[-_\s]?art/,
    /wall[-_\s]?art/,
    /\/prints(\/|$)/,
    /\/posters(\/|$)/,
    /art[-_\s]?(print|poster|work)/,
    /canvas[-_\s]?(print|wall|art)/,
    // Home / lifestyle non-apparel
    /homeware/,
    /home[-_\s]?decor/,
    /furniture/,
    /kitchenware/,
    /tableware/,
    /bedding/,
    /candle/,
    // Outdoor & camping gear (tent pegs, etc.) — not clothing SKUs
    /tent[-_\s]?peg/,
    /camping[-_\s]?(gear|equipment)/,
    /sleeping[-_\s]?bag/,
    /camp[-_\s]?chair/,
    /cooler\b/,
    /\bstove\b/,
    /\blantern\b/,
    // Misc non-apparel
    /(^|\/)(books?|music)(\/|$)/,
    /\bvinyl\b/,
    /electronics/,
    /subscription[-_\s]?box/,
    /digital[-_\s]?(download|product)/,
  ];

  return patterns.some((re) => re.test(pathQuery));
}

async function fetchWithRetry(
  url: string,
  opts: RequestInit = {},
): Promise<Response> {
  const controller = new AbortController();
  const headers: Record<string, string> = {
    "User-Agent": USER_AGENT,
    ...(opts.headers as Record<string, string> | undefined),
  };

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const res = await fetch(url, {
        ...opts,
        headers,
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (res.ok) return res;
      if (res.status === 429 || res.status >= 500) {
        const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1);
        log(`  HTTP ${res.status} on attempt ${attempt}/${MAX_RETRIES}, retrying in ${backoff}ms...`);
        await delay(backoff);
        continue;
      }
      return res;
    } catch (err) {
      clearTimeout(timeout);
      if (attempt === MAX_RETRIES) throw err;
      const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 1);
      log(`  Fetch error on attempt ${attempt}/${MAX_RETRIES}: ${(err as Error).message}, retrying in ${backoff}ms...`);
      await delay(backoff);
    }
  }
  throw new Error(`Failed to fetch ${url} after ${MAX_RETRIES} attempts`);
}

/** Decode the XML entities that actually appear in sitemap <loc> values. */
function decodeXmlEntities(s: string): string {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'");
}

export function extractLocsFromXml(xml: string): string[] {
  const urls: string[] = [];
  const re = /<loc>\s*(?:<!\[CDATA\[)?\s*(.*?)\s*(?:\]\]>)?\s*<\/loc>/gi;
  let match: RegExpExecArray | null;
  while ((match = re.exec(xml)) !== null) {
    const u = decodeXmlEntities(match[1].trim());
    if (u) urls.push(u);
  }
  return urls;
}

function isSitemapIndex(xml: string): boolean {
  return /<sitemapindex[\s>]/i.test(xml);
}

/** Fetch a sitemap URL, transparently gunzipping `.xml.gz` payloads. */
export async function fetchSitemapXml(url: string): Promise<string | null> {
  const res = await fetchWithRetry(url);
  if (!res.ok) return null;
  const buf = Buffer.from(await res.arrayBuffer());
  // Magic bytes beat file extensions: some servers serve gzip without .gz.
  if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) {
    try {
      return gunzipSync(buf).toString("utf-8");
    } catch {
      return null;
    }
  }
  return buf.toString("utf-8");
}

/**
 * Order sub-sitemaps so product ones come first and obvious non-product ones
 * (blogs, pages, collections) come last — most sites yield everything from the
 * product sitemaps, letting the crawl stop touching the rest.
 */
export function rankSubSitemaps(urls: string[]): string[] {
  const score = (u: string): number => {
    const l = u.toLowerCase();
    if (/product/.test(l)) return 0;
    if (/(blog|article|news|journal|page|post|category|collection|store-?locator)/.test(l)) return 2;
    return 1;
  };
  return [...urls].sort((a, b) => score(a) - score(b));
}

/** All same-origin anchor hrefs in an HTML document, resolved absolute. */
export function extractAnchorHrefs(html: string, baseUrl: string): string[] {
  const out = new Set<string>();
  const re = /<a\b[^>]*?href\s*=\s*("([^"]*)"|'([^']*)')/gi;
  let m: RegExpExecArray | null;
  let origin = "";
  try {
    origin = new URL(baseUrl).origin;
  } catch {
    return [];
  }
  while ((m = re.exec(html)) !== null) {
    const raw = decodeXmlEntities(m[2] ?? m[3] ?? "").trim();
    if (!raw || raw.startsWith("#") || raw.startsWith("mailto:") || raw.startsWith("tel:") || raw.startsWith("javascript:")) {
      continue;
    }
    try {
      const abs = new URL(raw, baseUrl);
      abs.hash = "";
      if (abs.origin === origin) out.add(abs.toString());
    } catch {
      /* skip malformed */
    }
  }
  return [...out];
}

/** href of `<link rel="next">` / `<a rel="next">`, if the page declares one. */
function extractRelNext(html: string, baseUrl: string): string | null {
  const m =
    html.match(/<link\b[^>]*rel\s*=\s*["']next["'][^>]*href\s*=\s*["']([^"']+)["']/i) ??
    html.match(/<link\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*rel\s*=\s*["']next["']/i) ??
    html.match(/<a\b[^>]*rel\s*=\s*["']next["'][^>]*href\s*=\s*["']([^"']+)["']/i);
  if (!m) return null;
  try {
    return new URL(decodeXmlEntities(m[1]), baseUrl).toString();
  } catch {
    return null;
  }
}

function maxCrawlUrls(): number {
  const raw = parseInt(process.env.CRAWL_MAX_URLS ?? "", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 50_000;
}

// ---------------------------------------------------------------------------
// robots.txt crawl-delay
// ---------------------------------------------------------------------------

async function getRobotsCrawlDelay(baseUrl: string): Promise<number> {
  try {
    const res = await fetchWithRetry(`${baseUrl}/robots.txt`);
    if (!res.ok) return 0;
    const text = await res.text();
    const match = text.match(/crawl-delay:\s*(\d+)/i);
    if (match) return parseInt(match[1], 10) * 1000;
  } catch {
    // ignore
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Strategy: Sitemap
// ---------------------------------------------------------------------------

async function crawlSitemap(
  config: Config,
  urls: Set<string>,
  onProgress: (count: number) => void,
  addUrl: (u: string) => void,
): Promise<void> {
  if (config.discovery.method !== "sitemap") return;

  const { url: sitemapUrl, productUrlPattern } = config.discovery.sitemap;
  const pattern = new RegExp(productUrlPattern);
  const delayMs = effectiveDelay(config);
  const maxUrls = maxCrawlUrls();
  const maxSubSitemaps = 60;
  const visited = new Set<string>();
  let fetched = 0;

  // Handles nested indexes (index → index → urlset) up to a small depth, with
  // product-named sub-sitemaps first so most runs never touch blog/page maps.
  const walk = async (url: string, depth: number): Promise<void> => {
    if (visited.has(url) || fetched >= maxSubSitemaps || urls.size >= maxUrls) return;
    visited.add(url);
    fetched++;
    if (fetched > 1) await delay(delayMs);
    log(`  Fetching sitemap${depth > 0 ? ` (depth ${depth})` : ""}: ${url}`);
    let xml: string | null;
    try {
      xml = await fetchSitemapXml(url);
    } catch (err) {
      log(`    Error: ${(err as Error).message}`);
      return;
    }
    if (!xml) {
      log(`    Failed to fetch or decode sitemap`);
      return;
    }

    if (isSitemapIndex(xml)) {
      if (depth >= 3) {
        log(`    Sitemap index nested deeper than 3 levels — skipping`);
        return;
      }
      const subs = rankSubSitemaps(extractLocsFromXml(xml));
      log(`    Sitemap index with ${subs.length} sub-sitemaps`);
      for (const sub of subs) {
        if (fetched >= maxSubSitemaps) {
          log(`    Reached sub-sitemap cap (${maxSubSitemaps}) — stopping`);
          break;
        }
        if (urls.size >= maxUrls) break;
        await walk(sub, depth + 1);
      }
      return;
    }

    const locs = extractLocsFromXml(xml);
    for (const loc of locs) {
      if (urls.size >= maxUrls) break;
      if (pattern.test(loc)) addUrl(loc);
    }
    log(`    ${locs.length} URLs in sitemap, ${urls.size} product URLs so far`);
    onProgress(urls.size);
  };

  await walk(sitemapUrl, 0);
  if (urls.size >= maxUrls) log(`  Reached CRAWL_MAX_URLS cap (${maxUrls}).`);
}

// ---------------------------------------------------------------------------
// Strategy: Category Pagination
// ---------------------------------------------------------------------------

async function crawlCategoryPagination(
  config: Config,
  urls: Set<string>,
  onProgress: (count: number) => void,
  addUrl: (u: string) => void,
): Promise<void> {
  if (config.discovery.method !== "categoryPagination") return;

  const { startUrls, paginationParam, pageStartsAt, pageIncrement, maxPages } =
    config.discovery.categoryPagination;
  const extraction = config.productLinkExtraction;
  const delayMs = effectiveDelay(config);
  const needsBrowser = config.requestConfig.requiresJsRendering;

  if (!extraction) {
    log("  No productLinkExtraction config — cannot extract links from category pages");
    return;
  }

  const pattern = new RegExp(extraction.productUrlPattern);

  if (needsBrowser) {
    log("  JS rendering required — using headless browser");
    const stagehand = new Stagehand({
      ...getStagehandBrowserOptions(),
      model: getStagehandModel(),
    });
    try {
      await stagehand.init();
      const page = stagehand.context.pages()[0];

      for (const startUrl of startUrls) {
        for (let p = 0; p < maxPages; p++) {
          const pageNum = pageStartsAt + p * pageIncrement;
          const separator = startUrl.includes("?") ? "&" : "?";
          const pageUrl =
            p === 0 ? startUrl : `${startUrl}${separator}${paginationParam}=${pageNum}`;

          log(`  Page ${p + 1}/${maxPages}: ${pageUrl}`);
          try {
            await page.goto(pageUrl, { waitUntil: "domcontentloaded" });
          } catch {
            log(`    Failed to load page, stopping pagination for this category`);
            break;
          }

          const extractArgs = { sel: extraction.selector, attr: extraction.urlAttribute, base: config.baseUrl };
          const links = await page.evaluate<string[]>(pageEvalExpr(collectLinksInPage, extractArgs));

          const before = urls.size;
          for (const link of links) {
            if (pattern.test(link)) addUrl(link);
          }
          log(`    Found ${links.length} links, ${urls.size - before} new product URLs`);
          onProgress(urls.size);

          if (links.length === 0) {
            log(`    No links found — stopping pagination for this category`);
            break;
          }

          await delay(delayMs);
        }
      }
    } finally {
      await stagehand.close();
    }
  } else {
    const maxUrls = maxCrawlUrls();
    for (const startUrl of startUrls) {
      let pageUrl: string | null = startUrl;
      let emptyStreak = 0;
      for (let p = 0; p < maxPages && pageUrl; p++) {
        if (urls.size >= maxUrls) {
          log(`  Reached CRAWL_MAX_URLS cap (${maxUrls}).`);
          return;
        }
        log(`  Page ${p + 1}/${maxPages}: ${pageUrl}`);
        let relNext: string | null = null;
        try {
          const res = await fetchWithRetry(pageUrl);
          if (!res.ok) {
            log(`    HTTP ${res.status} — stopping pagination for this category`);
            break;
          }
          const html = await res.text();
          relNext = extractRelNext(html, pageUrl);

          const before = urls.size;
          // First try the configured selector (cheap regex approximation)…
          const linkRe = new RegExp(
            `<[^>]*${escapeRegex(extraction.selector.replace(/\[.*$/, ""))}[^>]*${escapeRegex(extraction.urlAttribute)}=["']([^"']+)["']`,
            "gi",
          );
          let linkMatch: RegExpExecArray | null;
          while ((linkMatch = linkRe.exec(html)) !== null) {
            let href = linkMatch[1];
            try {
              href = new URL(href, config.baseUrl).toString();
            } catch {
              continue;
            }
            if (pattern.test(href)) addUrl(href);
          }
          // …and when it finds nothing, fall back to every anchor on the page.
          // The product URL pattern is the real filter; selectors drift as
          // sites redesign, and this keeps old configs working regardless.
          if (urls.size === before) {
            for (const href of extractAnchorHrefs(html, pageUrl)) {
              if (pattern.test(href)) addUrl(href);
            }
            if (urls.size > before) {
              log(`    Selector matched nothing; anchor-scan fallback found ${urls.size - before}`);
            }
          }
          log(`    ${urls.size - before} new product URLs (${urls.size} total)`);
          onProgress(urls.size);

          emptyStreak = urls.size === before ? emptyStreak + 1 : 0;
          if (emptyStreak >= 2) {
            log(`    No new URLs on 2 consecutive pages — stopping this category`);
            break;
          }
        } catch (err) {
          log(`    Error: ${(err as Error).message}`);
          break;
        }

        // Prefer the page's own rel=next (handles /page/2/ style paths);
        // otherwise synthesize the next page from the pagination params.
        if (relNext && relNext !== pageUrl) {
          pageUrl = relNext;
        } else {
          const pageNum = pageStartsAt + (p + 1) * pageIncrement;
          const separator = startUrl.includes("?") ? "&" : "?";
          pageUrl = `${startUrl}${separator}${paginationParam}=${pageNum}`;
        }
        await delay(delayMs);
      }
    }
  }
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Runs inside the browser page: collect hrefs via the configured selector, and
 * when it matches nothing (selector drift after a site redesign), fall back to
 * every anchor — the product URL pattern applied by the caller is the real gate.
 */
function collectLinksInPage(args: { sel: string; attr: string; base: string }): string[] {
  const out: string[] = [];
  const push = (val: string | null) => {
    if (!val) return;
    try {
      out.push(new URL(val, args.base).toString());
    } catch {
      /* skip */
    }
  };
  try {
    for (const el of document.querySelectorAll(args.sel)) push(el.getAttribute(args.attr));
  } catch {
    /* invalid selector */
  }
  if (out.length === 0) {
    for (const el of document.querySelectorAll("a[href]")) push(el.getAttribute("href"));
  }
  return out;
}

/**
 * Build a self-contained IIFE that runs `fn(arg)` inside the page DOM.
 *
 * We deliberately avoid Stagehand's `page.evaluate(fn, arg)` overload: it serializes
 * `fn` with `.toString()` and re-evaluates the text in the browser, but our prod
 * runtime (tsx → esbuild with `keepNames: true`) rewrites nested named functions as
 * `__name(fn, "name")` — a module-local helper that doesn't exist in the page. The
 * serialized source then throws `ReferenceError: __name is not defined`, which
 * Stagehand 3.1 reports opaquely as `StagehandEvalError: Uncaught` (it surfaces the
 * CDP `exceptionDetails.text` "Uncaught" instead of `.exception.description`).
 *
 * Passing a STRING expression skips Stagehand's serializer; the inline `__name` shim
 * satisfies the transpiled source. `arg` is JSON-injected, so `fn` must be
 * self-contained (no closed-over module references).
 */
function pageEvalExpr<A>(fn: (arg: A) => unknown, arg: A): string {
  return `(() => {
    const __name = (target) => target; // esbuild keepNames helper, absent in the browser
    const __fn = ${fn.toString()};
    return __fn(${JSON.stringify(arg)});
  })()`;
}

// ---------------------------------------------------------------------------
// Strategy: Infinite Scroll
// ---------------------------------------------------------------------------

async function crawlInfiniteScroll(
  config: Config,
  urls: Set<string>,
  onProgress: (count: number) => void,
  addUrl: (u: string) => void,
): Promise<void> {
  if (config.discovery.method !== "infiniteScroll") return;

  const { startUrls, scrollPauseMs, maxScrolls } = config.discovery.infiniteScroll;
  const extraction = config.productLinkExtraction;

  if (!extraction) {
    log("  No productLinkExtraction config — cannot extract links from scroll pages");
    return;
  }

  const pattern = new RegExp(extraction.productUrlPattern);

  const stagehand = new Stagehand({
    ...getStagehandBrowserOptions(),
    model: getStagehandModel(),
  });
  try {
    await stagehand.init();
    const page = stagehand.context.pages()[0];

    for (const startUrl of startUrls) {
      log(`  Loading: ${startUrl}`);
      try {
        await page.goto(startUrl, { waitUntil: "domcontentloaded" });
      } catch {
        log(`    Failed to load page, skipping`);
        continue;
      }

      for (let scroll = 0; scroll < maxScrolls; scroll++) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await delay(scrollPauseMs);

        const extractArgs = { sel: extraction.selector, attr: extraction.urlAttribute, base: config.baseUrl };
        const links = await page.evaluate<string[]>(pageEvalExpr(collectLinksInPage, extractArgs));

        const before = urls.size;
        for (const link of links) {
          if (pattern.test(link)) addUrl(link);
        }
        log(`    Scroll ${scroll + 1}/${maxScrolls}: ${urls.size - before} new URLs (${urls.size} total)`);
        onProgress(urls.size);

        if (urls.size === before && scroll > 2) {
          log(`    No new URLs after scroll — stopping`);
          break;
        }
      }
    }
  } finally {
    await stagehand.close();
  }
}

// ---------------------------------------------------------------------------
// Strategy: API
// ---------------------------------------------------------------------------

async function crawlApi(
  config: Config,
  urls: Set<string>,
  onProgress: (count: number) => void,
  addUrl: (u: string) => void,
): Promise<void> {
  if (config.discovery.method !== "api") return;

  const {
    endpoint,
    method,
    headers,
    paginationParam,
    pageSizeParam,
    pageSize,
    startPage,
    itemsPath,
    productUrlTemplate,
    totalItemsPath,
  } = config.discovery.api;
  const delayMs = effectiveDelay(config);
  const maxUrls = maxCrawlUrls();
  const sizeParam = pageSizeParam ?? "limit";

  let page = startPage ?? 1;
  let hasMore = true;

  while (hasMore && urls.size < maxUrls) {
    const separator = endpoint.includes("?") ? "&" : "?";
    const url = `${endpoint}${separator}${paginationParam}=${page}&${sizeParam}=${pageSize}`;
    log(`  API page ${page}: ${url}`);

    try {
      const res = await fetchWithRetry(url, {
        method: method.toUpperCase(),
        headers: headers ?? {},
      });

      if (!res.ok) {
        log(`    HTTP ${res.status} — stopping`);
        break;
      }

      const data = await res.json();

      // Navigate to the items array: explicit dot-path first, then common keys.
      let items: unknown[] = [];
      if (itemsPath) {
        let val: unknown = data;
        for (const part of itemsPath.split(".")) {
          val = (val as Record<string, unknown> | null)?.[part];
        }
        if (Array.isArray(val)) items = val;
      }
      if (items.length === 0 && Array.isArray(data)) {
        items = data;
      } else if (items.length === 0 && typeof data === "object" && data !== null) {
        for (const key of ["products", "items", "results", "data"]) {
          if (Array.isArray((data as Record<string, unknown>)[key])) {
            items = (data as Record<string, unknown>)[key] as unknown[];
            break;
          }
        }
      }

      if (items.length === 0) {
        log(`    No items returned — stopping`);
        break;
      }

      const before = urls.size;
      for (const item of items) {
        // Build URL from template, replacing {field} placeholders
        let productUrl = productUrlTemplate;
        if (typeof item === "object" && item !== null) {
          for (const [key, val] of Object.entries(item as Record<string, unknown>)) {
            productUrl = productUrl.replace(`{${key}}`, String(val));
          }
        } else {
          productUrl = productUrl.replace(/\{[^}]+\}/, String(item));
        }
        try {
          addUrl(new URL(productUrl, config.baseUrl).toString());
        } catch { /* skip */ }
      }
      log(`    ${urls.size - before} new product URLs (${urls.size} total)`);
      onProgress(urls.size);

      // Check total if path provided
      if (totalItemsPath) {
        const parts = totalItemsPath.split(".");
        let val: unknown = data;
        for (const p of parts) {
          val = (val as Record<string, unknown>)?.[p];
        }
        if (typeof val === "number" && urls.size >= val) {
          log(`    Reached total items (${val})`);
          hasMore = false;
        }
      }

      if (items.length < pageSize) {
        log(`    Received fewer items than page size — last page`);
        hasMore = false;
      }

      page++;
    } catch (err) {
      log(`    Error: ${(err as Error).message}`);
      break;
    }

    await delay(delayMs);
  }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

export interface CrawlResult {
  retailer: string;
  crawledAt: string;
  method: string;
  totalUrls: number;
  urls: string[];
  completed?: boolean;
  error?: string;
  resumedFromCheckpoint?: boolean;
}

export interface CrawlPersistenceHooks {
  initialUrls?: string[];
  onCheckpoint?: (partial: CrawlResult) => void | Promise<void>;
  onComplete?: (result: CrawlResult) => void | Promise<void>;
  onError?: (partial: CrawlResult, error: Error) => void | Promise<void>;
}

function parseCheckpointEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw == null || raw.trim() === "") return fallback;
  const parsed = parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/** Periodic atomic writes to `product-urls/<retailer>.partial.json` (env: CRAWL_CHECKPOINT_EVERY_N, CRAWL_CHECKPOINT_EVERY_MS). */
function crawlCheckpointWriter(
  config: Config,
  urls: Set<string>,
  hooks?: CrawlPersistenceHooks,
): () => void {
  const everyN = parseCheckpointEnv("CRAWL_CHECKPOINT_EVERY_N", 250);
  const everyMs = parseCheckpointEnv("CRAWL_CHECKPOINT_EVERY_MS", 30_000);
  const dir = process.env.PRODUCT_URLS_DIR ?? path.join(process.cwd(), "product-urls");
  let lastWrittenCount = 0;
  let lastWriteTime = Date.now();

  return () => {
    if (everyN <= 0 && everyMs <= 0) return;
    const n = urls.size;
    if (n === 0) return;
    const now = Date.now();
    const byN = everyN > 0 && n - lastWrittenCount >= everyN;
    const byMs = everyMs > 0 && now - lastWriteTime >= everyMs && n > lastWrittenCount;
    if (!byN && !byMs) return;
    lastWrittenCount = n;
    lastWriteTime = now;
    const partial: CrawlResult = {
      retailer: config.retailer,
      crawledAt: new Date().toISOString(),
      method: config.discovery.method,
      totalUrls: n,
      urls: Array.from(urls).sort(),
      completed: false,
      resumedFromCheckpoint: !!(hooks?.initialUrls && hooks.initialUrls.length > 0),
    };
    writeJsonAtomic(path.join(dir, `${config.retailer}.partial.json`), partial);
    if (hooks?.onCheckpoint) {
      Promise.resolve(hooks.onCheckpoint(partial)).catch((err) => {
        log(`  [checkpoint] persistence hook failed: ${(err as Error).message}`);
      });
    }
    log(`  [checkpoint] ${n} URLs → product-urls/${config.retailer}.partial.json`);
  };
}

export async function crawlProductUrls(
  config: Config,
  onLog?: (msg: string) => void,
  onProgress?: (count: number) => void,
  hooks?: CrawlPersistenceHooks,
): Promise<CrawlResult> {
  if (onLog) _logFn = onLog;
  if (!config?.discovery) {
    throw new Error(
      "Config must include a valid discovery block before crawling. Re-run explore or fix the saved config JSON.",
    );
  }

  const urls = new Set<string>(hooks?.initialUrls ?? []);
  let excludedNonClothing = 0;
  const addUrl = (u: string) => {
    if (isExcludedNonClothingProductUrl(u)) {
      excludedNonClothing++;
      return;
    }
    urls.add(u);
  };
  const runCheckpoint = crawlCheckpointWriter(config, urls, hooks);
  const progressCb = (count: number) => {
    (onProgress ?? (() => {}))(count);
    runCheckpoint();
  };

  log(`\nStarting product URL crawl for ${config.retailerDisplayName}`);
  log(`Discovery method: ${config.discovery.method}`);
  log(`Rate limit: ${effectiveDelay(config)}ms between requests`);
  if (urls.size > 0) {
    log(`Resuming crawl with ${urls.size} URL(s) already checkpointed`);
  }

  // Check robots.txt crawl-delay and use the larger value
  const robotsDelay = await getRobotsCrawlDelay(config.baseUrl);
  if (robotsDelay > 0 && robotsDelay > config.requestConfig.delayBetweenRequestsMs) {
    log(`robots.txt crawl-delay (${robotsDelay}ms) is higher than config delay — using ${robotsDelay}ms`);
    config = {
      ...config,
      requestConfig: {
        ...config.requestConfig,
        delayBetweenRequestsMs: robotsDelay,
      },
    };
  }

  let crawlError: Error | null = null;
  try {
    switch (config.discovery.method) {
      case "sitemap":
        await crawlSitemap(config, urls, progressCb, addUrl);
        break;
      case "categoryPagination":
        await crawlCategoryPagination(config, urls, progressCb, addUrl);
        break;
      case "infiniteScroll":
        await crawlInfiniteScroll(config, urls, progressCb, addUrl);
        break;
      case "api":
        await crawlApi(config, urls, progressCb, addUrl);
        break;
    }
  } catch (err) {
    crawlError = err instanceof Error ? err : new Error(String(err));
    log(`Crawl error: ${crawlError.message}`);
  }

  const result: CrawlResult = {
    retailer: config.retailer,
    crawledAt: new Date().toISOString(),
    method: config.discovery.method,
    totalUrls: urls.size,
    urls: Array.from(urls).sort(),
    completed: !crawlError,
    ...(crawlError ? { error: crawlError.message } : {}),
    resumedFromCheckpoint: !!(hooks?.initialUrls && hooks.initialUrls.length > 0),
  };

  const partialPath = path.join(
    process.env.PRODUCT_URLS_DIR ?? path.join(process.cwd(), "product-urls"),
    `${config.retailer}.partial.json`,
  );
  if (crawlError) {
    writeJsonAtomic(partialPath, result);
    if (hooks?.onError) {
      await hooks.onError(result, crawlError);
    }
  } else {
    try {
      if (fs.existsSync(partialPath)) fs.unlinkSync(partialPath);
    } catch {
      // ignore cleanup errors
    }
    if (hooks?.onComplete) {
      await hooks.onComplete(result);
    }
  }

  log(`\nCrawl complete: ${result.totalUrls} product URLs found`);
  if (excludedNonClothing > 0) {
    log(
      `  Excluded ${excludedNonClothing} URLs that look like non-clothing (e.g. gift cards, art, home & outdoor gear).`,
    );
  }

  // Reset log function
  _logFn = console.log;

  return result;
}
