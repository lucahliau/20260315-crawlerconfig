import path from "node:path";
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

function extractLocsFromXml(xml: string): string[] {
  const urls: string[] = [];
  const re = /<loc>\s*(.*?)\s*<\/loc>/gi;
  let match: RegExpExecArray | null;
  while ((match = re.exec(xml)) !== null) {
    const u = match[1].trim();
    if (u) urls.push(u);
  }
  return urls;
}

function isSitemapIndex(xml: string): boolean {
  return /<sitemapindex[\s>]/i.test(xml);
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

  log(`Fetching sitemap: ${sitemapUrl}`);
  const res = await fetchWithRetry(sitemapUrl);
  if (!res.ok) {
    log(`  Failed to fetch sitemap: HTTP ${res.status}`);
    return;
  }
  const xml = await res.text();

  if (isSitemapIndex(xml)) {
    const subSitemaps = extractLocsFromXml(xml);
    log(`  Sitemap index with ${subSitemaps.length} sub-sitemaps`);

    for (const subUrl of subSitemaps) {
      await delay(delayMs);
      log(`  Fetching sub-sitemap: ${subUrl}`);
      try {
        const subRes = await fetchWithRetry(subUrl);
        if (!subRes.ok) {
          log(`    Failed: HTTP ${subRes.status}`);
          continue;
        }
        const subXml = await subRes.text();
        const locs = extractLocsFromXml(subXml);
        for (const loc of locs) {
          if (pattern.test(loc)) addUrl(loc);
        }
        log(`    Extracted ${locs.length} URLs, ${urls.size} product URLs so far`);
        onProgress(urls.size);
      } catch (err) {
        log(`    Error fetching sub-sitemap: ${(err as Error).message}`);
      }
    }
  } else {
    const locs = extractLocsFromXml(xml);
    for (const loc of locs) {
      if (pattern.test(loc)) addUrl(loc);
    }
    log(`  Extracted ${locs.length} URLs, ${urls.size} match product pattern`);
    onProgress(urls.size);
  }
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
          const links = await page.evaluate((args: { sel: string; attr: string; base: string }) => {
              const els = document.querySelectorAll(args.sel);
              const hrefs: string[] = [];
              for (const el of els) {
                const val = el.getAttribute(args.attr);
                if (val) {
                  try {
                    hrefs.push(new URL(val, args.base).toString());
                  } catch { /* skip */ }
                }
              }
              return hrefs;
            }, extractArgs);

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
    for (const startUrl of startUrls) {
      for (let p = 0; p < maxPages; p++) {
        const pageNum = pageStartsAt + p * pageIncrement;
        const separator = startUrl.includes("?") ? "&" : "?";
        const pageUrl =
          p === 0 ? startUrl : `${startUrl}${separator}${paginationParam}=${pageNum}`;

        log(`  Page ${p + 1}/${maxPages}: ${pageUrl}`);
        try {
          const res = await fetchWithRetry(pageUrl);
          if (!res.ok) {
            log(`    HTTP ${res.status} — stopping pagination for this category`);
            break;
          }
          const html = await res.text();

          const linkRe = new RegExp(
            `<[^>]*${escapeRegex(extraction.selector.replace(/\[.*$/, ""))}[^>]*${escapeRegex(extraction.urlAttribute)}=["']([^"']+)["']`,
            "gi",
          );
          const before = urls.size;
          let linkMatch: RegExpExecArray | null;
          while ((linkMatch = linkRe.exec(html)) !== null) {
            let href = linkMatch[1];
            if (extraction.urlIsRelative) {
              try {
                href = new URL(href, config.baseUrl).toString();
              } catch {
                continue;
              }
            }
            if (pattern.test(href)) addUrl(href);
          }
          log(`    ${urls.size - before} new product URLs (${urls.size} total)`);
          onProgress(urls.size);

          if (urls.size === before && p > 0) {
            log(`    No new URLs found — stopping pagination for this category`);
            break;
          }
        } catch (err) {
          log(`    Error: ${(err as Error).message}`);
        }

        await delay(delayMs);
      }
    }
  }
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
        const links = await page.evaluate((args: { sel: string; attr: string; base: string }) => {
            const els = document.querySelectorAll(args.sel);
            const hrefs: string[] = [];
            for (const el of els) {
              const val = el.getAttribute(args.attr);
              if (val) {
                try {
                  hrefs.push(new URL(val, args.base).toString());
                } catch { /* skip */ }
              }
            }
            return hrefs;
          }, extractArgs);

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

  const { endpoint, method, headers, paginationParam, pageSize, productUrlTemplate, totalItemsPath } =
    config.discovery.api;
  const delayMs = effectiveDelay(config);

  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const separator = endpoint.includes("?") ? "&" : "?";
    const url = `${endpoint}${separator}${paginationParam}=${page}&limit=${pageSize}`;
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

      // Navigate to items array using a simple dot-path
      let items: unknown[] = [];
      if (Array.isArray(data)) {
        items = data;
      } else if (typeof data === "object" && data !== null) {
        // Try common paths: data.products, data.items, data.results
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
}

/** Periodic atomic writes to `product-urls/<retailer>.partial.json` (env: CRAWL_CHECKPOINT_EVERY_N, CRAWL_CHECKPOINT_EVERY_MS). */
function crawlCheckpointWriter(config: Config, urls: Set<string>): () => void {
  const everyN = parseInt(process.env.CRAWL_CHECKPOINT_EVERY_N ?? "0", 10);
  const everyMs = parseInt(process.env.CRAWL_CHECKPOINT_EVERY_MS ?? "0", 10);
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
    };
    writeJsonAtomic(path.join(dir, `${config.retailer}.partial.json`), partial);
    log(`  [checkpoint] ${n} URLs → product-urls/${config.retailer}.partial.json`);
  };
}

export async function crawlProductUrls(
  config: Config,
  onLog?: (msg: string) => void,
  onProgress?: (count: number) => void,
): Promise<CrawlResult> {
  if (onLog) _logFn = onLog;

  const urls = new Set<string>();
  let excludedNonClothing = 0;
  const addUrl = (u: string) => {
    if (isExcludedNonClothingProductUrl(u)) {
      excludedNonClothing++;
      return;
    }
    urls.add(u);
  };
  const runCheckpoint = crawlCheckpointWriter(config, urls);
  const progressCb = (count: number) => {
    (onProgress ?? (() => {}))(count);
    runCheckpoint();
  };

  log(`\nStarting product URL crawl for ${config.retailerDisplayName}`);
  log(`Discovery method: ${config.discovery.method}`);
  log(`Rate limit: ${effectiveDelay(config)}ms between requests`);

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
    log(`Crawl error: ${(err as Error).message}`);
  }

  const result: CrawlResult = {
    retailer: config.retailer,
    crawledAt: new Date().toISOString(),
    method: config.discovery.method,
    totalUrls: urls.size,
    urls: Array.from(urls).sort(),
  };

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
