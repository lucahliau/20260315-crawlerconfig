/**
 * Standard error codes used across the scraping pipeline. Use these instead of
 * inventing ad-hoc strings so the dashboard can group, filter, and explain
 * what's broken.
 *
 * Convention: SCREAMING_SNAKE_CASE, prefix indicates the stage:
 *   EXPLORE_*  — selector/config discovery
 *   CRAWL_*    — product URL discovery
 *   UPLOAD_*   — per-product fetch + DB write
 *   DISCOVER_* — brand discovery
 *   STORE_*    — persistence layer
 */
export const ErrorCodes = {
  // Generic
  UNKNOWN: "UNKNOWN",
  TIMEOUT: "TIMEOUT",
  NETWORK: "NETWORK",

  // Explore / Stagehand
  EXPLORE_BROWSER_INIT: "EXPLORE_BROWSER_INIT",
  EXPLORE_LLM_FAILED: "EXPLORE_LLM_FAILED",
  EXPLORE_NAV_TIMEOUT: "EXPLORE_NAV_TIMEOUT",
  EXPLORE_SELECTOR_MISS: "EXPLORE_SELECTOR_MISS",
  EXPLORE_NO_CATEGORIES: "EXPLORE_NO_CATEGORIES",
  EXPLORE_BLOCKED: "EXPLORE_BLOCKED",
  EXPLORE_CONFIG_INVALID: "EXPLORE_CONFIG_INVALID",
  EXPLORE_UNCAUGHT: "EXPLORE_UNCAUGHT",

  // Crawl
  CRAWL_HTTP_4XX: "CRAWL_HTTP_4XX",
  CRAWL_HTTP_5XX: "CRAWL_HTTP_5XX",
  CRAWL_NO_URLS_FOUND: "CRAWL_NO_URLS_FOUND",
  CRAWL_SITEMAP_PARSE: "CRAWL_SITEMAP_PARSE",
  CRAWL_PAGINATION_STUCK: "CRAWL_PAGINATION_STUCK",
  CRAWL_UNCAUGHT: "CRAWL_UNCAUGHT",

  // Upload (per-product)
  UPLOAD_FETCH_FAILED: "UPLOAD_FETCH_FAILED",
  UPLOAD_PARSE_FAILED: "UPLOAD_PARSE_FAILED",
  UPLOAD_IMAGE_FAILED: "UPLOAD_IMAGE_FAILED",
  UPLOAD_R2_FAILED: "UPLOAD_R2_FAILED",
  UPLOAD_DB_FAILED: "UPLOAD_DB_FAILED",
  UPLOAD_NO_IMAGES: "UPLOAD_NO_IMAGES",
  UPLOAD_BLOCKED: "UPLOAD_BLOCKED",
  UPLOAD_RATE_LIMITED: "UPLOAD_RATE_LIMITED",
  UPLOAD_UNCAUGHT: "UPLOAD_UNCAUGHT",

  // Discover brands
  DISCOVER_LLM_FAILED: "DISCOVER_LLM_FAILED",
  DISCOVER_NO_RESULTS: "DISCOVER_NO_RESULTS",
  DISCOVER_UNCAUGHT: "DISCOVER_UNCAUGHT",

  // Store
  STORE_WRITE_FAILED: "STORE_WRITE_FAILED",
} as const;

export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes];

/**
 * Classify an arbitrary Error into one of the codes above. Falls back to the
 * caller-supplied default. Pattern-matches on common messages from `pg`,
 * `node:fetch`, Stagehand/Playwright, and the AWS SDK.
 */
export function classifyError(err: unknown, fallback: ErrorCode = ErrorCodes.UNKNOWN): ErrorCode {
  const msg = (err instanceof Error ? err.message : String(err ?? "")).toLowerCase();
  if (!msg) return fallback;
  if (msg.includes("429") || msg.includes("rate limit") || msg.includes("too many requests"))
    return ErrorCodes.UPLOAD_RATE_LIMITED;
  if (msg.includes("timeout") || msg.includes("timed out")) return ErrorCodes.TIMEOUT;
  if (msg.includes("econnrefused") || msg.includes("enotfound") || msg.includes("network"))
    return ErrorCodes.NETWORK;
  if (msg.includes("403") || msg.includes("blocked") || msg.includes("captcha") || msg.includes("cloudflare"))
    return ErrorCodes.EXPLORE_BLOCKED;
  if (/\bhttp\s*4\d\d\b/.test(msg) || msg.includes("404") || msg.includes("400"))
    return ErrorCodes.CRAWL_HTTP_4XX;
  if (/\bhttp\s*5\d\d\b/.test(msg) || msg.includes("500") || msg.includes("502") || msg.includes("503"))
    return ErrorCodes.CRAWL_HTTP_5XX;
  return fallback;
}

/** Short, human-friendly explanations shown alongside the code in the UI. */
export const ErrorCodeDescriptions: Record<string, string> = {
  UNKNOWN: "Unclassified failure — see detail.",
  TIMEOUT: "Operation exceeded its time budget.",
  NETWORK: "Connection-level failure (DNS, refused, reset).",
  EXPLORE_BROWSER_INIT: "Could not start a Stagehand browser session.",
  EXPLORE_LLM_FAILED: "Gemini call inside Stagehand failed or returned junk.",
  EXPLORE_NAV_TIMEOUT: "Page never finished loading during explore.",
  EXPLORE_SELECTOR_MISS: "Inferred selector matched zero elements.",
  EXPLORE_NO_CATEGORIES: "Could not identify any product category links.",
  EXPLORE_BLOCKED: "Site blocked the crawler (CF / captcha / 403).",
  EXPLORE_CONFIG_INVALID: "Final config failed schema validation.",
  EXPLORE_UNCAUGHT: "Unhandled exception inside the explore loop.",
  CRAWL_HTTP_4XX: "Site returned a 4xx for a listing/sitemap request.",
  CRAWL_HTTP_5XX: "Site returned a 5xx for a listing/sitemap request.",
  CRAWL_NO_URLS_FOUND: "Crawl finished with zero product URLs.",
  CRAWL_SITEMAP_PARSE: "Sitemap XML failed to parse.",
  CRAWL_PAGINATION_STUCK: "Pagination loop stopped advancing.",
  CRAWL_UNCAUGHT: "Unhandled exception inside the crawl loop.",
  UPLOAD_FETCH_FAILED: "Product page fetch failed.",
  UPLOAD_PARSE_FAILED: "Could not extract product data from page.",
  UPLOAD_IMAGE_FAILED: "Image download/processing failed.",
  UPLOAD_R2_FAILED: "Cloudflare R2 upload rejected.",
  UPLOAD_DB_FAILED: "Postgres upsert failed.",
  UPLOAD_NO_IMAGES: "Product had no usable images.",
  UPLOAD_BLOCKED: "Product page returned 403 / captcha.",
  UPLOAD_RATE_LIMITED: "Site rate-limited the fetch (HTTP 429 / Retry-After).",
  UPLOAD_UNCAUGHT: "Unhandled exception during upload.",
  DISCOVER_LLM_FAILED: "Brand discovery LLM call failed.",
  DISCOVER_NO_RESULTS: "Brand discovery returned no usable brands.",
  DISCOVER_UNCAUGHT: "Unhandled exception inside discover-brands.",
  STORE_WRITE_FAILED: "Persistence layer write failed.",
};
