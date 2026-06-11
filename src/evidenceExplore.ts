/**
 * Evidence-based single-call AI explore.
 *
 * The legacy explore drives a browser with an LLM in the loop (Stagehand) —
 * many model calls, minutes of wall-clock, dollars per site, and flaky output.
 * This module replaces that for the common case with a fundamentally cheaper
 * shape: deterministically COLLECT evidence with plain HTTP (robots.txt,
 * sitemap samples, homepage + listing-page link inventories), make ONE Gemini
 * Flash call with a strict JSON response schema to interpret the evidence into
 * a crawl config, then deterministically VERIFY the result (fetch the sample
 * product URLs, check JSON-LD/OG; test the sitemap pattern) before accepting.
 *
 * Returns null when evidence is too thin or verification fails — the caller
 * falls back to the Stagehand browser explore (still the right tool for
 * heavily JS-rendered sites).
 */

import { GoogleGenAI } from "@google/genai";
import { extractLocsFromXml, fetchSitemapXml, rankSubSitemaps, extractAnchorHrefs } from "./crawl.js";
import {
  fetchWithTimeout,
  sampleUrlQuality,
  buildQualityBlocks,
  type UrlQualitySample,
} from "./platformExplore.js";
import { safeParseConfig } from "./schemas/config.js";
import { estimateUsdFromDiscoverUsage } from "./pricing.js";
import { recordExploreUsage } from "./usageLedger.js";

const DEFAULT_MODEL = "gemini-3.5-flash";

function exploreModelId(): string {
  const m = process.env.GEMINI_EXPLORE_MODEL?.trim();
  return m && m.length > 0 ? m : DEFAULT_MODEL;
}

// ---------------------------------------------------------------------------
// Evidence collection (all plain fetch, bounded)
// ---------------------------------------------------------------------------

interface Evidence {
  origin: string;
  robotsSitemaps: string[];
  /** Up to 60 URLs sampled from the most product-looking sitemap. */
  sitemapSampleUrls: string[];
  sitemapEntryUrl: string | null;
  /** Internal links found on the homepage (paths only, deduped). */
  homepageLinks: string[];
  /** The listing page we fetched (best category guess) and its internal links. */
  listingPageUrl: string | null;
  listingPageLinks: string[];
  listingHasRelNext: boolean;
  /** Heuristic: very few anchors in raw HTML usually means a JS-rendered SPA. */
  looksJsRendered: boolean;
}

const LISTING_HINT = /(shop|collection|categor|products|store|all|mens|womens|clothing|apparel)/i;

async function collectEvidence(url: string, onLog?: (m: string) => void): Promise<Evidence | null> {
  const log = (m: string) => onLog?.(m);
  let origin: string;
  try {
    origin = new URL(url).origin;
  } catch {
    return null;
  }

  // robots.txt → sitemap entries
  const robotsSitemaps: string[] = [];
  const robotsRes = await fetchWithTimeout(`${origin}/robots.txt`, "text/plain");
  if (robotsRes?.ok) {
    const text = await robotsRes.text();
    for (const m of text.matchAll(/^\s*sitemap:\s*(\S+)\s*$/gim)) robotsSitemaps.push(m[1]);
  }

  // Sample the most promising sitemap (one level of index indirection).
  let sitemapSampleUrls: string[] = [];
  let sitemapEntryUrl: string | null = null;
  for (const candidate of [...robotsSitemaps, `${origin}/sitemap.xml`].slice(0, 3)) {
    try {
      const xml = await fetchSitemapXml(candidate);
      if (!xml) continue;
      let urls: string[] = [];
      if (/<sitemapindex[\s>]/i.test(xml)) {
        const subs = rankSubSitemaps(extractLocsFromXml(xml));
        for (const sub of subs.slice(0, 2)) {
          const subXml = await fetchSitemapXml(sub).catch(() => null);
          if (subXml) urls.push(...extractLocsFromXml(subXml));
          if (urls.length > 200) break;
        }
      } else {
        urls = extractLocsFromXml(xml);
      }
      if (urls.length > 0) {
        sitemapEntryUrl = candidate;
        // Spread the sample across the list so multiple URL shapes show up.
        const step = Math.max(1, Math.floor(urls.length / 60));
        sitemapSampleUrls = urls.filter((_, i) => i % step === 0).slice(0, 60);
        break;
      }
    } catch {
      /* try next candidate */
    }
  }
  log(`Evidence: ${sitemapSampleUrls.length} sitemap sample URLs${sitemapEntryUrl ? ` from ${sitemapEntryUrl}` : ""}.`);

  // Homepage links
  const homeRes = await fetchWithTimeout(origin, "text/html");
  if (!homeRes || !homeRes.ok) return null;
  const homeHtml = await homeRes.text();
  const homepageLinks = extractAnchorHrefs(homeHtml, origin).slice(0, 150);
  const looksJsRendered = homepageLinks.length < 8 && /<script/i.test(homeHtml);

  // Fetch one listing-page candidate and inventory its links.
  let listingPageUrl: string | null = null;
  let listingPageLinks: string[] = [];
  let listingHasRelNext = false;
  const listingCandidate =
    homepageLinks.find((l) => LISTING_HINT.test(new URL(l).pathname) && !/\/(products?|p)\//.test(new URL(l).pathname)) ??
    null;
  if (listingCandidate) {
    const listRes = await fetchWithTimeout(listingCandidate, "text/html");
    if (listRes?.ok) {
      const listHtml = await listRes.text();
      listingPageUrl = listingCandidate;
      listingPageLinks = extractAnchorHrefs(listHtml, listingCandidate).slice(0, 150);
      listingHasRelNext = /rel\s*=\s*["']next["']/i.test(listHtml);
    }
  }
  log(
    `Evidence: ${homepageLinks.length} homepage links, listing page ${listingPageUrl ?? "none"} (${listingPageLinks.length} links)${looksJsRendered ? ", looks JS-rendered" : ""}.`,
  );

  return {
    origin,
    robotsSitemaps,
    sitemapSampleUrls,
    sitemapEntryUrl,
    homepageLinks,
    listingPageUrl,
    listingPageLinks,
    listingHasRelNext,
    looksJsRendered,
  };
}

// ---------------------------------------------------------------------------
// The single structured model call
// ---------------------------------------------------------------------------

/** Flat response shape (no oneOf — Gemini's schema support is happier this way). */
const RESPONSE_JSON_SCHEMA = {
  type: "object",
  properties: {
    method: { type: "string", enum: ["sitemap", "categoryPagination", "api", "none"] },
    confidence: { type: "string", enum: ["high", "medium", "low"] },
    notes: { type: "string" },
    sitemap: {
      type: "object",
      properties: {
        url: { type: "string" },
        productUrlPattern: { type: "string" },
      },
      required: ["url", "productUrlPattern"],
    },
    categoryPagination: {
      type: "object",
      properties: {
        startUrls: { type: "array", items: { type: "string" }, minItems: 1, maxItems: 6 },
        paginationParam: { type: "string" },
        pageStartsAt: { type: "number" },
        pageIncrement: { type: "number" },
        maxPages: { type: "number" },
        productUrlPattern: { type: "string" },
      },
      required: ["startUrls", "paginationParam", "pageStartsAt", "pageIncrement", "maxPages", "productUrlPattern"],
    },
    api: {
      type: "object",
      properties: {
        endpoint: { type: "string" },
        paginationParam: { type: "string" },
        pageSizeParam: { type: "string" },
        pageSize: { type: "number" },
        productUrlTemplate: { type: "string" },
        itemsPath: { type: "string" },
      },
      required: ["endpoint", "paginationParam", "pageSize", "productUrlTemplate"],
    },
    sampleProductUrls: { type: "array", items: { type: "string" }, minItems: 0, maxItems: 5 },
  },
  required: ["method", "confidence", "notes", "sampleProductUrls"],
} as const;

interface AiExploreAnswer {
  method: "sitemap" | "categoryPagination" | "api" | "none";
  confidence: "high" | "medium" | "low";
  notes: string;
  sitemap?: { url: string; productUrlPattern: string };
  categoryPagination?: {
    startUrls: string[];
    paginationParam: string;
    pageStartsAt: number;
    pageIncrement: number;
    maxPages: number;
    productUrlPattern: string;
  };
  api?: {
    endpoint: string;
    paginationParam: string;
    pageSizeParam?: string;
    pageSize: number;
    productUrlTemplate: string;
    itemsPath?: string;
  };
  sampleProductUrls: string[];
}

function buildPrompt(ev: Evidence, feedback?: string): string {
  const lines: string[] = [];
  lines.push(
    `You are configuring a polite product crawler for an e-commerce clothing site: ${ev.origin}`,
    ``,
    `Decide the BEST discovery method for enumerating ALL product-page URLs, using ONLY the evidence below (collected just now via plain HTTP). Do not invent URLs that are not consistent with the evidence.`,
    ``,
    `METHOD RULES (in preference order):`,
    `1. "sitemap" — choose when the sitemap sample contains product-page URLs. productUrlPattern must be a JavaScript regex SOURCE (no slashes) that matches the product URLs in the evidence and does NOT match category/blog/page URLs. Prefer simple path shapes like \\/products\\/[^/?#]+`,
    `2. "api" — only when the evidence clearly shows a public JSON product feed (do not guess endpoints).`,
    `3. "categoryPagination" — when there is no usable sitemap: startUrls = absolute listing-page URLs from the evidence (the main shop/category pages, max 6), paginationParam is the page query parameter (e.g. "page"), pageStartsAt usually 1, pageIncrement 1, maxPages 60. productUrlPattern as above, matching the product links visible on the listing page.`,
    `4. "none" — when the evidence is insufficient to configure any method confidently.`,
    ``,
    `Also return sampleProductUrls: 2-5 absolute product-page URLs taken VERBATIM from the evidence (used to verify your answer; an empty array is acceptable only with method "none").`,
  );
  if (feedback) {
    lines.push(``, `YOUR PREVIOUS ANSWER WAS REJECTED: ${feedback}`, `Fix exactly that problem.`);
  }
  lines.push(``, `EVIDENCE`, `========`);
  lines.push(`robots.txt sitemaps: ${ev.robotsSitemaps.length ? ev.robotsSitemaps.join(", ") : "(none)"}`);
  lines.push(
    ``,
    `Sitemap sample (${ev.sitemapSampleUrls.length} of the URLs in ${ev.sitemapEntryUrl ?? "n/a"}):`,
    ...ev.sitemapSampleUrls.map((u) => `  ${u}`),
  );
  lines.push(``, `Homepage internal links:`, ...ev.homepageLinks.slice(0, 120).map((u) => `  ${u}`));
  if (ev.listingPageUrl) {
    lines.push(
      ``,
      `Listing page ${ev.listingPageUrl} internal links${ev.listingHasRelNext ? " (page declares rel=next pagination)" : ""}:`,
      ...ev.listingPageLinks.slice(0, 120).map((u) => `  ${u}`),
    );
  }
  if (ev.looksJsRendered) {
    lines.push(``, `NOTE: the homepage HTML contains very few anchors — the site may be JS-rendered. Only configure a method you can support from this evidence; otherwise return "none".`);
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Assembly + verification
// ---------------------------------------------------------------------------

function assembleConfig(
  ev: Evidence,
  ans: AiExploreAnswer,
  identity: { retailer: string; displayName: string },
  samples: UrlQualitySample[],
): Record<string, unknown> | { error: string } {
  let discovery: Record<string, unknown>;
  let productLinkExtraction: Record<string, unknown> | undefined;
  if (ans.method === "sitemap") {
    if (!ans.sitemap) return { error: "method=sitemap but no sitemap block" };
    discovery = { method: "sitemap", sitemap: ans.sitemap };
  } else if (ans.method === "api") {
    if (!ans.api) return { error: "method=api but no api block" };
    discovery = { method: "api", api: { method: "GET", ...ans.api } };
  } else if (ans.method === "categoryPagination") {
    const cp = ans.categoryPagination;
    if (!cp) return { error: "method=categoryPagination but no categoryPagination block" };
    discovery = {
      method: "categoryPagination",
      categoryPagination: {
        startUrls: cp.startUrls,
        paginationParam: cp.paginationParam,
        pageStartsAt: cp.pageStartsAt,
        pageIncrement: cp.pageIncrement,
        maxPages: Math.min(cp.maxPages, 120),
      },
    };
    // The anchor-scan fallback in crawl.ts makes the selector almost decorative;
    // the productUrlPattern is what matters.
    productLinkExtraction = {
      selector: "a",
      urlAttribute: "href",
      productUrlPattern: cp.productUrlPattern,
      urlIsRelative: true,
    };
  } else {
    return { error: "model chose none" };
  }

  const quality = buildQualityBlocks(samples, 0);
  const config: Record<string, unknown> = {
    retailer: identity.retailer,
    retailerDisplayName: identity.displayName,
    baseUrl: ev.origin,
    discovery,
    ...(productLinkExtraction ? { productLinkExtraction } : {}),
    ...quality,
    requestConfig: {
      requiresJsRendering: ev.looksJsRendered,
      delayBetweenRequestsMs: 2000,
      blocksHeadlessBrowsers: false,
      notes: `Config produced by single-call evidence explore (confidence: ${ans.confidence}). ${ans.notes}`.trim(),
    },
    generatedBy: "evidence-explore",
  };
  const parsed = safeParseConfig(config);
  if (!parsed.success) {
    return { error: `schema validation failed: ${JSON.stringify(parsed.error.flatten().fieldErrors)}` };
  }
  return config;
}

async function verifyAnswer(ev: Evidence, ans: AiExploreAnswer, onLog?: (m: string) => void): Promise<
  | { ok: true; samples: UrlQualitySample[] }
  | { ok: false; reason: string }
> {
  const log = (m: string) => onLog?.(m);
  if (ans.method === "none") return { ok: false, reason: "model declined (method none)" };

  // The regex must compile and must match the model's own sample URLs.
  const patternSource =
    ans.method === "sitemap"
      ? ans.sitemap?.productUrlPattern
      : ans.method === "categoryPagination"
        ? ans.categoryPagination?.productUrlPattern
        : null;
  let pattern: RegExp | null = null;
  if (patternSource) {
    try {
      pattern = new RegExp(patternSource);
    } catch {
      return { ok: false, reason: `productUrlPattern is not a valid regex: ${patternSource}` };
    }
  }
  if (pattern && !ans.sampleProductUrls.some((u) => pattern.test(u))) {
    return { ok: false, reason: "productUrlPattern does not match any of your sampleProductUrls" };
  }
  if (ans.method === "sitemap" && pattern) {
    const hits = ev.sitemapSampleUrls.filter((u) => pattern.test(u)).length;
    if (hits === 0) {
      return { ok: false, reason: "productUrlPattern matches zero URLs from the sitemap evidence" };
    }
  }

  // Fetch up to 2 sample product pages and demand product signals.
  const samples: UrlQualitySample[] = [];
  for (const u of ans.sampleProductUrls.slice(0, 2)) {
    const s = await sampleUrlQuality(u).catch(() => null);
    if (s) {
      samples.push(s);
      log(`Verified ${s.url} — JSON-LD: ${s.hasJsonLd ? "yes" : "no"}, OG: ${s.hasOpenGraph ? "yes" : "no"}`);
    }
    await new Promise((r) => setTimeout(r, 400));
  }
  if (samples.length === 0) {
    return { ok: false, reason: "none of the sampleProductUrls could be fetched" };
  }
  if (!samples.some((s) => s.hasJsonLd || s.hasOpenGraph)) {
    return { ok: false, reason: "sample pages show neither product JSON-LD nor OG tags — likely not product pages" };
  }
  return { ok: true, samples };
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

export async function tryEvidenceExplore(
  url: string,
  identity: { retailer: string; displayName: string },
  onLog?: (msg: string) => void,
): Promise<Record<string, unknown> | null> {
  const log = (m: string) => onLog?.(m);
  const apiKey = process.env.GOOGLE_GENERATIVE_AI_API_KEY;
  if (!apiKey) return null;

  const ev = await collectEvidence(url, onLog);
  if (!ev) return null;
  // Nothing for the model to reason over → let the browser explore handle it.
  if (ev.sitemapSampleUrls.length === 0 && ev.homepageLinks.length < 5) {
    log("Evidence too thin for the single-call explore — deferring to browser exploration.");
    return null;
  }

  const ai = new GoogleGenAI({ apiKey, httpOptions: { timeout: 120_000 } });
  let feedback: string | undefined;
  const started = Date.now();
  let totalPrompt = 0;
  let totalCompletion = 0;

  for (let attempt = 1; attempt <= 2; attempt++) {
    log(`Asking ${exploreModelId()} to interpret the evidence (attempt ${attempt}/2)…`);
    let answer: AiExploreAnswer;
    try {
      const response = await ai.models.generateContent({
        model: exploreModelId(),
        contents: buildPrompt(ev, feedback),
        config: {
          // Gemini 3 Flash spends "thinking" tokens inside this budget; a tight
          // cap truncates the JSON mid-string. Keep generous headroom.
          maxOutputTokens: 8192,
          responseMimeType: "application/json",
          responseJsonSchema: RESPONSE_JSON_SCHEMA,
        },
      });
      totalPrompt += response.usageMetadata?.promptTokenCount ?? 0;
      totalCompletion += response.usageMetadata?.candidatesTokenCount ?? 0;
      try {
        answer = JSON.parse(response.text ?? "{}") as AiExploreAnswer;
      } catch (parseErr) {
        log(`Model returned invalid JSON (${(parseErr as Error).message}) — retrying.`);
        feedback = "your reply was not valid JSON; reply with ONLY the JSON object, complete and terminated";
        continue;
      }
    } catch (err) {
      log(`Evidence explore model call failed: ${(err as Error).message}`);
      if (attempt === 1) continue; // transient API failure — one more try
      return null;
    }

    const verdict = await verifyAnswer(ev, answer, onLog);
    if (!verdict.ok) {
      log(`Rejected: ${verdict.reason}`);
      feedback = verdict.reason;
      continue;
    }
    const config = assembleConfig(ev, answer, identity, verdict.samples);
    if ("error" in config) {
      log(`Rejected: ${config.error}`);
      feedback = config.error;
      continue;
    }

    const estimatedUsd = estimateUsdFromDiscoverUsage({
      input_tokens: totalPrompt,
      output_tokens: totalCompletion,
      web_search_requests: 0,
    });
    recordExploreUsage({
      retailer: identity.retailer,
      estimatedUsd,
      promptTokens: totalPrompt,
      completionTokens: totalCompletion,
      inferenceTimeMs: Date.now() - started,
    });
    log(`Evidence explore accepted (method: ${answer.method}, ~$${estimatedUsd.toFixed(4)}).`);
    return config;
  }
  log("Evidence explore could not produce a verifiable config — deferring to browser exploration.");
  return null;
}
