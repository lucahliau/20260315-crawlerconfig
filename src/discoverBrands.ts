import "dotenv/config";
import { GoogleGenAI } from "@google/genai";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "./jsonFs.js";
import { estimateUsdFromDiscoverUsage, type DiscoverApiUsage } from "./pricing.js";

const DISCOVERED_BRANDS_PATH =
  process.env.DISCOVERED_BRANDS_PATH ?? path.join(process.cwd(), "discovered-brands.json");

/** Default model for niche-brand discovery (Gemini API id, not Stagehand `google/...` form). */
const DEFAULT_GEMINI_DISCOVER_MODEL = "gemini-2.5-flash";

function discoverModelId(): string {
  const m = process.env.GEMINI_DISCOVER_MODEL?.trim();
  return m && m.length > 0 ? m : DEFAULT_GEMINI_DISCOVER_MODEL;
}

/** Default 30m — grounding + search can exceed client defaults. */
const DEFAULT_DISCOVER_TIMEOUT_MS = 30 * 60 * 1000;

function discoverTimeoutMs(): number {
  const raw = process.env.DISCOVER_TIMEOUT_MS ?? process.env.GEMINI_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_DISCOVER_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_DISCOVER_TIMEOUT_MS;
  return Math.floor(n);
}

export interface DiscoveredBrand {
  name: string;
  url: string;
}

/** Appended when a discovery run finishes (see `discoverBrands`). */
export interface DiscoveryCountSnapshot {
  at: string;
  count: number;
}

interface MasterList {
  brands: DiscoveredBrand[];
  urls: string[];
  history?: DiscoveryCountSnapshot[];
}

function loadMasterList(): MasterList {
  try {
    const raw = fs.readFileSync(DISCOVERED_BRANDS_PATH, "utf-8");
    const data = JSON.parse(raw) as MasterList;
    const historyRaw = (data as { history?: unknown }).history;
    const history: DiscoveryCountSnapshot[] = Array.isArray(historyRaw)
      ? historyRaw.filter(
          (h): h is DiscoveryCountSnapshot =>
            !!h &&
            typeof h === "object" &&
            typeof (h as DiscoveryCountSnapshot).at === "string" &&
            typeof (h as DiscoveryCountSnapshot).count === "number" &&
            Number.isFinite((h as DiscoveryCountSnapshot).count),
        )
      : [];
    return {
      brands: Array.isArray(data.brands) ? data.brands : [],
      urls: Array.isArray(data.urls) ? data.urls : [],
      history,
    };
  } catch {
    return { brands: [], urls: [], history: [] };
  }
}

function saveMasterList(list: MasterList): void {
  writeJsonAtomic(DISCOVERED_BRANDS_PATH, list);
}

const masterQueue: Array<() => void> = [];
let masterQueueFlushing = false;

/** Serialize read–modify–write so concurrent add/save calls don't interleave. */
function enqueueMaster(fn: () => void): void {
  masterQueue.push(fn);
  if (masterQueueFlushing) return;
  masterQueueFlushing = true;
  try {
    while (masterQueue.length > 0) {
      const f = masterQueue.shift()!;
      f();
    }
  } finally {
    masterQueueFlushing = false;
  }
}

function extractJsonFromResponse(text: string): { brands: DiscoveredBrand[] } | null {
  const codeBlockMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const jsonStr = codeBlockMatch ? codeBlockMatch[1].trim() : text.trim();

  try {
    const parsed = JSON.parse(jsonStr) as { brands?: unknown };
    if (!parsed || typeof parsed !== "object") return null;
    const brands = parsed.brands;
    if (!Array.isArray(brands)) return null;

    const result: DiscoveredBrand[] = [];
    for (const b of brands) {
      if (b && typeof b === "object" && typeof b.name === "string" && typeof b.url === "string") {
        const url = b.url.trim();
        if (url.startsWith("http://") || url.startsWith("https://")) {
          result.push({ name: String(b.name).trim(), url });
        }
      }
    }
    return result.length > 0 ? { brands: result } : null;
  } catch {
    return null;
  }
}

function extractBrandsFromText(text: string): { brands: DiscoveredBrand[]; rawText: string } | null {
  const parsed = extractJsonFromResponse(text);
  if (parsed) return { brands: parsed.brands, rawText: text };
  return null;
}

function normalizeUrl(raw: string): string | null {
  let s = raw.trim();
  if (!s) return null;
  s = s.replace(/^["'<]+|["'>]+$/g, "");
  if (s.startsWith("//")) s = "https:" + s;
  if (!/^https?:\/\//i.test(s)) s = "https://" + s;
  try {
    const u = new URL(s);
    if (!u.pathname || u.pathname === "/") u.pathname = "/";
    return u.toString();
  } catch {
    return null;
  }
}

export function addToMasterList(url: string, name?: string): void {
  enqueueMaster(() => {
    const normalized = normalizeUrl(url);
    if (!normalized) return;
    const master = loadMasterList();
    const existingUrls = new Set([...master.urls, ...master.brands.map((b) => b.url)]);
    if (existingUrls.has(normalized)) return;
    const brand: DiscoveredBrand = { name: name ?? normalized, url: normalized };
    const updatedMaster: MasterList = {
      brands: [...master.brands, brand],
      urls: [...master.urls, normalized],
      history: master.history ?? [],
    };
    saveMasterList(updatedMaster);
  });
}

export function syncConfigsToMasterList(configsDir: string): void {
  try {
    if (!fs.existsSync(configsDir)) return;
    const files = fs.readdirSync(configsDir).filter((f) => f.endsWith(".json"));
    for (const filename of files) {
      try {
        const raw = fs.readFileSync(path.join(configsDir, filename), "utf-8");
        const config = JSON.parse(raw) as { baseUrl?: string; retailerDisplayName?: string };
        const baseUrl = config.baseUrl;
        if (baseUrl && typeof baseUrl === "string") {
          addToMasterList(baseUrl, config.retailerDisplayName);
        }
      } catch {
        // Skip invalid configs
      }
    }
  } catch {
    // Configs dir may not exist
  }
}

const MAX_DISCOVER_CATEGORY_LEN = 500;

/** Trim, cap length; returns undefined if empty or not a non-empty string. */
export function normalizeDiscoverCategory(raw: unknown): string | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "string") return undefined;
  const t = raw.trim();
  if (!t) return undefined;
  return t.length > MAX_DISCOVER_CATEGORY_LEN ? t.slice(0, MAX_DISCOVER_CATEGORY_LEN) : t;
}

export interface DiscoverBrandsOptions {
  /** Optional theme, region, or niche to prioritize in web search (e.g. "San Diego surf brands"). */
  category?: string;
}

function buildDiscoverUsage(args: {
  promptTokenCount: number;
  candidatesTokenCount: number;
  webSearchRequests: number;
}): DiscoverApiUsage {
  return {
    input_tokens: args.promptTokenCount,
    output_tokens: args.candidatesTokenCount,
    web_search_requests: args.webSearchRequests,
  };
}

export async function discoverBrands(
  onProgress: (msg: string) => void,
  /** Fired once with full model text (for SSE / collapsible UI). */
  onModelResponse?: (fullText: string) => void,
  options?: DiscoverBrandsOptions,
): Promise<{ brands: DiscoveredBrand[]; usage: DiscoverApiUsage; estimatedUsd: number }> {
  const apiKey = process.env.GOOGLE_GENERATIVE_AI_API_KEY;
  if (!apiKey || !apiKey.trim()) {
    throw new Error(
      "GOOGLE_GENERATIVE_AI_API_KEY is not set. Add it to your .env file (Google AI Studio).",
    );
  }

  const master = loadMasterList();
  const exclusionList = [...new Set([...master.urls, ...master.brands.map((b) => b.url)])];

  const exclusionText =
    exclusionList.length > 0
      ? `EXCLUSION LIST — do NOT include any brand whose URL or name appears here:\n${exclusionList.join("\n")}\n\n`
      : "";

  const category = normalizeDiscoverCategory(options?.category);
  const categoryBlock =
    category !== undefined
      ? `USER-SPECIFIED SEARCH FOCUS (prioritize this theme, region, or niche in your web search):
"${category}"

Still apply every rule below: only brands that pass the STRICT DEFINITION, exclude everything in STRICT EXCLUSIONS and the exclusion list, and return 15-20 results in the required JSON format when possible.

`
      : "";

  const prompt = `You are a fashion researcher. Use web search to find 15-20 cool, niche CLOTHING and APPAREL brands.

${categoryBlock}STRICT DEFINITION — what counts as a valid result:
- The brand's PRIMARY business must be selling wearable apparel: tops, bottoms, outerwear, dresses, tailoring, knitwear, denim, activewear, technical/outdoor CLOTHING (jackets, pants, baselayers), footwear sold as fashion or apparel, hats/headwear, and fashion bags/backpacks when sold by a clothing label.
- The brand must be a clothing/apparel company first. It is NOT enough that they sell a few T-shirts alongside other product lines.

STRICT EXCLUSIONS — do NOT include brands that are mainly any of the following (even if they also sell some apparel):
- Outdoor/camping/equipment retailers (tents, tent pegs, sleeping bags as gear, climbing hardware, general camp supplies)
- Art, prints, posters, wall decor, framing, or "gallery" shops
- Gift cards, e-gifts, vouchers, or digital-only gift products as a primary offering
- Home goods, furniture, kitchenware, bedding-only, candles, general lifestyle objects
- General merchandise, bookstores, music, electronics, beauty-only, or sporting goods stores where apparel is not the core business
- Marketplaces or department stores (unless you can point to a single in-house apparel label as the brand)

STYLE PREFERENCES (among valid clothing brands only):
- Men-focused or unisex
- NOT mainstream (avoid Nike, Zara, H&M, Uniqlo, ASOS, etc.)
- Independent, underground, or lesser-known
- Examples of niches: streetwear boutiques, sustainable/ethical brands, vintage-inspired, avant-garde, workwear, technical apparel, Japanese/European independents

${exclusionText}REQUIREMENTS:
- Return EXACTLY 15-20 brands (fewer is not acceptable)
- Every brand must satisfy the STRICT DEFINITION above; exclude anything that fails the STRICT EXCLUSIONS
- Each brand must have a valid homepage URL (https preferred) for a clothing/apparel brand
- Respond with ONLY a single JSON object—no markdown, no code blocks, no explanation before or after

Output format (copy this structure exactly):
{"brands":[{"name":"Brand Name","url":"https://example.com"},{"name":"Another Brand","url":"https://another.com"},...]}

Search the web, verify each candidate is primarily a clothing company, then respond with ONLY the JSON object as shown above.`;

  onProgress("Searching the web for niche brands...");

  const ai = new GoogleGenAI({
    apiKey,
    httpOptions: { timeout: discoverTimeoutMs() },
  });

  // Google Search grounding: https://ai.google.dev/gemini-api/docs/google-search
  const response = await ai.models.generateContent({
    model: discoverModelId(),
    contents: prompt,
    config: {
      tools: [{ googleSearch: {} }],
      maxOutputTokens: 4096,
    },
  });

  onProgress("Parsing results...");

  const text = response.text;
  const responseForUi = text ?? "(no text)";
  onModelResponse?.(responseForUi);

  const um = response.usageMetadata;
  const promptTokenCount = um?.promptTokenCount ?? 0;
  const candidatesTokenCount = um?.candidatesTokenCount ?? 0;

  const gm = response.candidates?.[0]?.groundingMetadata;
  let webSearchRequests = gm?.webSearchQueries?.length ?? 0;
  if (webSearchRequests === 0 && (gm?.groundingChunks?.length ?? 0) > 0) {
    webSearchRequests = 1;
  }

  onProgress(
    `Model response received (${text ? "text" : "empty"}). Expand the panel below to read the full message.`,
  );

  const result = text ? extractBrandsFromText(text) : null;

  if (!result || result.brands.length === 0) {
    onProgress("Failed to parse. Text received:\n" + responseForUi);
    throw new Error("Could not parse brands from model response. Please try again.");
  }

  onProgress(`Found ${result.brands.length} brands. Saving to master list...`);

  const seenUrls = new Set(master.urls);
  const newBrands: DiscoveredBrand[] = [];

  for (const b of result.brands) {
    const url = normalizeUrl(b.url);
    if (url && !seenUrls.has(url)) {
      seenUrls.add(url);
      newBrands.push({ name: b.name, url });
    }
  }

  const newTotal = master.brands.length + newBrands.length;
  const priorHistory = master.history ?? [];
  const updatedMaster: MasterList = {
    brands: [...master.brands, ...newBrands],
    urls: [...seenUrls],
    history: [...priorHistory, { at: new Date().toISOString(), count: newTotal }],
  };
  enqueueMaster(() => saveMasterList(updatedMaster));

  onProgress(`Done. Added ${newBrands.length} new brands.`);

  const usage = buildDiscoverUsage({
    promptTokenCount,
    candidatesTokenCount,
    webSearchRequests,
  });
  const estimatedUsd = estimateUsdFromDiscoverUsage(usage);
  onProgress(`Estimated API cost (this run): ~$${estimatedUsd.toFixed(4)}`);

  return { brands: result.brands, usage, estimatedUsd };
}
