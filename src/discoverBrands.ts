import "dotenv/config";
import { GoogleGenAI } from "@google/genai";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "./jsonFs.js";
import { estimateUsdFromDiscoverUsage, type DiscoverApiUsage } from "./pricing.js";

const DISCOVERED_BRANDS_PATH =
  process.env.DISCOVERED_BRANDS_PATH ?? path.join(process.cwd(), "discovered-brands.json");

/** Default model for brand discovery (Gemini API id, not Stagehand `google/...` form). */
const DEFAULT_GEMINI_DISCOVER_MODEL = "gemini-3-flash-preview";

/** JSON Schema for structured discover output (Gemini `responseJsonSchema`). */
const DISCOVER_BRANDS_RESPONSE_JSON_SCHEMA = {
  type: "object",
  properties: {
    brands: {
      type: "array",
      items: {
        type: "object",
        properties: {
          name: { type: "string" },
          url: { type: "string" },
        },
        required: ["name", "url"],
      },
    },
  },
  required: ["brands"],
} as const;

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

function parseBrandsFromPayload(parsed: unknown): { brands: DiscoveredBrand[] } | null {
  if (!parsed || typeof parsed !== "object") return null;
  const brands = (parsed as { brands?: unknown }).brands;
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
}

/** Fallback when the model returns markdown or multiple fences; structured JSON skips this path. */
function extractJsonFromResponse(text: string): { brands: DiscoveredBrand[] } | null {
  const candidates: string[] = [];
  const trimmed = text.trim();
  if (trimmed) candidates.push(trimmed);

  const fenceRe = /```(?:json)?\s*([\s\S]*?)```/g;
  let fm: RegExpExecArray | null;
  while ((fm = fenceRe.exec(text)) !== null) {
    const inner = fm[1].trim();
    if (inner) candidates.push(inner);
  }

  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start >= 0 && end > start) {
    candidates.push(text.slice(start, end + 1));
  }

  const tried = new Set<string>();
  for (const s of candidates) {
    if (tried.has(s)) continue;
    tried.add(s);
    try {
      const parsed = JSON.parse(s) as unknown;
      const p = parseBrandsFromPayload(parsed);
      if (p) return p;
    } catch {
      // try next candidate
    }
  }
  return null;
}

function extractBrandsFromText(text: string): { brands: DiscoveredBrand[]; rawText: string } | null {
  const parsed = extractJsonFromResponse(text);
  if (parsed) return { brands: parsed.brands, rawText: text };
  return null;
}

export function normalizeUrl(raw: string): string | null {
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
  /** Optional theme, region, or focus to prioritize in web search (e.g. "San Diego surf brands"). */
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
      ? `USER-SPECIFIED SEARCH FOCUS (prioritize this theme, region, or category in your web search):
"${category}"

Still apply every rule below: only brands that pass the STRICT DEFINITION, exclude everything in STRICT EXCLUSIONS and the exclusion list, and return 15-20 results matching the API JSON schema when possible.

`
      : "";

  const prompt = `You are a fashion researcher. Use web search to find 15-20 interesting CLOTHING and APPAREL brands that are NOT commoditized mass-market fashion.

"Not commoditized" means the brand has a real product identity and its own line of apparel—not interchangeable trend-factory mall fashion or pure aggregators. You do NOT need to find "niche" or underground labels only.

The API enforces a fixed JSON schema for your reply. Output only that JSON structure: do not wrap it in markdown, do not use code fences (no triple backticks), do not add explanations, and do not repeat or restart the answer mid-output—complete a single valid response.

${categoryBlock}STRICT DEFINITION — what counts as a valid result:
- The brand's PRIMARY business must be selling wearable apparel: tops, bottoms, outerwear, dresses, tailoring, knitwear, denim, activewear, technical/outdoor CLOTHING (jackets, pants, baselayers), footwear sold as fashion or apparel, hats/headwear, and fashion bags/backpacks when sold by a clothing label.
- The brand must be a clothing/apparel company first. It is NOT enough that they sell a few T-shirts alongside other product lines.
- Specialty sport and performance apparel brands are valid when apparel/footwear is a substantive part of their offering (e.g. Wilson tennis, running, cycling brands)—these are welcome even if they are well-known in their category.

STRICT EXCLUSIONS — do NOT include brands that are mainly any of the following (even if they also sell some apparel):
- Outdoor/camping/equipment retailers (tents, tent pegs, sleeping bags as gear, climbing hardware, general camp supplies)
- Art, prints, posters, wall decor, framing, or "gallery" shops
- Gift cards, e-gifts, vouchers, or digital-only gift products as a primary offering
- Home goods, furniture, kitchenware, bedding-only, candles, general lifestyle objects
- General merchandise, bookstores, music, electronics, beauty-only
- Big-box sporting goods megastores (chains that primarily resell many brands) — but apparel-first brand sites (e.g. wilson.com for Wilson) are OK
- Marketplaces or department stores (unless you can point to a single in-house apparel label as the brand)

STYLE PREFERENCES (among valid clothing brands only):
- Men-focused or unisex
- Strongly avoid commoditized fashion mass retailers and aggregators: ASOS, Shein-style ultra-fast fashion, and interchangeable fast-fashion chains (Zara, H&M, Uniqlo, etc.)
- Prefer brands with distinct identity: independents, heritage labels, workwear, technical apparel, sustainable/ethical lines, streetwear, Japanese/European specialty labels, and specialty sport/performance apparel (e.g. Wilson tennis)—not required to be obscure

${exclusionText}REQUIREMENTS:
- Return EXACTLY 15-20 brands (fewer is not acceptable)
- Every brand must satisfy the STRICT DEFINITION above; exclude anything that fails the STRICT EXCLUSIONS
- Each brand must have a valid homepage URL (https preferred) for a clothing/apparel brand
- Your reply must be one JSON object only, matching the schema: an object with a "brands" array of { "name", "url" } objects

Search the web, verify each candidate is primarily a clothing company, then output only the JSON object (no markdown, no code fences).`;

  onProgress("Searching the web for brands...");

  const ai = new GoogleGenAI({
    apiKey,
    httpOptions: { timeout: discoverTimeoutMs() },
  });

  // Google Search grounding + structured JSON (Gemini 3): https://ai.google.dev/gemini-api/docs/google-search
  const response = await ai.models.generateContent({
    model: discoverModelId(),
    contents: prompt,
    config: {
      tools: [{ googleSearch: {} }],
      maxOutputTokens: 8192,
      responseMimeType: "application/json",
      responseJsonSchema: DISCOVER_BRANDS_RESPONSE_JSON_SCHEMA,
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
