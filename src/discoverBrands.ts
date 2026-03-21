import "dotenv/config";
import Anthropic from "@anthropic-ai/sdk";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "./jsonFs.js";
import { estimateUsdFromMessageUsage } from "./pricing.js";
import type { Usage } from "@anthropic-ai/sdk/resources/messages/messages.js";

const DISCOVERED_BRANDS_PATH =
  process.env.DISCOVERED_BRANDS_PATH ?? path.join(process.cwd(), "discovered-brands.json");

/** Default 30m — web search turns can exceed the SDK default (~10m) and trigger APIConnectionTimeoutError. */
const DEFAULT_ANTHROPIC_TIMEOUT_MS = 30 * 60 * 1000;

function anthropicTimeoutMs(): number {
  const raw = process.env.ANTHROPIC_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_ANTHROPIC_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_ANTHROPIC_TIMEOUT_MS;
  return Math.floor(n);
}

export interface DiscoveredBrand {
  name: string;
  url: string;
}

interface MasterList {
  brands: DiscoveredBrand[];
  urls: string[];
}

function loadMasterList(): MasterList {
  try {
    const raw = fs.readFileSync(DISCOVERED_BRANDS_PATH, "utf-8");
    const data = JSON.parse(raw) as MasterList;
    return {
      brands: Array.isArray(data.brands) ? data.brands : [],
      urls: Array.isArray(data.urls) ? data.urls : [],
    };
  } catch {
    return { brands: [], urls: [] };
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
  // Try to find JSON in markdown code block first
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

/**
 * Extracts brands JSON from the API response content blocks.
 *
 * With web search enabled, the response contains multiple content blocks
 * (tool_use, tool_result, text, etc.). The first text block is usually
 * preamble like "Let me search for brands…" — the actual JSON lives in
 * the *last* text block. We iterate from last to first and return the
 * first successful parse. As a final fallback we concatenate every text
 * block and try parsing that (handles the edge case where JSON is split
 * across blocks).
 */
function extractBrandsFromResponse(
  content: Anthropic.Messages.ContentBlock[],
): { brands: DiscoveredBrand[]; rawText: string } | null {
  const textBlocks = content
    .filter((b): b is Anthropic.Messages.TextBlock => b.type === "text")
    .map((b) => b.text);

  // Try each text block individually, starting from the last (most likely to contain JSON)
  for (let i = textBlocks.length - 1; i >= 0; i--) {
    const parsed = extractJsonFromResponse(textBlocks[i]);
    if (parsed) {
      return { brands: parsed.brands, rawText: textBlocks[i] };
    }
  }

  // Fallback: concatenate all text blocks and try parsing (handles split JSON)
  if (textBlocks.length > 1) {
    const combined = textBlocks.join("\n");
    const parsed = extractJsonFromResponse(combined);
    if (parsed) {
      return { brands: parsed.brands, rawText: combined };
    }
  }

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

export async function discoverBrands(
  onProgress: (msg: string) => void,
  /** Fired once with the concatenated Claude text blocks (for SSE / collapsible UI). */
  onClaudeResponse?: (fullText: string) => void,
): Promise<{ brands: DiscoveredBrand[]; usage: Usage; estimatedUsd: number }> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey || !apiKey.trim()) {
    throw new Error("ANTHROPIC_API_KEY is not set. Add it to your .env file.");
  }

  const master = loadMasterList();
  const exclusionList = [...new Set([...master.urls, ...master.brands.map((b) => b.url)])];

  const exclusionText =
    exclusionList.length > 0
      ? `EXCLUSION LIST — do NOT include any brand whose URL or name appears here:\n${exclusionList.join("\n")}\n\n`
      : "";

  const prompt = `You are a fashion researcher. Use web search to find 15-20 cool, niche CLOTHING and APPAREL brands.

STRICT DEFINITION — what counts as a valid result:
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

  const anthropic = new Anthropic({ apiKey, timeout: anthropicTimeoutMs() });

  const response = await anthropic.messages.create({
    model: "claude-sonnet-4-6",
    max_tokens: 4096,
    messages: [{ role: "user", content: prompt }],
    tools: [{ type: "web_search_20260209", name: "web_search" }],
  });

  onProgress("Parsing results...");

  // Log all content block types for debugging
  const blockTypes = response.content.map((b) => b.type).join(", ");
  onProgress(`Response contains ${response.content.length} blocks: [${blockTypes}]`);

  // Log full response every time for debugging
  const allTextBlocks = response.content
    .filter((b): b is Anthropic.Messages.TextBlock => b.type === "text")
    .map((b, i) => `--- text block ${i} ---\n${b.text}`)
    .join("\n\n");
  const responseForUi = allTextBlocks || "(no text blocks)";
  onClaudeResponse?.(responseForUi);
  onProgress("Claude response received (" + response.content.filter((b) => b.type === "text").length + " text block(s)). Expand the panel below to read the full message.");

  const result = extractBrandsFromResponse(response.content);

  if (!result || result.brands.length === 0) {
    // Log what we actually received to help debug
    const textBlocks = response.content
      .filter((b): b is Anthropic.Messages.TextBlock => b.type === "text")
      .map((b, i) => `--- text block ${i} ---\n${b.text}`)
      .join("\n");
    onProgress("Failed to parse. Text blocks received:\n" + textBlocks);
    throw new Error("Could not parse brands from Claude response. Please try again.");
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

  const updatedMaster: MasterList = {
    brands: [...master.brands, ...newBrands],
    urls: [...seenUrls],
  };
  enqueueMaster(() => saveMasterList(updatedMaster));

  onProgress(`Done. Added ${newBrands.length} new brands.`);

  const estimatedUsd = estimateUsdFromMessageUsage(response.usage);
  onProgress(`Estimated API cost (this run): ~$${estimatedUsd.toFixed(4)}`);

  return { brands: result.brands, usage: response.usage, estimatedUsd };
}