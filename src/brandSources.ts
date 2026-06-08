import "dotenv/config";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { writeJsonAtomic } from "./jsonFs.js";
import {
  convertToUsd,
  resolveCurrencyForUsd,
  roundUsdPrice,
} from "./currencyToUsd.js";
import { classifyPriceTier, type PriceTier } from "./priceProbe.js";

/**
 * Stockist / editorial brand-source mining — zero AI, near-free.
 *
 * Cool multi-brand boutiques curate exactly the kind of labels we want. Most run
 * Shopify, whose public `/products.json` exposes every product's `vendor` (the brand
 * name) plus variant prices. By mining a curated set of tasteful, accessible-tier
 * stockists and aggregating their `vendor` fields, we get dozens of high-signal brand
 * leads per run without a single LLM token: a brand carried by several cool shops, at
 * an accessible price, is a strong candidate.
 *
 * Output is a ranked list of brand-NAME leads (Shopify vendor fields don't include the
 * brand's own homepage), deduped against the master list, written to brand-leads.json.
 * A later cheap step (one Gemini call, or manual curation) resolves names -> URLs before
 * they enter the discover/explore/crawl pipeline.
 */

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";
const DEFAULT_TIMEOUT_MS = 15000;
const PAGES_PER_STOCKIST = 4;
const PAGE_LIMIT = 250;

const BRAND_LEADS_PATH =
  process.env.BRAND_LEADS_PATH ?? path.join(process.cwd(), "brand-leads.json");

export interface Stockist {
  /** Display name of the boutique. */
  name: string;
  /** Storefront origin (Shopify). */
  url: string;
}

/**
 * Curated cool, accessible-leaning multi-brand boutiques (mostly Shopify).
 * Non-Shopify or blocked stores are skipped gracefully (yield nothing).
 * EDIT THIS to steer which brands get surfaced.
 */
export const DEFAULT_STOCKISTS: Stockist[] = [
  { name: "Stag Provisions", url: "https://stagprovisions.com" }, // Austin men's, accessible
  { name: "Oi Polloi", url: "https://www.oipolloi.com" }, // Manchester, cool/accessible
  { name: "Goodhood", url: "https://goodhoodstore.com" }, // London
  { name: "Haven", url: "https://havenshop.com" }, // Canada
  { name: "Notre", url: "https://www.notre-shop.com" }, // Chicago
  { name: "Gentry NYC", url: "https://gentrynyc.com" }, // New York
  { name: "Lost & Found", url: "https://www.lostandfoundshop.com" }, // Rome
  { name: "Wallace Mercantile", url: "https://wallacemercantileshop.com" }, // Toronto
];

/**
 * Vendor names to drop before they become leads — pure noise for our taste:
 *  - GENERIC megabrands the discovery prompt already rejects (mall / mass sportswear), and
 *  - HOMEWARE / lifestyle labels that cool boutiques also stock (candles, furniture, ceramics).
 * Stored as normalized keys (see normalizeBrandKey). Edit to steer.
 */
const DENYLIST_KEYS = new Set<string>(
  [
    // generic / mass sportswear & fast fashion
    "nike", "jordan", "adidas", "vans", "converse", "puma", "reebok", "asics",
    "new balance", "under armour", "the north face", "champion", "fila",
    "zara", "h and m", "uniqlo", "shein", "asos", "mango", "gap", "levis",
    "levis premium", "polo ralph lauren", "tommy hilfiger", "calvin klein",
    // homeware / lifestyle / beauty (not apparel-first)
    "hay", "ferm living", "normann copenhagen", "diptyque", "puebco", "kinto",
    "areaware", "menu", "marshall", "aesop", "le labo", "byredo", "santa maria novella",
    "fellow", "fredericks and mae", "found feather",
  ].map((s) => s.replace(/[^a-z0-9]+/g, " ").trim()),
);

interface ShopifyVariant {
  price?: string | number;
}
interface ShopifyProduct {
  vendor?: string;
  variants?: ShopifyVariant[];
}

async function fetchWithTimeout(
  url: string,
  timeoutMs = DEFAULT_TIMEOUT_MS,
): Promise<Response | null> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": UA, Accept: "*/*" },
      redirect: "follow",
      signal: controller.signal,
    });
    return res.ok ? res : null;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

function originOf(rawUrl: string): string | null {
  try {
    return new URL(rawUrl).origin;
  } catch {
    return null;
  }
}

function minVariantPrice(p: ShopifyProduct): number | null {
  let min: number | null = null;
  for (const v of p.variants ?? []) {
    const amt = typeof v.price === "number" ? v.price : parseFloat(String(v.price ?? ""));
    if (Number.isFinite(amt) && amt > 0) {
      min = min === null ? amt : Math.min(min, amt);
    }
  }
  return min;
}

function median(nums: number[]): number {
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/** Normalize a brand/vendor name for dedup & self-vendor detection. */
export function normalizeBrandKey(name: string): string {
  return name
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "") // strip diacritics
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function shopifyStoreCurrency(origin: string): Promise<string> {
  const res = await fetchWithTimeout(`${origin}/cart.json`, 8000);
  if (!res) return "";
  try {
    const data = (await res.json()) as { currency?: unknown };
    return typeof data.currency === "string" ? data.currency.trim() : "";
  } catch {
    return "";
  }
}

/** Pull all products across a few pages of a Shopify storefront. */
async function fetchShopifyProducts(origin: string): Promise<ShopifyProduct[] | null> {
  const all: ShopifyProduct[] = [];
  for (let page = 1; page <= PAGES_PER_STOCKIST; page++) {
    const res = await fetchWithTimeout(
      `${origin}/products.json?limit=${PAGE_LIMIT}&page=${page}`,
    );
    if (!res) break;
    const ct = res.headers.get("content-type") ?? "";
    if (!ct.includes("json")) break;
    let data: { products?: ShopifyProduct[] };
    try {
      data = (await res.json()) as { products?: ShopifyProduct[] };
    } catch {
      break;
    }
    const products = data.products ?? [];
    if (products.length === 0) break;
    all.push(...products);
    if (products.length < PAGE_LIMIT) break; // last page
  }
  return all.length > 0 ? all : null;
}

/** Aggregated evidence for one mined vendor (= brand name). */
export interface VendorLead {
  name: string;
  /** USD-normalized representative (median of per-store medians). */
  usd: number;
  tier: PriceTier;
  /** Total products seen across all stockists. */
  productCount: number;
  /** Distinct cool stockists that carry this vendor (taste signal). */
  stockists: string[];
}

interface StockistMineResult {
  stockist: string;
  ok: boolean;
  vendorCount: number;
  productCount: number;
}

interface VendorAccumulator {
  name: string;
  productCount: number;
  perStoreUsd: number[];
  stockists: Set<string>;
}

export interface MineSummary {
  stockists: StockistMineResult[];
  leads: VendorLead[];
}

/**
 * Mine the curated stockists for brand-name leads. Zero AI.
 * Pre-filters to accessible/unknown tier and dedups against an optional known-name set.
 */
export async function mineStockists(opts?: {
  stockists?: Stockist[];
  /** Normalized brand keys already known (from the master list) to drop from results. */
  knownKeys?: Set<string>;
  /** Min distinct products required for a vendor to count (filters one-off accessories). */
  minProducts?: number;
  /** Keep only these tiers in the final leads (default: accessible + unknown). */
  keepTiers?: PriceTier[];
  onProgress?: (msg: string) => void;
}): Promise<MineSummary> {
  const stockists = opts?.stockists ?? DEFAULT_STOCKISTS;
  const knownKeys = opts?.knownKeys ?? new Set<string>();
  const minProducts = opts?.minProducts ?? 2;
  const keepTiers = new Set<PriceTier>(opts?.keepTiers ?? ["accessible", "unknown"]);
  const onProgress = opts?.onProgress ?? (() => {});

  const vendors = new Map<string, VendorAccumulator>();
  const stockistResults: StockistMineResult[] = [];

  for (const s of stockists) {
    const origin = originOf(s.url);
    if (!origin) {
      stockistResults.push({ stockist: s.name, ok: false, vendorCount: 0, productCount: 0 });
      continue;
    }
    onProgress(`Mining ${s.name}...`);
    const products = await fetchShopifyProducts(origin);
    if (!products) {
      onProgress(`  ${s.name}: not Shopify or blocked — skipped`);
      stockistResults.push({ stockist: s.name, ok: false, vendorCount: 0, productCount: 0 });
      continue;
    }
    const storeCurrency = await shopifyStoreCurrency(origin);
    const selfKey = normalizeBrandKey(s.name);

    // group this store's products by vendor, take per-vendor min-price median
    const byVendor = new Map<string, { name: string; prices: number[]; count: number }>();
    for (const p of products) {
      const vendorName = (p.vendor ?? "").trim();
      if (!vendorName) continue;
      const key = normalizeBrandKey(vendorName);
      if (!key) continue;
      // skip the store's own house label (exact or shared-prefix, e.g. "Goodhood Lifestore")
      if (key === selfKey || key.startsWith(selfKey + " ") || selfKey.startsWith(key + " ")) continue;
      const min = minVariantPrice(p);
      const g = byVendor.get(key) ?? { name: vendorName, prices: [], count: 0 };
      g.count++;
      if (min !== null) g.prices.push(min);
      byVendor.set(key, g);
    }

    for (const [key, g] of byVendor) {
      const repRaw = g.prices.length > 0 ? median(g.prices) : 0;
      const { iso } = resolveCurrencyForUsd(origin, repRaw, storeCurrency);
      const usd = repRaw > 0 ? roundUsdPrice(convertToUsd(repRaw, iso).usd) : 0;
      const acc =
        vendors.get(key) ??
        ({ name: g.name, productCount: 0, perStoreUsd: [], stockists: new Set<string>() } as VendorAccumulator);
      acc.productCount += g.count;
      if (usd > 0) acc.perStoreUsd.push(usd);
      acc.stockists.add(s.name);
      vendors.set(key, acc);
    }

    stockistResults.push({
      stockist: s.name,
      ok: true,
      vendorCount: byVendor.size,
      productCount: products.length,
    });
  }

  const leads: VendorLead[] = [];
  for (const [key, acc] of vendors) {
    if (knownKeys.has(key)) continue;
    if (DENYLIST_KEYS.has(key)) continue;
    if (acc.productCount < minProducts) continue;
    const usd = acc.perStoreUsd.length > 0 ? roundUsdPrice(median(acc.perStoreUsd)) : 0;
    const tier = usd > 0 ? classifyPriceTier(usd) : "unknown";
    if (!keepTiers.has(tier)) continue;
    leads.push({
      name: acc.name,
      usd,
      tier,
      productCount: acc.productCount,
      stockists: [...acc.stockists].sort(),
    });
  }

  // Rank: carried by more cool shops first, then accessible tier, then breadth.
  leads.sort((a, b) => {
    if (b.stockists.length !== a.stockists.length) return b.stockists.length - a.stockists.length;
    const at = a.tier === "accessible" ? 0 : 1;
    const bt = b.tier === "accessible" ? 0 : 1;
    if (at !== bt) return at - bt;
    return b.productCount - a.productCount;
  });

  return { stockists: stockistResults, leads };
}

export interface BrandLeadsFile {
  generatedAt: string;
  stockists: StockistMineResult[];
  leads: VendorLead[];
}

/** Mine + dedup against the master list + persist to brand-leads.json. */
export async function mineAndWriteLeads(opts?: {
  stockists?: Stockist[];
  onProgress?: (msg: string) => void;
}): Promise<BrandLeadsFile> {
  // Lazy import so this stays decoupled from discovery's hot path.
  const { listBrands } = await import("./discoverBrands.js");
  const knownKeys = new Set<string>();
  for (const b of listBrands()) {
    knownKeys.add(normalizeBrandKey(b.name));
    try {
      const host = new URL(b.url).hostname.replace(/^www\./, "").split(".")[0];
      if (host) knownKeys.add(normalizeBrandKey(host));
    } catch {
      /* ignore */
    }
  }

  const { stockists, leads } = await mineStockists({
    stockists: opts?.stockists,
    knownKeys,
    onProgress: opts?.onProgress,
  });

  const file: BrandLeadsFile = {
    generatedAt: new Date().toISOString(),
    stockists,
    leads,
  };
  writeJsonAtomic(BRAND_LEADS_PATH, file);
  return file;
}

// --- CLI: `tsx src/brandSources.ts [--write]` -------------------------------
const isMain =
  typeof process !== "undefined" &&
  !!process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const write = process.argv.includes("--write");
  const run = write
    ? mineAndWriteLeads({ onProgress: (m) => console.log(m) })
    : mineStockists({ onProgress: (m) => console.log(m) }).then((s) => ({
        generatedAt: new Date().toISOString(),
        stockists: s.stockists,
        leads: s.leads,
      }));

  run
    .then((file) => {
      console.log("\nStockists:");
      for (const s of file.stockists) {
        console.log(
          `  ${s.ok ? "✓" : "✗"} ${s.stockist} — ${s.vendorCount} vendors, ${s.productCount} products`,
        );
      }
      console.log(`\nTop brand leads (${file.leads.length} total):`);
      for (const l of file.leads.slice(0, 40)) {
        const price = l.usd > 0 ? `$${l.usd}` : "?";
        console.log(
          `  ${l.name} — ${price} (${l.tier}) · ${l.stockists.length} shop(s) · ${l.productCount} products`,
        );
      }
      if (write) console.log(`\nWrote ${BRAND_LEADS_PATH}`);
    })
    .catch((e) => {
      console.error("Mining failed:", e);
      process.exit(1);
    });
}
