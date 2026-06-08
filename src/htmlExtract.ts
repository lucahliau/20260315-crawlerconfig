/**
 * Pure, dependency-free HTML extraction helpers (regex-based, no DOM, no network, no SDKs).
 *
 * Kept separate from upload.ts so lightweight consumers (e.g. priceProbe.ts) can reuse the
 * exact same JSON-LD / meta parsing without pulling in the S3 + Stagehand dependency chain.
 */

export function extractJsonLdBlocks(html: string): Record<string, unknown>[] {
  const blocks: Record<string, unknown>[] = [];
  const re = /<script[^>]*type\s*=\s*["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match: RegExpExecArray | null;
  while ((match = re.exec(html)) !== null) {
    try {
      const parsed = JSON.parse(match[1].trim());
      blocks.push(parsed);
    } catch {
      /* malformed JSON-LD */
    }
  }
  return blocks;
}

export function findProductInJsonLd(
  blocks: Record<string, unknown>[],
): Record<string, unknown> | null {
  for (const block of blocks) {
    // Direct Product object
    const type = String(block["@type"] ?? "").toLowerCase();
    if (type.includes("product")) return block;

    // @graph array
    if (Array.isArray(block["@graph"])) {
      for (const item of block["@graph"] as Record<string, unknown>[]) {
        const itemType = String(item["@type"] ?? "").toLowerCase();
        if (itemType.includes("product")) return item;
        // Handle array types like ["Product", "ItemPage"]
        if (Array.isArray(item["@type"])) {
          const types = (item["@type"] as string[]).map((t) => t.toLowerCase());
          if (types.some((t) => t.includes("product"))) return item;
        }
      }
    }
  }
  return null;
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function extractMetaContent(html: string, property: string): string | null {
  // Match both property="..." and name="..."
  const re = new RegExp(
    `<meta[^>]*(?:property|name)\\s*=\\s*["']${escapeRegex(property)}["'][^>]*content\\s*=\\s*["']([^"']*)["']` +
      `|<meta[^>]*content\\s*=\\s*["']([^"']*)["'][^>]*(?:property|name)\\s*=\\s*["']${escapeRegex(property)}["']`,
    "i",
  );
  const m = html.match(re);
  if (m) return m[1] ?? m[2] ?? null;
  return null;
}

export function normalizeOffers(offers: unknown): Record<string, unknown>[] {
  if (!offers) return [];
  if (Array.isArray(offers)) return offers as Record<string, unknown>[];
  if (typeof offers === "object" && offers !== null) {
    const obj = offers as Record<string, unknown>;
    // AggregateOffer with offers array inside
    if (Array.isArray(obj.offers)) return obj.offers as Record<string, unknown>[];
    return [obj];
  }
  return [];
}
