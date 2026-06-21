/**
 * Resolve a trustworthy brand name for a scraped product.
 *
 * The scraped JSON-LD `brand` / Shopify `vendor` is usually the REAL brand — and
 * for multi-brand stockists (one store, many labels) it's the only correct source,
 * so we must not blindly overwrite it with the store's own name. But it sometimes
 * holds the distributor/operating company (Danton → "Bshop Co.,Ltd"), a gender or
 * category word (Mads Nørgaard → "Women", Community Clothing → "Accessories"), or a
 * region/collection variant (Drake's → "Drakes - Archive"). In those cases we fall
 * back to the crawl target's discovered name (config.retailerDisplayName).
 *
 * A trailing audience word is trimmed in place so brand fragmentation collapses
 * ("Won Hundred Men" / "Won Hundred Women" → "Won Hundred").
 */

const GENDER_WORD =
  /^(men|women|kid|kids|unisex|boy|boys|girl|girls|baby|babies|mens|womens|men['’]s|women['’]s|herren|damen|homme|femme)$/i;
const TRAILING_GENDER =
  /\s+(men|women|kids?|unisex|mens|womens|men['’]s|women['’]s|herren|damen|homme|femme)$/i;
const CATEGORY_WORD =
  /^(accessories|shirt|shirts|t-?shirts?|tops?|bottoms?|trousers|pants|knitwear|outerwear|footwear|shoes|bags?|hats?|caps?|socks?|sale|new in|new arrivals|home|gifts?|all|jewell?ery)$/i;
// Distributor / operating-company signatures. Deliberately NOT matching a bare
// "Co." (legit in real brand names like "Captain Fin Co.") — only entity suffixes.
const CORP_SUFFIX =
  /(\bco\.?\s*,?\s*ltd\b|\bcompany\s+limited\b|\bgmbh\b|\binc\.?\b|\bllc\b|\bltd\.?\b|\blimited\b|\bb\.?v\.?\b|\bs\.?r\.?l\.?\b|\bs\.?p\.?a\.?\b|\bk\.?k\.?\b|\bkabushiki\b|\bpty\b|\bsarl\b)/i;
const REGION_SPLIT = /\s[-–—]\s*(uk|usa?|eu|row|archive|sale|outlet|men|women|kids|mainline|store|online)\b/i;

/** True when a scraped brand is clearly NOT a consumer brand name. */
function isUntrustworthyBrand(s: string): boolean {
  return GENDER_WORD.test(s) || CATEGORY_WORD.test(s) || CORP_SUFFIX.test(s) || REGION_SPLIT.test(s);
}

export function sanitizeBrand(
  scraped: string | null | undefined,
  fallback: string | null | undefined,
): string | null {
  const fb = (fallback ?? "").trim();
  let s = (scraped ?? "").replace(/\s+/g, " ").trim();
  if (!s) return fb || null;

  // Collapse "Won Hundred Men" / "Won Hundred Women" → "Won Hundred" (but don't
  // reduce a bare "Men" to nothing — that's handled as untrustworthy below).
  const stripped = s.replace(TRAILING_GENDER, "").trim();
  if (stripped && stripped !== s && !GENDER_WORD.test(stripped)) s = stripped;

  if (isUntrustworthyBrand(s)) return fb || s;
  return s;
}
