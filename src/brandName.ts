/**
 * Resolve a trustworthy brand name for a scraped product.
 *
 * The scraped JSON-LD `brand` / Shopify `vendor` is usually the REAL brand — and
 * for multi-brand stockists (one store, many labels) it's the only correct source,
 * so we must not blindly overwrite it with the store's own name. But it sometimes
 * holds the distributor/operating company (Danton → "Bshop Co.,Ltd"), a gender or
 * category word (Mads Nørgaard → "Women", Community Clothing → "Accessories"), or a
 * region/collection variant (Drake's → "Drakes - Archive"). In those cases we fall
 * back to the crawl target's discovered name.
 *
 * Two curated, per-retailer overrides sit on top of that heuristic:
 *  - UNIFY_BRAND: stores the user explicitly wants shown as a SINGLE brand for every
 *    item, even when the store lists several labels (e.g. J.Press's store also lists
 *    "Pennant"/"Jack Victor"). Add a retailer here only when the user names it.
 *  - CLEAN_NAME: a proper brand name for retailers whose discovered name is a mashed
 *    domain slug ("Madsnorgaard" → "Mads Nørgaard"). Used as the fallback name (so it
 *    only renames junk values, and at a genuine stockist leaves real labels alone).
 */

const GENDER_WORD =
  /^(men|women|kid|kids|unisex|boy|boys|girl|girls|baby|babies|mens|womens|men['’]s|women['’]s|herren|damen|homme|femme)$/i;
const TRAILING_GENDER =
  /\s+(men|women|kids?|unisex|mens|womens|men['’]s|women['’]s|herren|damen|homme|femme)$/i;
const CATEGORY_WORD =
  /^(accessories|shirt|shirts|t-?shirts?|tops?|bottoms?|trousers|pants|knitwear|outerwear|footwear|shoes|bags?|hats?|caps?|socks?|sale|new in|new arrivals|home|gifts?|all|jewell?ery)$/i;
const CORP_SUFFIX =
  /(\bco\.?\s*,?\s*ltd\b|\bcompany\s+limited\b|\bgmbh\b|\binc\.?\b|\bllc\b|\bltd\.?\b|\blimited\b|\bb\.?v\.?\b|\bs\.?r\.?l\.?\b|\bs\.?p\.?a\.?\b|\bk\.?k\.?\b|\bkabushiki\b|\bpty\b|\bsarl\b)/i;
const REGION_SPLIT = /\s[-–—]\s*(uk|usa?|eu|row|archive|sale|outlet|men|women|kids|mainline|store|online)\b/i;

/** Stores to show as ONE brand for every item (user-named; see "unify only stores I name"). */
export const UNIFY_BRAND: Record<string, string> = {
  jpressonline: "J. Press",
};

/** Proper brand names for retailers whose discovered name is a mashed domain slug. */
export const CLEAN_NAME: Record<string, string> = {
  madsnorgaard: "Mads Nørgaard",
  communityclothing: "Community Clothing",
  ministryofsupply: "Ministry of Supply",
  secondlayer: "SECOND/LAYER",
  ballandbuck: "Ball and Buck",
  nantucketreds: "Nantucket Reds",
  antonymorato: "Antony Morato",
  therealmccoys: "The Real McCoy's",
  doomsdaysociety: "Doomsday Society",
  bonnegueule: "BonneGueule",
  champdemanoeuvres: "Champ de Manœuvres",
  sattalivity: "Satta",
};

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

/**
 * Final brand for an item: per-retailer unify override → else sanitize the scraped
 * brand, falling back to the retailer's clean name (curated or discovered).
 */
export function resolveBrand(
  scraped: string | null | undefined,
  retailer: string | null | undefined,
  retailerDisplayName: string | null | undefined,
): string | null {
  const r = (retailer ?? "").trim().toLowerCase();
  if (UNIFY_BRAND[r]) return UNIFY_BRAND[r];
  const fallback = CLEAN_NAME[r] ?? retailerDisplayName;
  return sanitizeBrand(scraped, fallback);
}
