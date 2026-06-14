// ---------------------------------------------------------------------------
// classify.ts — canonical heuristic classifier for catalog items.
//
// Pure, dependency-free, fully unit-testable. Given an item's text signals
// (name, raw category/subcategory, URL, breadcrumbs, tags) it returns:
//   - isClothing : keep (wearable) vs hide (non-wearable: wallet, candle, tent…)
//   - gender     : controlled vocab { male, female, unisex, kids }
//   - productType: controlled vocab matching the app + backend
//   - confidence : 0..1
//   - signal     : short human-readable explanation (audit/tuning)
//
// Used by BOTH the forward upload path (src/upload.ts) and the one-time
// backfill (scripts/classify-catalog.ts), so cleanup and prevention share one
// source of truth. Design notes:
//
//   * Weighted MULTI-SOURCE scoring. The structured fields (subcategory,
//     category, breadcrumbs) are trusted more than free-text name tokens, and
//     the head noun of the name (its last tokens — product names end in the
//     product type) gets an extra boost.
//   * A wearable signal BEATS an incidental denylist mention. "Cat-O-Lantern
//     Sweater", "Shaggy Dog Cap", "Blanket Stripe Overshirt" stay clothing
//     because the garment word outweighs the lantern/dog/blanket token. We only
//     hide when the non-wearable evidence strictly outweighs the wearable
//     evidence — biased toward KEEPING so real clothing is never hidden.
//   * Gender uses a priority ladder (URL > breadcrumb/category > title >
//     garment-implied > retailer default > unisex). The garment-implied rule
//     (dress/skirt/blouse → female) is what stops dresses from defaulting to
//     unisex and leaking into men's feeds.
// ---------------------------------------------------------------------------

export type Gender = "male" | "female" | "unisex" | "kids";
export type ProductType = "tops" | "bottoms" | "jackets" | "bags" | "accessories" | "other";

export interface ClassifyInput {
  name?: string | null;
  category?: string | null;
  subcategory?: string | null;
  tags?: string[] | null;
  sourceUrl?: string | null;
  description?: string | null;
  /** Optional breadcrumb trail (e.g. from JSON-LD BreadcrumbList). */
  breadcrumbs?: string[] | null;
  retailer?: string | null;
  /**
   * Optional data-driven per-retailer gender fallback. The backfill computes
   * this from a first pass (the dominant signal-resolved gender of a
   * retailer's catalog) and feeds it back so single-gender brands with no
   * per-item gender signal resolve correctly instead of defaulting to unisex.
   */
  retailerDefaultGender?: Gender | null;
}

/** Where the resolved gender came from — lets callers trust explicit site
 * signals while ignoring weaker inferences when aggregating (e.g. computing a
 * per-retailer default must NOT fold in garment-implied guesses). */
export type GenderSource = "explicit" | "garment" | "retailer" | "mixed" | "none";

export interface Classification {
  isClothing: boolean;
  gender: Gender;
  genderSource: GenderSource;
  productType: ProductType;
  confidence: number;
  signal: string;
}

// ---------------------------------------------------------------------------
// Text normalization
// ---------------------------------------------------------------------------

/** Lowercase, decode common HTML entities, reduce to `[a-z0-9 ]`, collapse ws. */
export function norm(s: string | null | undefined): string {
  return (s ?? "")
    .toLowerCase()
    .replace(/&amp;/g, " and ")
    .replace(/&[a-z]+;/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function urlPathText(url: string | null | undefined): string {
  if (!url) return "";
  let path = url;
  try {
    path = new URL(url).pathname;
  } catch {
    // not an absolute URL — treat the whole thing as a path
  }
  return norm(path);
}

/** Last `n` tokens of a normalized string (the head noun of a product name). */
function tailTokens(normText: string, n: number): string {
  if (!normText) return "";
  const toks = normText.split(" ");
  return toks.slice(Math.max(0, toks.length - n)).join(" ");
}

/** Drop a trailing " - Color" / " | Variant" / " / Size" segment when it's short
 * (≤3 words) so the real product noun stays the head. Run on the RAW name
 * before norm() (which would erase the separators). "Coat Long Wallet - Tumbled
 * Black" → "Coat Long Wallet" so the head noun is "wallet", not "black". */
function stripVariantSuffix(raw: string): string {
  const m = raw.match(/^(.*?)\s*[-–—|/]\s*([^-–—|/]+)$/);
  if (m && m[1].trim() && m[2].trim().split(/\s+/).length <= 3) return m[1];
  return raw;
}

// ---------------------------------------------------------------------------
// Lexicons
// ---------------------------------------------------------------------------

type WearEntry = { re: RegExp; t: ProductType; w?: number };
type DenyEntry = { re: RegExp; w?: number };

function w(body: string): RegExp {
  // Word-boundary match against already-normalized (`[a-z0-9 ]`) text.
  return new RegExp(`\\b(?:${body})\\b`);
}

// Wearable garments / accessories → productType. Default entry weight 1.
const WEARABLE: WearEntry[] = [
  // explicit garment-category words (appear in breadcrumb paths like
  // "…/Clothing/Tops/…" and in the app's own taxonomy)
  { re: w("tops|topwear"), t: "tops" },
  { re: w("bottoms|bottomwear"), t: "bottoms" },
  { re: w("suiting|tailoring|suits?"), t: "jackets" },
  // tops
  { re: w("t ?shirts?|tees?|tee shirts?"), t: "tops" },
  { re: w("shirts?|overshirts?|button ?ups?|flannels?"), t: "tops" },
  { re: w("polos?|henleys?|rugby"), t: "tops" },
  { re: w("blouses?|tanks?|camisoles?|camis|bralettes?|bodysuits?|bras?"), t: "tops" },
  { re: w("sweaters?|jumpers?|sweatshirts?|hoodie|hoody|hoodies"), t: "tops" },
  { re: w("knit|knits|knitwear|pullovers?|cardigans?|turtlenecks?|crewnecks?|crew|jerseys?|thermals?|baselayers?"), t: "tops" },
  { re: w("tunics?|pyjamas?|pajamas?|nightwear|sleepwear|loungewear"), t: "tops" },
  // bottoms
  { re: w("pants?|trousers?|trouser|slacks?|chinos?|cords?|corduroys?"), t: "bottoms" },
  { re: w("jeans|denims?"), t: "bottoms" },
  { re: w("shorts?|skirts?|skorts?|culottes?|kilts?"), t: "bottoms" },
  { re: w("leggings?|joggers?|sweatpants?|sweats|trackpants?"), t: "bottoms" },
  { re: w("dungarees|overalls?|boxers?|briefs?|trunks?|underwear"), t: "bottoms" },
  // jackets / outerwear
  { re: w("jackets?|blazers?|coats?|parkas?|bombers?|anoraks?|windbreakers?|raincoats?|overcoats?|trench|peacoats?"), t: "jackets" },
  { re: w("gilets?|vests?|fleeces?|fleece|puffers?|shackets?|outerwear|cagoules?|waistcoats?"), t: "jackets" },
  { re: w("quarter ?zips?|full ?zips?|half ?zips?"), t: "jackets" },
  // shoes → accessories (the app has no `shoes` productType; matches backend convention)
  { re: w("shoes?|sneakers?|trainers?|boots?|loafers?|sandals?|heels?|mules?|slides?|oxfords?"), t: "accessories" },
  { re: w("derbys?|derbies|chukkas?|espadrilles?|clogs?|moccasins?|footwear|brogues?|plimsolls?|slippers?"), t: "accessories" },
  // bags
  { re: w("bags?|totes?|backpacks?|rucksacks?|pouches?|holdalls?|duffels?|duffles?|satchels?"), t: "bags" },
  { re: w("crossbody|messenger bags?|weekenders?|handbags?|purses?|briefcases?|musette"), t: "bags" },
  // accessories (worn)
  { re: w("hats?|caps?|beanies?|fedoras?|berets?|snapbacks?|visors?|bucket hats?"), t: "accessories" },
  { re: w("bandanas?|scarves|scarf|gaiters?|balaclavas?|headbands?|neck ?warmers?"), t: "accessories" },
  { re: w("belts?|suspenders|braces"), t: "accessories" },
  { re: w("ties?|neckties?|bow ?ties?|pocket squares?"), t: "accessories" },
  { re: w("gloves?|mittens?"), t: "accessories" },
  { re: w("socks?|legwear|hosiery"), t: "accessories" },
  { re: w("sunglasses|eyewear|glasses"), t: "accessories" },
  { re: w("watches?|watch|bracelets?|necklaces?|rings?|earrings?|cufflinks?|cuff links|jewelry|jewellery|brooch|brooches"), t: "accessories" },
  // generic clothing context (weak — keeps a plain "accessories"-categorised belt
  // alive, but not strong enough to override a clear non-wearable head noun)
  { re: w("accessor(?:y|ies)"), t: "accessories", w: 0.5 },
  { re: w("apparel|clothing|garments?|menswear|womenswear|fashion"), t: "other", w: 0.4 },
];

// Non-wearable products → hide (isClothing = false).
const NONWEARABLE: DenyEntry[] = [
  // "Hard" non-wearable nouns — these essentially never name a real garment, so
  // weight them high enough to beat a single bogus "outerwear"/"mens" category
  // that mis-scraped source data sometimes attaches to home/fragrance products.
  { re: w("wallets?|card ?holders?|card ?cases?|money ?clips?"), w: 2 },
  { re: w("keychains?|key ?chains?|key ?rings?|key ?fobs?|lanyards?|split rings?"), w: 1.5 },
  { re: w("candles?|incense|censers?|diffusers?|reed diffusers?|room sprays?|potpourri|vide poche"), w: 2 },
  { re: w("fragrances?|perfumes?|colognes?|eau de|after ?shave|shaving|pomade|air fresheners?|fresheners?|car scents?|scents?"), w: 2 },
  { re: w("soaps?|body wash|shower gels?|shampoos?|conditioners?|skin ?care|moisturis(?:er|ers)|moisturiz(?:er|ers)|lip balms?|grooming|deodorants?"), w: 1.5 },
  { re: w("phone ?cases?|airpods?|earbuds?|chargers?|cables?") },
  { re: w("mugs?|tumblers?|flasks?|bottles?|cups?|glasses ware|glassware|kettles?|pans?|cookware|cook ?sets?|dinnerware|cutlery|chopsticks?|coasters?") },
  { re: w("cushions?|pillows?|blankets?|throws?|towels?|homeware|rugs?|bedding|duvets?") },
  { re: w("notebooks?|stationery|pens?|pencils?|magazines?|publications?|posters?|art prints?|print frames?|framed (?:art|prints?)|books?|zines?") },
  { re: w("gift ?cards?|vouchers?|e ?gift") },
  { re: w("stickers?|embroidered patch|embroidered patches|iron ?on patch|iron ?on patches|patches|pin badges?|enamel pins?|magnets?|decals?") },
  { re: w("vitamins?|supplements?|protein") },
  { re: w("tents?|tarps?|grills?|griddles?|fireplace|firepits?|stoves?|burners?|camp furniture|furniture|tables?|chairs?|coolers?|lanterns?|cook ?stoves?|containers?|storage boxes?") },
  // camping / outdoor hardware (snowpeak-style) — clearly non-wearable
  { re: w("stakes?|pegs?|tent pegs?|mallets?|hammers?|poles?|guy ?lines?|carabiners?|canopy|canopies|gazebos?|hammocks?|cots?|groundsheets?|awnings?|sleeping bags?|sleeping mats?|bivvy|bivy") },
  { re: w("dog (?:collars?|beds?|toys?|bowls?|leashes?|harness(?:es)?)|pet|pets|cat (?:beds?|toys?)") },
  { re: w("coffee|teas?|snacks?|chocolate|candy|sauce|seasoning|spice") },
  { re: w("umbrellas?|tote boxes?|trays?") },
];

// Strong female-gendered garments (drive the dress/skirt-leak fix).
// `dress` is guarded against menswear adjective uses ("dress shirt/pants/shoes").
const FEMALE_GARMENT = w("skirts?|skorts?|blouses?|camisoles?|bralettes?|bras?|bodysuits?|leotards?|jumpsuits?|rompers?|playsuits?|peplum|sundress(?:es)?|maxi dress(?:es)?");
const DRESS = /\bdress(?:es)?\b/;
const DRESS_ADJ = /\bdress(?:es)?\s+(?:shirt|shirts|pant|pants|trouser|trousers|sock|socks|shoe|shoes|boot|boots|belt|belts|code|sneaker|sneakers|watch|watches)\b/;
const GOWN = /\bgowns?\b/;
const DRESSING_GOWN = /\bdressing gowns?\b/;
const MALE_GARMENT = w("boxer briefs?|boxer shorts?");

// Gender word tokens (deliberately omit bare "man"/"w"/"m" — too noisy / brand-laden).
const G_FEMALE = w("women|womens|woman|womans|ladies|lady|wmn|wmns|female|womenswear");
const G_MALE = w("men|mens|male|gents|gentlemen|menswear");
const G_KIDS = w("kids|kid|child|children|childrens|boys|boy|girls|girl|junior|juniors|youth|toddler|toddlers|baby|infant");
const G_UNISEX = w("unisex|all gender|gender ?neutral");

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

type Source = { text: string; weight: number; label: string };

function buildSources(input: ClassifyInput): Source[] {
  const name = norm(input.name);
  const sources: Source[] = [
    { text: norm(input.subcategory), weight: 3, label: "subcategory" },
    { text: norm(input.category), weight: 2.5, label: "category" },
    { text: norm((input.breadcrumbs ?? []).join(" ")), weight: 2, label: "breadcrumb" },
    { text: name, weight: 1.5, label: "name" },
    { text: tailTokens(norm(stripVariantSuffix(input.name ?? "")), 3), weight: 1.5, label: "name-head" },
    { text: urlPathText(input.sourceUrl), weight: 1, label: "url" },
    { text: norm((input.tags ?? []).join(" ")), weight: 0.8, label: "tags" },
    { text: norm(input.description), weight: 0.4, label: "description" },
  ];
  return sources.filter((s) => s.text.length > 0);
}

function classifyClothing(sources: Source[]): {
  isClothing: boolean;
  productType: ProductType;
  wearScore: number;
  nonwearScore: number;
} {
  let wearScore = 0;
  let nonwearScore = 0;
  const typeVotes: Record<ProductType, number> = {
    tops: 0,
    bottoms: 0,
    jackets: 0,
    bags: 0,
    accessories: 0,
    other: 0,
  };

  for (const src of sources) {
    for (const e of WEARABLE) {
      if (e.re.test(src.text)) {
        const score = src.weight * (e.w ?? 1);
        wearScore += score;
        typeVotes[e.t] += score;
      }
    }
    for (const e of NONWEARABLE) {
      if (e.re.test(src.text)) {
        nonwearScore += src.weight * (e.w ?? 1);
      }
    }
  }

  // Bias toward KEEP: hide only when non-wearable evidence strictly outweighs
  // wearable evidence and clears a small floor. Unknown (no signal) → keep.
  const isClothing = !(nonwearScore > wearScore && nonwearScore >= 1.0);

  // productType = strongest wearable vote (ignoring the generic "other" bucket
  // unless nothing else fired).
  let productType: ProductType = "other";
  if (isClothing) {
    let best = 0;
    for (const t of ["tops", "bottoms", "jackets", "bags", "accessories"] as ProductType[]) {
      if (typeVotes[t] > best) {
        best = typeVotes[t];
        productType = t;
      }
    }
  }

  return { isClothing, productType, wearScore, nonwearScore };
}

function impliesFemale(sources: Source[]): boolean {
  for (const s of sources) {
    if (FEMALE_GARMENT.test(s.text)) return true;
    if (DRESS.test(s.text) && !DRESS_ADJ.test(s.text)) return true;
    if (GOWN.test(s.text) && !DRESSING_GOWN.test(s.text)) return true;
  }
  return false;
}

function impliesMale(sources: Source[]): boolean {
  return sources.some((s) => MALE_GARMENT.test(s.text));
}

function classifyGender(input: ClassifyInput, sources: Source[]): { gender: Gender; reason: string; source: GenderSource } {
  // Explicit, site-stated gender (from gender WORDS in url/category/title).
  // Tracked separately from garment-implied inference so that a per-retailer
  // default can be derived from explicit evidence only.
  let male = 0;
  let female = 0;
  let kids = 0;
  let unisex = 0;

  // URL path is the most reliable gender signal — weight it heavily.
  for (const s of sources) {
    const sw = s.label === "url" ? s.weight * 5 : s.label === "category" || s.label === "breadcrumb" ? s.weight * 1.6 : s.weight;
    if (G_FEMALE.test(s.text)) female += sw;
    if (G_MALE.test(s.text)) male += sw;
    if (G_KIDS.test(s.text)) kids += sw;
    if (G_UNISEX.test(s.text)) unisex += sw;
  }

  const EXPLICIT = 2.5; // minimum score to claim an explicit gender
  const scores: Array<[Gender, number]> = [
    ["male", male],
    ["female", female],
    ["kids", kids],
  ];
  scores.sort((a, b) => b[1] - a[1]);
  const [topG, topS] = scores[0];
  const secondS = scores[1][1];

  // Explicit men's+women's both present and close → genuinely mixed → unisex.
  if (topS >= EXPLICIT && secondS >= EXPLICIT && Math.abs(topS - secondS) < 1.5 && (topG === "male" || topG === "female")) {
    return { gender: "unisex", reason: "mixed men's+women's", source: "mixed" };
  }
  // Clear explicit winner.
  if (topS >= EXPLICIT && topS - secondS >= 0.5) {
    return { gender: topG, reason: `explicit ${topG} (${topS.toFixed(1)})`, source: "explicit" };
  }

  // No clear explicit signal — fall back through weaker evidence.
  // Garment-implied gender is the dress/skirt-leak fix: a dress/skirt/blouse
  // resolves female instead of leaking into men's feeds as "unisex". It is
  // deliberately NOT treated as "explicit" (callers exclude it from retailer
  // aggregation), and it still yields to a strong explicit male signal above.
  const femImplied = impliesFemale(sources);
  const maleImplied = impliesMale(sources);
  if (femImplied && male < EXPLICIT) return { gender: "female", reason: "female garment", source: "garment" };
  if (maleImplied && female < EXPLICIT) return { gender: "male", reason: "male garment", source: "garment" };
  if (unisex >= EXPLICIT) return { gender: "unisex", reason: "explicit unisex", source: "explicit" };
  if (input.retailerDefaultGender) {
    return { gender: input.retailerDefaultGender, reason: "retailer default", source: "retailer" };
  }
  return { gender: "unisex", reason: "no gender signal", source: "none" };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Normalize a legacy/raw gender string to the controlled vocab, or null. */
export function normalizeGender(raw: string | null | undefined): Gender | null {
  const g = (raw ?? "").trim().toLowerCase();
  if (!g) return null;
  if (["male", "men", "mens", "man", "men's"].includes(g)) return "male";
  if (["female", "women", "womens", "woman", "women's", "ladies"].includes(g)) return "female";
  if (["kids", "kid", "children", "boys", "girls", "youth"].includes(g)) return "kids";
  if (["unisex", "all", "everyone"].includes(g)) return "unisex";
  return null;
}

export function classifyItem(input: ClassifyInput): Classification {
  const sources = buildSources(input);

  if (sources.length === 0) {
    return { isClothing: true, gender: "unisex", genderSource: "none", productType: "other", confidence: 0.2, signal: "no text signal → kept" };
  }

  const { isClothing, productType, wearScore, nonwearScore } = classifyClothing(sources);
  const { gender, reason, source: genderSource } = classifyGender(input, sources);

  // Confidence: a blend of the clothing-decision margin and gender certainty.
  const clothingMargin = isClothing
    ? wearScore - nonwearScore
    : nonwearScore - wearScore;
  const clothingConf = clamp01(0.45 + 0.12 * clothingMargin);
  const genderConf =
    reason.startsWith("no gender") ? 0.4 : reason === "retailer default" ? 0.55 : reason.includes("female garment") ? 0.7 : 0.85;
  const confidence = Math.round(((clothingConf + genderConf) / 2) * 100) / 100;

  const signal = `${isClothing ? "clothing" : "NON-CLOTHING"} (wear ${wearScore.toFixed(1)} vs deny ${nonwearScore.toFixed(1)}); ${productType}; gender=${gender} [${reason}]`;

  return { isClothing, gender, genderSource, productType, confidence, signal };
}

function clamp01(n: number): number {
  return Math.max(0, Math.min(1, n));
}
