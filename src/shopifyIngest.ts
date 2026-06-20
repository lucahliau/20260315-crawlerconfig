/**
 * Shopify-native ingestion: map a product object from the public
 * `/products/<handle>.json` (or `/products.json`) endpoint straight into our
 * `ClothingItemInput` fields — full image gallery, variant price, and
 * `compare_at_price` (sale) in one structured read, instead of scraping the
 * HTML product page.
 *
 * This module is pure (no network / no DB) so it can be unit-tested against
 * sample product JSON. `upload.ts` fetches the JSON, calls `mapShopifyProductFields`,
 * then runs the shared classifier + currency normalization + R2 upload + upsert.
 */

export interface ShopifyVariant {
  price?: string | number | null;
  compare_at_price?: string | number | null;
  sku?: string | null;
  title?: string | null;
}

export interface ShopifyImage {
  src?: string | null;
}

export interface ShopifyOption {
  name?: string | null;
  values?: string[] | null;
}

export interface ShopifyProduct {
  id?: number | string;
  handle?: string;
  title?: string | null;
  body_html?: string | null;
  vendor?: string | null;
  product_type?: string | null;
  tags?: string[] | string | null;
  images?: ShopifyImage[] | null;
  variants?: ShopifyVariant[] | null;
  options?: ShopifyOption[] | null;
}

/** The subset of ClothingItemInput we can derive from a Shopify product object.
 *  Classification (gender/productType/isClothing) is added later by the shared
 *  classifier; currency is resolved later from the hostname. */
export interface ShopifyMappedFields {
  name: string | null;
  description: string | null;
  brand: string | null;
  category: string | null;
  images: string[];
  price: number | null;
  salePrice: number | null;
  compareAtPrice: number | null;
  sizes: string[];
  colors: string[];
  tags: string[];
  externalId: string | null;
  sourceUrl: string;
}

/** Cap stored images per product (matches the per-page gallery-rescue cap). */
export const MAX_GALLERY_IMAGES = 12;

function toNumber(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === "number" ? v : parseFloat(String(v));
  return Number.isFinite(n) ? n : null;
}

function stripHtml(html: string | null | undefined): string {
  if (!html) return "";
  return html
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Map one Shopify product object to our item fields. Pure + deterministic. */
export function mapShopifyProductFields(product: ShopifyProduct, origin: string): ShopifyMappedFields {
  const handle = (product.handle ?? "").trim();

  const images = (product.images ?? [])
    .map((i) => (typeof i?.src === "string" ? i.src : ""))
    .filter(Boolean)
    .slice(0, MAX_GALLERY_IMAGES);

  // Representative variant = the lowest-priced one (matches the price Shopify
  // surfaces on the listing). Sale = that variant's compare_at_price > price.
  const variants = product.variants ?? [];
  let repPrice: number | null = null;
  let repCompareAt: number | null = null;
  for (const v of variants) {
    const p = toNumber(v?.price);
    if (p == null) continue;
    if (repPrice == null || p < repPrice) {
      repPrice = p;
      repCompareAt = toNumber(v?.compare_at_price);
    }
  }
  let salePrice: number | null = null;
  let compareAtPrice: number | null = null;
  if (repPrice != null && repCompareAt != null && repCompareAt > repPrice) {
    compareAtPrice = repCompareAt;
    salePrice = repPrice;
  }

  // Sizes / colors from the product options (the clean structured source).
  const sizes = new Set<string>();
  const colors = new Set<string>();
  for (const opt of product.options ?? []) {
    const name = (opt?.name ?? "").toLowerCase();
    const values = (opt?.values ?? []).filter((v): v is string => typeof v === "string" && v.trim() !== "");
    if (/size/.test(name)) {
      for (const v of values) sizes.add(v.trim().toUpperCase());
    } else if (/colou?r/.test(name)) {
      for (const v of values) colors.add(v.trim().toLowerCase());
    }
  }

  // Tags: products.json returns a comma-separated string (older array shape too).
  let tags: string[] = [];
  if (Array.isArray(product.tags)) {
    tags = product.tags.filter((t): t is string => typeof t === "string").map((t) => t.trim()).filter(Boolean);
  } else if (typeof product.tags === "string") {
    tags = product.tags.split(",").map((t) => t.trim()).filter(Boolean);
  }

  // externalId: first usable variant SKU, else null → caller falls back to the
  // URL handle (extractSlugFromUrl), matching the per-page path so re-crawls
  // update existing rows in place rather than inserting duplicates.
  const sku = variants.map((v) => (typeof v?.sku === "string" ? v.sku.trim() : "")).find((s) => s !== "") ?? null;

  return {
    name: (product.title ?? "").trim() || null,
    description: stripHtml(product.body_html) || null,
    brand: (product.vendor ?? "").trim() || null,
    category: (product.product_type ?? "").trim() || null,
    images,
    price: repPrice,
    salePrice,
    compareAtPrice,
    sizes: [...sizes],
    colors: [...colors],
    tags,
    externalId: sku,
    sourceUrl: `${origin}/products/${handle}`,
  };
}
