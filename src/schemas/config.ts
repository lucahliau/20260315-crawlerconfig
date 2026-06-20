import { z } from "zod";

const SitemapDiscoverySchema = z.object({
  method: z.literal("sitemap"),
  sitemap: z.object({
    url: z.string(),
    productUrlPattern: z.string(),
  }),
});

const CategoryPaginationDiscoverySchema = z.object({
  method: z.literal("categoryPagination"),
  categoryPagination: z.object({
    startUrls: z.array(z.string()),
    paginationParam: z.string(),
    pageStartsAt: z.number(),
    pageIncrement: z.number(),
    maxPages: z.number(),
  }),
});

const InfiniteScrollDiscoverySchema = z.object({
  method: z.literal("infiniteScroll"),
  infiniteScroll: z.object({
    startUrls: z.array(z.string()),
    scrollPauseMs: z.number(),
    maxScrolls: z.number(),
    xhrPattern: z.string().optional(),
  }),
});

const ApiDiscoverySchema = z.object({
  method: z.literal("api"),
  api: z.object({
    endpoint: z.string(),
    method: z.string(),
    headers: z.record(z.string()).optional(),
    paginationParam: z.string(),
    /** Query param carrying the page size (e.g. "limit" for Shopify, "per_page" for WooCommerce). */
    pageSizeParam: z.string().optional(),
    pageSize: z.number(),
    /** First page number (defaults to 1). */
    startPage: z.number().optional(),
    /** Dot-path to the items array in the response (e.g. "products"); auto-detected when omitted. */
    itemsPath: z.string().optional(),
    productUrlTemplate: z.string(),
    totalItemsPath: z.string().optional(),
    /** When true the upload step ingests each product directly from
     * `/products/<handle>.json` (full gallery + variant price + compare_at_price)
     * instead of scraping the HTML product page. Set by the Shopify fast-path explore. */
    shopifyNative: z.boolean().optional(),
  }),
});

const DiscoverySchema = z.discriminatedUnion("method", [
  SitemapDiscoverySchema,
  CategoryPaginationDiscoverySchema,
  InfiniteScrollDiscoverySchema,
  ApiDiscoverySchema,
]);

const ProductLinkExtractionSchema = z.object({
  selector: z.string(),
  urlAttribute: z.string(),
  productUrlPattern: z.string(),
  urlIsRelative: z.boolean(),
});

const ProductPageSchema = z.object({
  hasJsonLd: z.boolean(),
  jsonLdNotes: z.string(),
  hasOpenGraph: z.boolean(),
  additionalImageSelector: z.string().optional(),
  genderInUrl: z
    .object({
      detected: z.boolean(),
      patterns: z.record(z.string()),
    })
    .optional(),
  categoryInUrl: z
    .object({
      detected: z.boolean(),
      breadcrumbSelector: z.string().optional(),
    })
    .optional(),
});

const RequestConfigSchema = z.object({
  requiresJsRendering: z.boolean(),
  delayBetweenRequestsMs: z.number(),
  requiredCookies: z.record(z.string()).optional(),
  requiredHeaders: z.record(z.string()).optional(),
  blocksHeadlessBrowsers: z.boolean(),
  notes: z.string(),
});

const DataQualitySchema = z.object({
  jsonLdCompleteness: z.enum(["high", "medium", "low"]),
  estimatedProductCount: z.number(),
  sampleProductUrls: z.array(z.string()),
  missingFields: z.array(z.string()),
  overallRecommendation: z.enum(["recommended", "usable", "not recommended"]),
});

export const ConfigSchema = z.object({
  retailer: z.string(),
  retailerDisplayName: z.string(),
  baseUrl: z.string(),
  discovery: DiscoverySchema,
  productLinkExtraction: ProductLinkExtractionSchema.optional(),
  productPage: ProductPageSchema,
  requestConfig: RequestConfigSchema,
  dataQuality: DataQualitySchema,
});

export type Config = z.infer<typeof ConfigSchema>;

export function validateConfig(data: unknown): Config {
  return ConfigSchema.parse(data);
}

export function safeParseConfig(data: unknown) {
  return ConfigSchema.safeParse(data);
}
