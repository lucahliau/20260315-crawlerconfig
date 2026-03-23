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
    pageSize: z.number(),
    productUrlTemplate: z.string(),
    totalItemsPath: z.string().optional(),
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
