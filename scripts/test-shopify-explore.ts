// Manual smoke test for the Shopify fast path (plain HTTP, no AI spend).
// Usage: npx tsx scripts/test-shopify-explore.ts [url]
import { tryShopifyExplore, isShopifyStore } from "../src/shopifyExplore.js";
import { safeParseConfig } from "../src/schemas/config.js";

async function main() {
  const url = process.argv[2] ?? "https://18east.co";
  const cfg = await tryShopifyExplore(url, { retailer: "smoke-test", displayName: "Smoke Test" }, console.log);
  if (!cfg) {
    console.log("FAST PATH RETURNED NULL for", url);
    process.exit(1);
  }
  const parsed = safeParseConfig(cfg);
  console.log("schema valid:", parsed.success);
  if (!parsed.success) console.log(JSON.stringify(parsed.error.flatten(), null, 2));
  console.log(
    JSON.stringify(
      { discovery: cfg.discovery, dataQuality: cfg.dataQuality, displayName: cfg.retailerDisplayName },
      null,
      2,
    ),
  );
  console.log("non-shopify negative check (asket.com):", await isShopifyStore("https://www.asket.com"));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
