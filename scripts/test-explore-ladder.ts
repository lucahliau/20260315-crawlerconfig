// Smoke test for the zero-AI explore fast paths (and optionally the AI step).
// Usage: npx tsx scripts/test-explore-ladder.ts <url> [--ai]
import "dotenv/config";
import { isShopifyStore } from "../src/shopifyExplore.js";
import { tryWooCommerceExplore, trySitemapExplore } from "../src/platformExplore.js";
import { tryEvidenceExplore } from "../src/evidenceExplore.js";
import { safeParseConfig } from "../src/schemas/config.js";

async function main() {
  const url = process.argv[2];
  const withAi = process.argv.includes("--ai");
  if (!url) {
    console.error("Usage: npx tsx scripts/test-explore-ladder.ts <url> [--ai]");
    process.exit(1);
  }
  const identity = { retailer: "ladder-test", displayName: "Ladder Test" };
  const log = (m: string) => console.log("  " + m);

  console.log("1) Shopify:", (await isShopifyStore(url)) ? "YES (would use Shopify fast path)" : "no");

  console.log("2) WooCommerce:");
  const woo = await tryWooCommerceExplore(url, identity, log);
  if (woo) return report("woocommerce", woo);

  console.log("3) Universal sitemap:");
  const smap = await trySitemapExplore(url, identity, log);
  if (smap) return report("sitemap", smap);

  if (withAi) {
    console.log("4) Evidence explore (one Gemini call):");
    const ai = await tryEvidenceExplore(url, identity, log);
    if (ai) return report("evidence", ai);
  }
  console.log("No fast path matched — would fall back to Stagehand browser explore.");
}

function report(path: string, cfg: Record<string, unknown>) {
  const parsed = safeParseConfig(cfg);
  console.log(`\n=== ${path} fast path produced a config (schema valid: ${parsed.success}) ===`);
  console.log(JSON.stringify({ discovery: cfg.discovery, dataQuality: cfg.dataQuality }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
