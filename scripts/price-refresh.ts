/**
 * CLI for the cheap price/sale refresh (see src/priceRefresh.ts).
 *
 * Dry-run by default — prints what WOULD change, writes nothing.
 *
 *   npx tsx scripts/price-refresh.ts --retailer=taylorstitch          # dry-run one
 *   npx tsx scripts/price-refresh.ts --retailer=taylorstitch --apply  # write one
 *   npx tsx scripts/price-refresh.ts --all                            # dry-run sweep
 *   npx tsx scripts/price-refresh.ts --all --apply                    # write sweep
 *
 * Needs DATABASE_URL (run with `node --env-file=.env` semantics via tsx + dotenv).
 */
import "dotenv/config";
import { getPool } from "../src/pipelineStore.js";
import {
  loadRefreshableConfigs,
  refreshRetailerPrices,
  runPriceRefreshSweep,
} from "../src/priceRefresh.js";

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const apply = args.includes("--apply");
  const all = args.includes("--all");
  const retailerArg = args.find((a) => a.startsWith("--retailer="))?.split("=")[1];

  if (!all && !retailerArg) {
    console.log("Usage: npx tsx scripts/price-refresh.ts (--retailer=<slug> | --all) [--apply]");
    process.exit(1);
  }
  const dryRun = !apply;
  if (dryRun) console.log("DRY-RUN (pass --apply to write)\n");

  if (all) {
    const outcomes = await runPriceRefreshSweep({ dryRun });
    const bad = outcomes.filter((o) => o.status === "error");
    if (bad.length > 0) {
      console.log(`\n${bad.length} retailer(s) errored: ${bad.map((o) => o.retailer).join(", ")}`);
    }
  } else {
    const pool = getPool();
    if (!pool) {
      console.error("DATABASE_URL not set.");
      process.exit(1);
    }
    const configs = await loadRefreshableConfigs();
    const config = configs.find((c) => c.retailer === retailerArg);
    if (!config) {
      console.error(`No valid stored config for retailer "${retailerArg}".`);
      console.error(`Known: ${configs.map((c) => c.retailer).join(", ")}`);
      process.exit(1);
    }
    const outcome = await refreshRetailerPrices(config, pool, { dryRun });
    console.log(JSON.stringify(outcome, null, 2));
  }
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
