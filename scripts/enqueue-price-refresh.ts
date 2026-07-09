/**
 * Enqueue a price-refresh job on the shared pg-boss queue so the Railway
 * SERVER executes it from its datacenter IP (the home connection exits via a
 * VPN that Shopify's /products.json rate limiter blocks outright).
 *
 * No `sweep` flag → the server treats it as a manual run and bypasses the
 * conveyor kill switch.
 *
 *   npx tsx scripts/enqueue-price-refresh.ts             # full sweep
 *   npx tsx scripts/enqueue-price-refresh.ts <retailer>  # one retailer
 */
import "dotenv/config";
import { enqueuePriceRefresh, stopBoss } from "../src/queue.js";

const retailer = process.argv[2];
const jobId = await enqueuePriceRefresh(retailer ? { retailer } : {});
if (jobId) {
  console.log(`enqueued price-refresh job ${jobId} (${retailer ?? "ALL retailers"})`);
} else {
  console.log("not enqueued — queue unavailable or an identical job is already queued today.");
}
await stopBoss();
process.exit(0);
