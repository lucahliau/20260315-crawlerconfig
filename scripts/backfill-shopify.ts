/**
 * Batched Shopify-native backfill: re-crawl every Shopify brand's existing items
 * in place (full galleries + sales + flag reset) via uploadRetailer, run as
 * chunked tasks across a worker pool. Brands are split into ~CHUNK-URL tasks and
 * interleaved round-robin so the pool stays balanced (no big-brand long pole)
 * while keeping ~1 concurrent stream per domain (polite). Resumable via a
 * per-chunk done-file. Run on the M4:
 *   npx tsx scripts/backfill-shopify.ts --dry-run
 *   npx tsx scripts/backfill-shopify.ts --limit=3 --asc       # small validation wave
 *   npx tsx scripts/backfill-shopify.ts --concurrency=8       # full run
 */
import "dotenv/config";
import { uploadRetailer } from "../src/upload.js";
import type { Config } from "../src/schemas/config.js";
import { Pool } from "pg";
import * as fs from "fs";

const clean = (u: string) => u.replace(/^["']+|["']+$/g, "");
const DONE_FILE = "tmp/backfill-done.txt";
const CHUNK = 1500;
const log = (m: string) => console.log(`[${new Date().toISOString().slice(11, 19)}] ${m}`);

async function isShopify(origin: string): Promise<boolean> {
  try {
    const r = await fetch(`${origin}/products.json?limit=1`, { headers: { "User-Agent": "Mozilla/5.0" } });
    if (!r.ok) return false;
    const d = (await r.json()) as { products?: unknown };
    return Array.isArray(d.products) && d.products.length > 0;
  } catch {
    return false;
  }
}

function buildConfig(retailer: string, origin: string): Config {
  return {
    retailer,
    retailerDisplayName: retailer,
    baseUrl: origin,
    discovery: {
      method: "api",
      api: {
        endpoint: `${origin}/products.json`,
        method: "GET",
        paginationParam: "page",
        pageSize: 250,
        productUrlTemplate: `${origin}/products/{handle}`,
        shopifyNative: true,
      },
    },
    productPage: { hasJsonLd: true, jsonLdNotes: "native backfill", hasOpenGraph: true },
    requestConfig: { requiresJsRendering: false, delayBetweenRequestsMs: 1000, blocksHeadlessBrowsers: false, notes: "backfill" },
    dataQuality: { jsonLdCompleteness: "high", estimatedProductCount: 0, sampleProductUrls: [], missingFields: [], overallRecommendation: "recommended" },
  };
}

type Task = { retailer: string; origin: string; urls: string[]; label: string };

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const order = args.includes("--asc") ? "ASC" : "DESC";
  const limit = Number(args.find((a) => a.startsWith("--limit="))?.split("=")[1] ?? 0);
  const concurrency = Number(args.find((a) => a.startsWith("--concurrency="))?.split("=")[1] ?? 8);

  const pool = new Pool({ connectionString: clean(process.env.DATABASE_URL ?? ""), max: 4 });
  const { rows: brands } = await pool.query<{ retailer: string; n: number; sample: string }>(
    `SELECT retailer, count(*)::int AS n, (array_agg("sourceUrl" ORDER BY "sourceUrl"))[1] AS sample
     FROM "ClothingItem" WHERE active AND "sourceUrl" LIKE '%/products/%' AND retailer IS NOT NULL
     GROUP BY retailer ORDER BY n ${order}`,
  );
  const done = new Set(fs.existsSync(DONE_FILE) ? fs.readFileSync(DONE_FILE, "utf8").split("\n").filter(Boolean) : []);
  log(`${brands.length} brands with /products/ sourceUrls; filtering to Shopify (skipping done chunks)...`);

  const targets: { retailer: string; origin: string; n: number }[] = [];
  for (const b of brands) {
    let origin = "";
    try {
      origin = new URL(b.sample).origin;
    } catch {
      continue;
    }
    if (await isShopify(origin)) {
      targets.push({ retailer: b.retailer, origin, n: b.n });
      if (limit && targets.length >= limit) break;
    }
  }

  // Build chunked tasks, then interleave round-robin (chunk 0 of every brand,
  // then chunk 1, …) so big brands don't monopolize a worker and each domain
  // sees ~1 stream at a time.
  const perBrand: Task[][] = [];
  for (const t of targets) {
    const { rows } = await pool.query<{ sourceUrl: string }>(
      `SELECT "sourceUrl" FROM "ClothingItem" WHERE retailer=$1 AND active AND "sourceUrl" LIKE '%/products/%'`,
      [t.retailer],
    );
    const urls = rows.map((r) => r.sourceUrl);
    const chunks: Task[] = [];
    for (let i = 0; i < urls.length; i += CHUNK) {
      const label = `${t.retailer}#${i / CHUNK}`;
      if (!done.has(label)) chunks.push({ retailer: t.retailer, origin: t.origin, urls: urls.slice(i, i + CHUNK), label });
    }
    if (chunks.length) perBrand.push(chunks);
  }
  const tasks: Task[] = [];
  const maxC = Math.max(0, ...perBrand.map((c) => c.length));
  for (let i = 0; i < maxC; i++) for (const c of perBrand) if (c[i]) tasks.push(c[i]);

  const totalItems = tasks.reduce((s, t) => s + t.urls.length, 0);
  log(`${targets.length} Shopify brands → ${tasks.length} pending chunks = ${totalItems} items. concurrency=${concurrency} order=${order}`);
  if (dryRun) {
    for (const t of targets) log(`  ${t.retailer.padEnd(24)} ${t.n} items  ${t.origin}`);
    await pool.end();
    return;
  }

  fs.mkdirSync("tmp", { recursive: true });
  let idx = 0,
    doneCount = 0,
    totUp = 0,
    totFail = 0;
  async function worker(wid: number) {
    while (idx < tasks.length) {
      const t = tasks[idx++];
      log(`[w${wid}] START ${t.label} (${t.urls.length} urls)`);
      const t0 = Date.now();
      try {
        const res = await uploadRetailer(buildConfig(t.retailer, t.origin), t.urls, () => {}, undefined, {});
        totUp += res.uploaded;
        totFail += res.failed;
        fs.appendFileSync(DONE_FILE, t.label + "\n");
        log(`[w${wid}] DONE ${t.label}: up=${res.uploaded} skip=${res.skipped} fail=${res.failed} (${Math.round((Date.now() - t0) / 1000)}s)`);
      } catch (e) {
        log(`[w${wid}] ERROR ${t.label}: ${(e as Error).message}`);
      }
      doneCount++;
      log(`progress: ${doneCount}/${tasks.length} chunks | items uploaded=${totUp} failed=${totFail}`);
    }
  }
  await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
  log(`BACKFILL COMPLETE: ${doneCount} chunks, items uploaded=${totUp} failed=${totFail}`);
  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
