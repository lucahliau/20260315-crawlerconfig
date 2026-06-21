/**
 * One-time backfill: clean up wrong brand names, matching the deployed upload
 * behaviour (sanitizeBrand). Keeps real scraped brands — including multi-brand
 * stockists — and only replaces distributor/operating-company names
 * (Danton → "Bshop Co.,Ltd"), gender/category words ("Women"/"Accessories"), and
 * region/collection variants ("Drakes - Archive") with the retailer's discovered
 * name. Also collapses "Won Hundred Men/Women" → "Won Hundred".
 *
 * Mapping: retailer_pipeline_state.display_name (fallback explore_state config).
 * Per distinct (retailer, brand) value so stockists keep their many labels.
 *
 *   DRY RUN (default): node --env-file=.env --import tsx scripts/backfill-brands.ts
 *   APPLY:             ... scripts/backfill-brands.ts --apply
 */
import { Client } from "pg";
import { sanitizeBrand } from "../src/brandName.js";

const APPLY = process.argv.includes("--apply");

function clientFromUrl(raw: string): Client {
  const u = new URL(raw);
  return new Client({
    host: u.hostname,
    port: u.port ? Number(u.port) : 5432,
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
    database: u.pathname.replace(/^\//, ""),
    ssl: { rejectUnauthorized: false },
  });
}

(async () => {
  const c = clientFromUrl(process.env.DATABASE_URL as string);
  await c.connect();

  const { rows: map } = await c.query<{ retailer: string; dn: string | null }>(`
    SELECT retailer,
           COALESCE(NULLIF(display_name, ''), explore_state->'config'->>'retailerDisplayName') AS dn
    FROM retailer_pipeline_state
  `);
  const displayName = new Map(map.map((m) => [m.retailer, (m.dn ?? "").trim()]));

  // Distinct (retailer, brand) pairs + row counts.
  const { rows: pairs } = await c.query<{ retailer: string; brand: string; n: string }>(`
    SELECT retailer, brand, count(*)::text AS n
    FROM "ClothingItem"
    GROUP BY retailer, brand
  `);

  let rowsChanged = 0;
  let pairsChanged = 0;
  const samples: string[] = [];

  for (const p of pairs) {
    const fb = displayName.get(p.retailer) ?? "";
    const next = sanitizeBrand(p.brand, fb);
    if (!next || next === p.brand) continue;
    const n = Number(p.n);
    rowsChanged += n;
    pairsChanged++;
    if (samples.length < 30) samples.push(`  [${p.retailer}] "${p.brand}" -> "${next}"  (${n} rows)`);
    if (APPLY) {
      await c.query(`UPDATE "ClothingItem" SET brand = $3 WHERE retailer = $1 AND brand = $2`, [
        p.retailer,
        p.brand,
        next,
      ]);
    }
  }

  console.log(`distinct (retailer,brand) pairs: ${pairs.length}`);
  console.log(`pairs changed: ${pairsChanged}  | rows changed: ${rowsChanged}`);
  console.log("samples:\n" + samples.join("\n"));
  console.log(APPLY ? "\nAPPLIED." : "\nDRY RUN — re-run with --apply to write.");

  await c.end();
})().catch((e) => {
  console.error("ERR", e.message);
  process.exit(1);
});
