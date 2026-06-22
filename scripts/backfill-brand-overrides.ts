/**
 * Backfill the per-retailer brand overrides (brandName.ts), matching deployed upload:
 *  - UNIFY_BRAND: set every item of that retailer to the single store brand (J.Press).
 *  - CLEAN_NAME : rename only the mashed-slug brand value (norm(brand)==norm(retailer))
 *    to the proper name — so single-brand slug stores get cleaned fully, while genuine
 *    stockists keep their real per-item labels (only their junk-fallback rows change).
 *
 *   DRY RUN (default): node --env-file=.env --import tsx scripts/backfill-brand-overrides.ts
 *   APPLY:             ... scripts/backfill-brand-overrides.ts --apply
 */
import { Client } from "pg";
import { UNIFY_BRAND, CLEAN_NAME } from "../src/brandName.js";

const APPLY = process.argv.includes("--apply");
const norm = (s) => (s || "").toLowerCase().replace(/[^a-z0-9]/g, "");

function clientFromUrl(raw) {
  const u = new URL(raw);
  return new Client({
    host: u.hostname, port: u.port ? Number(u.port) : 5432,
    user: decodeURIComponent(u.username), password: decodeURIComponent(u.password),
    database: u.pathname.replace(/^\//, ""), ssl: { rejectUnauthorized: false },
  });
}

(async () => {
  const c = clientFromUrl(process.env.DATABASE_URL);
  await c.connect();
  let total = 0;

  for (const [retailer, brand] of Object.entries(UNIFY_BRAND)) {
    const sel = await c.query(`SELECT count(*)::int n FROM "ClothingItem" WHERE retailer=$1 AND brand IS DISTINCT FROM $2`, [retailer, brand]);
    const n = sel.rows[0].n; total += n;
    console.log(`  UNIFY ${retailer} -> "${brand}": ${n} rows`);
    if (APPLY && n) await c.query(`UPDATE "ClothingItem" SET brand=$2 WHERE retailer=$1 AND brand IS DISTINCT FROM $2`, [retailer, brand]);
  }

  for (const [retailer, brand] of Object.entries(CLEAN_NAME)) {
    // Only the mashed-slug value for this retailer (leaves real labels at stockists).
    const sel = await c.query(
      `SELECT count(*)::int n FROM "ClothingItem"
       WHERE retailer=$1 AND lower(regexp_replace(brand,'[^A-Za-z0-9]','','g'))=$2 AND brand IS DISTINCT FROM $3`,
      [retailer, norm(retailer), brand]);
    const n = sel.rows[0].n; total += n;
    if (n) console.log(`  CLEAN ${retailer} slug -> "${brand}": ${n} rows`);
    if (APPLY && n)
      await c.query(
        `UPDATE "ClothingItem" SET brand=$3
         WHERE retailer=$1 AND lower(regexp_replace(brand,'[^A-Za-z0-9]','','g'))=$2 AND brand IS DISTINCT FROM $3`,
        [retailer, norm(retailer), brand]);
  }

  console.log(`\ntotal rows ${APPLY ? "changed" : "to change"}: ${total}`);
  console.log(APPLY ? "APPLIED." : "DRY RUN — re-run with --apply to write.");
  await c.end();
})().catch((e) => { console.error("ERR", e.message); process.exit(1); });
