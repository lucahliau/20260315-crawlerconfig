/**
 * One-time backfill: strip embedded home-market VAT from already-converted catalog
 * prices, matching the deployed `normalizeItemPriceToUsd` behaviour (commit that
 * added vatRateForSource). VAT-stripping is a uniform divide-by-(1+vat) applied
 * before the linear FX, so for an already-converted USD price it's just price/(1+vat).
 *
 * Idempotent: skips rows already marked `metadata.vatRateStripped`. Recompute source
 * is the row's resolved `originalCurrency` + `sourceUrl` (country), never the raw
 * `price`, so re-runs are safe.
 *
 *   DRY RUN (default): NODE_PATH=node_modules npx tsx scripts/backfill-prices-vat.ts
 *   APPLY:             ... scripts/backfill-prices-vat.ts --apply
 */
import { Client } from "pg";
import { vatRateForSource } from "../src/currencyToUsd.js";

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

const round2 = (n: number): number => Math.round(n * 100) / 100;

(async () => {
  const c = clientFromUrl(process.env.DATABASE_URL as string);
  await c.connect();

  // Only items that were actually converted from a foreign currency and not yet
  // VAT-stripped. Currency-mislabel rows (no originalCurrency) are a separate bug
  // and are intentionally left untouched.
  const { rows } = await c.query<{
    id: string;
    price: string;
    salePrice: string | null;
    compareAtPrice: string | null;
    sourceUrl: string | null;
    oc: string;
  }>(`
    SELECT id, price::text, "salePrice"::text, "compareAtPrice"::text,
           "sourceUrl", metadata->>'originalCurrency' AS oc
    FROM "ClothingItem"
    WHERE metadata ? 'originalCurrency'
      AND metadata->>'originalCurrency' <> 'USD'
      AND NOT (metadata ? 'vatRateStripped')
  `);

  let changed = 0;
  const byRate: Record<string, number> = {};
  const samples: string[] = [];

  for (const r of rows) {
    const vat = vatRateForSource(r.sourceUrl ?? "", r.oc);
    if (vat <= 0) continue;
    const oldP = Number(r.price);
    const newP = round2(oldP / (1 + vat));
    const newSale = r.salePrice == null ? null : round2(Number(r.salePrice) / (1 + vat));
    const newCmp = r.compareAtPrice == null ? null : round2(Number(r.compareAtPrice) / (1 + vat));
    changed++;
    byRate[`${Math.round(vat * 100)}%`] = (byRate[`${Math.round(vat * 100)}%`] ?? 0) + 1;
    if (samples.length < 12) samples.push(`  ${r.oc} ${Math.round(vat * 100)}% VAT: $${oldP} -> $${newP}`);

    if (APPLY) {
      await c.query(
        `UPDATE "ClothingItem"
         SET price = $2, "salePrice" = $3, "compareAtPrice" = $4,
             metadata = jsonb_set(COALESCE(metadata,'{}'::jsonb), '{vatRateStripped}', to_jsonb($5::numeric))
         WHERE id = $1`,
        [r.id, newP, newSale, newCmp, vat],
      );
    }
  }

  console.log(`candidates (converted, not yet stripped): ${rows.length}`);
  console.log(`would change: ${changed}`);
  console.log(`by VAT rate: ${JSON.stringify(byRate)}`);
  console.log("samples:\n" + samples.join("\n"));
  console.log(APPLY ? "\nAPPLIED." : "\nDRY RUN — re-run with --apply to write.");

  await c.end();
})().catch((e) => {
  console.error("ERR", e.message);
  process.exit(1);
});
