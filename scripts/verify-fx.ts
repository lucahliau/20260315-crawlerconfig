/**
 * Quick sanity checks for currency normalization (run: npx tsx scripts/verify-fx.ts).
 */
import assert from "node:assert/strict";
import {
  convertToUsd,
  correctLikelyMisdeclaredUsd,
  inferCurrencyFromHostname,
  normalizeCurrencyCode,
  resolveCurrencyForUsd,
  roundUsdPrice,
  vatRateForSource,
} from "../src/currencyToUsd.js";
import { sanitizeBrand, resolveBrand } from "../src/brandName.js";

assert.equal(normalizeCurrencyCode(" eur "), "EUR");
assert.equal(normalizeCurrencyCode("EURO"), "EUR");
assert.equal(normalizeCurrencyCode("!!"), null);

assert.equal(inferCurrencyFromHostname("https://shop.example.jp/p/1"), "JPY");
assert.equal(inferCurrencyFromHostname("https://www.amazon.co.jp/foo"), "JPY");
assert.equal(inferCurrencyFromHostname("https://a.co.uk/b"), "GBP");
assert.equal(inferCurrencyFromHostname("https://x.com/p"), null);

assert.equal(correctLikelyMisdeclaredUsd("https://x.jp/a", 18000, "USD"), "JPY");
assert.equal(correctLikelyMisdeclaredUsd("https://x.com/a", 18000, "USD"), "USD");

const jp = resolveCurrencyForUsd("https://store.jp/p", 18000, "USD");
assert.equal(jp.iso, "JPY");
assert.ok(jp.notes.some((n) => n.includes("corrected")));

const eur = convertToUsd(100, "EUR");
assert.ok(eur.usd > 100 && eur.usd < 120);
assert.equal(roundUsdPrice(eur.usd), Math.round(eur.usd * 100) / 100);

const jpy = convertToUsd(15000, "JPY");
assert.ok(jpy.usd > 90 && jpy.usd < 130);

// VAT stripping: US prices strip nothing; EU/UK home stores strip the home VAT.
assert.equal(vatRateForSource("https://taylorstitch.com/p", "USD"), 0);
assert.equal(vatRateForSource("https://www.sunspel.com/p", "GBP"), 0.2); // GBP → UK 20%
assert.equal(vatRateForSource("https://www.mc2saintbarth.com/p", "EUR"), 0.21); // EUR no-TLD → 21% default
assert.equal(vatRateForSource("https://it.mc2saintbarth.com/p", "EUR"), 0.22); // .it → Italy 22%
assert.equal(vatRateForSource("https://www.adaysmarch.com/dk/p", "DKK"), 0.25); // /dk/ → Denmark 25%
assert.equal(vatRateForSource("https://www.armedangels.com/de/p", "EUR"), 0.19); // /de/ → Germany 19%
assert.equal(vatRateForSource("https://x.com/en-us/p", "EUR"), 0); // /en-us/ presentment → no strip
// £155 inc-20%-VAT × 1.27 = $164 ex-VAT (vs $196.85 with VAT) — fixes the over-charge.
{
  const vat = vatRateForSource("https://www.sunspel.com/p", "GBP");
  const exVat = 155 / (1 + vat);
  const usd = roundUsdPrice(convertToUsd(exVat, "GBP").usd);
  assert.ok(usd > 160 && usd < 170, `expected ~164, got ${usd}`);
}

// Brand resolution: replace distributor/gender/category/region values with the
// discovered name; KEEP genuine brand names (incl. multi-brand stockists).
assert.equal(sanitizeBrand("Bshop Co.,Ltd", "Danton"), "Danton"); // distributor
assert.equal(sanitizeBrand("Women", "Mads Nørgaard"), "Mads Nørgaard"); // gender word
assert.equal(sanitizeBrand("Accessories", "Community Clothing"), "Community Clothing"); // category
assert.equal(sanitizeBrand("Drakes - Archive", "Drakes"), "Drakes"); // region/collection
assert.equal(sanitizeBrand("Genexy Company Limited", "Linksoul"), "Linksoul"); // distributor
assert.equal(sanitizeBrand("Won Hundred Men", "Won Hundred"), "Won Hundred"); // trailing gender
assert.equal(sanitizeBrand("Aimé Leon Dore", "Aimeleondore"), "Aimé Leon Dore"); // KEEP (slug fallback)
assert.equal(sanitizeBrand("Dark Seas", "Darkseas"), "Dark Seas"); // KEEP
assert.equal(sanitizeBrand("Billabong", "Southcoast"), "Billabong"); // KEEP (stockist's real label)
assert.equal(sanitizeBrand("California Arts", "bodywaves"), "California Arts"); // KEEP (corrupt fallback)
assert.equal(sanitizeBrand("Captain Fin Co.", "Captainfin"), "Captain Fin Co."); // KEEP (bare "Co." is fine)
assert.equal(sanitizeBrand("", "Folk"), "Folk"); // empty scraped → fallback

// resolveBrand: per-retailer unify + clean-name overrides on top of sanitizeBrand.
assert.equal(resolveBrand("Pennant", "jpressonline", "Jpressonline"), "J. Press"); // UNIFY: store wins
assert.equal(resolveBrand("Jack Victor", "jpressonline", "Jpressonline"), "J. Press"); // UNIFY: even a real co-label
assert.equal(resolveBrand("Women", "madsnorgaard", "Madsnorgaard"), "Mads Nørgaard"); // bad scraped → CLEAN_NAME
assert.equal(resolveBrand("Save Khaki United", "nantucketreds", "Nantucketreds"), "Save Khaki United"); // stockist keeps real brand
assert.equal(resolveBrand("Billabong", "southcoast", "Southcoast"), "Billabong"); // un-named stockist keeps real brand
assert.equal(resolveBrand("Aimé Leon Dore", "aimeleondore", "Aimeleondore"), "Aimé Leon Dore"); // KEEP clean scraped

console.log("verify-fx: all assertions passed");
