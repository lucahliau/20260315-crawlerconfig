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
} from "../src/currencyToUsd.js";

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

console.log("verify-fx: all assertions passed");
