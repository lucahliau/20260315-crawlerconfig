/**
 * Approximate USD value of one unit of each currency (24‑month rolling averages, ~Mar 2024–Mar 2026).
 * Hardcoded for stability; refresh periodically when you need fresher retail pricing.
 */
export const USD_PER_UNIT: Record<string, number> = {
  USD: 1,
  EUR: 1.08,
  GBP: 1.27,
  JPY: 1 / 150, // ~150 JPY per USD
  CNY: 0.14,
  CHF: 1.13,
  CAD: 0.71,
  AUD: 0.65,
  NZD: 0.59,
  SEK: 0.094,
  NOK: 0.091,
  DKK: 0.145,
  PLN: 0.25,
  INR: 0.012,
  KRW: 0.00072,
  SGD: 0.74,
  HKD: 0.128,
  MXN: 0.055,
  BRL: 0.18,
  TRY: 0.03,
  ZAR: 0.055,
  THB: 0.029,
  TWD: 0.031,
};

const ALIASES: Record<string, string> = {
  EURO: "EUR",
  RMB: "CNY",
  RENMINBI: "CNY",
  YUAN: "CNY",
};

function stripToLetters(raw: string): string {
  return raw.replace(/[^A-Za-z]/g, "");
}

/**
 * Normalize scraped currency strings toward ISO 4217 (3 letters).
 */
export function normalizeCurrencyCode(raw: string): string | null {
  const t = raw.trim();
  if (!t) return null;
  const upper = t.toUpperCase();
  if (ALIASES[upper]) return ALIASES[upper];
  const letters = stripToLetters(upper);
  if (letters.length === 3) return letters;
  if (letters.length > 3) return letters.slice(0, 3);
  return null;
}

/**
 * Best-effort currency guess from the URL host when product markup omits priceCurrency.
 */
export function inferCurrencyFromHostname(url: string): string | null {
  let host: string;
  try {
    host = new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }

  if (host.endsWith(".jp")) return "JPY";
  if (host.endsWith(".kr")) return "KRW";
  if (host.endsWith(".cn")) return "CNY";
  if (host.endsWith(".tw")) return "TWD";
  if (host.endsWith(".au")) return "AUD";
  if (host.endsWith(".nz")) return "NZD";
  if (host.endsWith(".ca")) return "CAD";
  if (host.endsWith(".ch")) return "CHF";
  if (host.endsWith(".se")) return "SEK";
  if (host.endsWith(".no")) return "NOK";
  if (host.endsWith(".dk")) return "DKK";
  if (host.endsWith(".in")) return "INR";
  if (host.endsWith(".sg")) return "SGD";
  if (host.endsWith(".hk")) return "HKD";
  if (host.endsWith(".mx")) return "MXN";
  if (host.endsWith(".br")) return "BRL";
  if (host.endsWith(".tr")) return "TRY";
  if (host.endsWith(".za")) return "ZAR";
  if (host.endsWith(".pl")) return "PLN";
  if (host.endsWith(".th")) return "THB";

  if (host.endsWith(".co.uk") || host.endsWith(".uk")) return "GBP";
  if (host.endsWith(".fr") || host.endsWith(".de") || host.endsWith(".it") || host.endsWith(".es") || host.endsWith(".nl") || host.endsWith(".be") || host.endsWith(".at") || host.endsWith(".pt") || host.endsWith(".ie") || host.endsWith(".fi")) {
    return "EUR";
  }

  return null;
}

function isIntegerish(price: number): boolean {
  return Number.isInteger(price) || Math.abs(price - Math.round(price)) < 1e-9;
}

/**
 * When schema.org says USD but the host and nominal amount look like another currency, override.
 */
export function correctLikelyMisdeclaredUsd(url: string, price: number, declared: string): string {
  if (declared !== "USD") return declared;

  let host: string;
  try {
    host = new URL(url).hostname.toLowerCase();
  } catch {
    return "USD";
  }

  const int = isIntegerish(price);

  if ((host.endsWith(".jp") || host.endsWith(".co.jp")) && int && price >= 1000 && price <= 9_999_999) {
    return "JPY";
  }

  if (host.endsWith(".kr") && int && price >= 1000 && price <= 99_999_999) {
    return "KRW";
  }

  if (host.endsWith(".tw") && int && price >= 100 && price <= 999_999) {
    return "TWD";
  }

  return "USD";
}

export interface ResolveCurrencyResult {
  iso: string;
  notes: string[];
}

/**
 * Final ISO currency code to use before USD conversion: explicit data, mis-declared USD fix, then hostname.
 */
export function resolveCurrencyForUsd(url: string, price: number, rawCurrency: string): ResolveCurrencyResult {
  const notes: string[] = [];
  const trimmed = rawCurrency.trim();

  if (!trimmed) {
    const inferred = inferCurrencyFromHostname(url);
    if (inferred) {
      notes.push(`inferred ${inferred} from hostname`);
      return { iso: inferred, notes };
    }
    notes.push("defaulted to USD (no currency in markup, hostname ambiguous)");
    return { iso: "USD", notes };
  }

  let iso = normalizeCurrencyCode(trimmed);
  if (!iso) {
    notes.push(`unrecognized currency "${trimmed}", treating as USD`);
    return { iso: "USD", notes };
  }

  if (iso === "USD") {
    const corrected = correctLikelyMisdeclaredUsd(url, price, "USD");
    if (corrected !== "USD") {
      notes.push(`corrected mis-declared USD → ${corrected} (host/magnitude)`);
      iso = corrected;
    }
  }

  return { iso, notes };
}

export interface ConvertToUsdResult {
  usd: number;
  usdPerUnit: number;
  unknownCurrency: boolean;
}

/**
 * Convert amount from `currencyIso` to USD using `USD_PER_UNIT`.
 * Unknown codes: no conversion (treat as USD) and `unknownCurrency: true`.
 */
export function convertToUsd(amount: number, currencyIso: string): ConvertToUsdResult {
  const iso = currencyIso.toUpperCase();
  const usdPerUnit = USD_PER_UNIT[iso];
  if (usdPerUnit == null) {
    return { usd: amount, usdPerUnit: 1, unknownCurrency: true };
  }
  const usd = amount * usdPerUnit;
  return { usd, usdPerUnit, unknownCurrency: false };
}

export function roundUsdPrice(n: number): number {
  return Math.round(n * 100) / 100;
}
