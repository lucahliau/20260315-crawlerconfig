/**
 * Live FX rates for USD normalization.
 *
 * The hardcoded USD_PER_UNIT table in currencyToUsd.ts is a 24-month rolling
 * average — safe but stale (GBP 1.27 vs ~1.33 spot ≈ 5% price error). This
 * module refreshes it daily from a free no-key API and overlays the live rates
 * via setLiveFxRates(); the hardcoded table stays as the fallback so a dead
 * API can never break uploads.
 *
 * Persistence: pipeline_settings KV ("fx_rates_usd") so every process (Railway
 * server, M1/M4 workers) shares one fetch per day instead of hitting the API
 * per process restart.
 *
 * Sanity guard: a live rate is accepted only within ±25% of the hardcoded
 * baseline for that currency — garbage API data degrades to the baseline
 * instead of corrupting the whole catalog's prices.
 */
import { USD_PER_UNIT, setLiveFxRates } from "./currencyToUsd.js";
import { getSetting, setSetting } from "./pipelineStore.js";

const FX_KV_KEY = "fx_rates_usd";
const FX_MAX_AGE_MS = 24 * 60 * 60 * 1000; // refetch after 24h
const FX_RETRY_MS = 60 * 60 * 1000; // after a failed fetch, retry hourly
const FX_API_URL = process.env.FX_API_URL ?? "https://open.er-api.com/v6/latest/USD";
const MAX_DEVIATION = 0.25;

interface StoredFxRates {
  /** USD value of one unit of each currency (same shape as USD_PER_UNIT). */
  usdPerUnit: Record<string, number>;
  fetchedAt: string;
  source: string;
}

let lastAttemptAt = 0;
let appliedFetchedAt: string | null = null;

/** Clamp to supported currencies and reject rates wildly off the baseline. */
function sanitizeRates(usdPerUnit: Record<string, number>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [iso, baseline] of Object.entries(USD_PER_UNIT)) {
    const live = usdPerUnit[iso];
    if (typeof live !== "number" || !Number.isFinite(live) || live <= 0) continue;
    const deviation = Math.abs(live - baseline) / baseline;
    if (deviation > MAX_DEVIATION) {
      console.warn(
        `[fx] live rate for ${iso} (${live}) deviates ${(deviation * 100).toFixed(0)}% from baseline ${baseline} — ignoring it`,
      );
      continue;
    }
    out[iso] = live;
  }
  return out;
}

async function fetchLiveRates(): Promise<StoredFxRates | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);
  try {
    const res = await fetch(FX_API_URL, { signal: controller.signal });
    if (!res.ok) {
      console.warn(`[fx] rate API HTTP ${res.status} — keeping current rates`);
      return null;
    }
    const data = (await res.json()) as { result?: string; rates?: Record<string, number> };
    if (data.result !== "success" || !data.rates || data.rates.USD !== 1) {
      console.warn("[fx] rate API returned unexpected payload — keeping current rates");
      return null;
    }
    // open.er-api returns units-per-USD (1 USD = X EUR); invert to USD-per-unit.
    const usdPerUnit: Record<string, number> = {};
    for (const [iso, perUsd] of Object.entries(data.rates)) {
      if (typeof perUsd === "number" && Number.isFinite(perUsd) && perUsd > 0) {
        usdPerUnit[iso] = 1 / perUsd;
      }
    }
    return { usdPerUnit, fetchedAt: new Date().toISOString(), source: FX_API_URL };
  } catch (err) {
    console.warn(`[fx] rate fetch failed: ${(err as Error).message} — keeping current rates`);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Ensure the freshest available FX rates are applied. Order: KV cache when
 * <24h old, else live fetch (persisted back to KV), else whatever was already
 * applied / the hardcoded baseline. Cheap to call per run — memoized in-process.
 */
export async function ensureFreshFxRates(): Promise<void> {
  const now = Date.now();
  if (now - lastAttemptAt < FX_RETRY_MS && appliedFetchedAt) return;
  lastAttemptAt = now;

  try {
    let stored = await getSetting<StoredFxRates | null>(FX_KV_KEY, null);
    const age = stored ? now - Date.parse(stored.fetchedAt) : Infinity;

    if (!stored || !Number.isFinite(age) || age > FX_MAX_AGE_MS) {
      const fresh = await fetchLiveRates();
      if (fresh) {
        stored = fresh;
        await setSetting(FX_KV_KEY, fresh);
      }
    }

    if (stored && stored.fetchedAt !== appliedFetchedAt) {
      const sane = sanitizeRates(stored.usdPerUnit ?? {});
      if (Object.keys(sane).length > 0) {
        setLiveFxRates(sane);
        appliedFetchedAt = stored.fetchedAt;
        console.log(
          `[fx] applied live rates from ${stored.fetchedAt} (${Object.keys(sane).length} currencies; e.g. GBP ${sane.GBP ?? "n/a"}, EUR ${sane.EUR ?? "n/a"})`,
        );
      }
    }
  } catch (err) {
    console.warn(`[fx] ensureFreshFxRates failed: ${(err as Error).message} — using baseline rates`);
  }
}
