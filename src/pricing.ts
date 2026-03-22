import type { StagehandMetrics } from "@browserbasehq/stagehand";

/**
 * Normalized discover-run usage (Gemini API + Google Search grounding).
 * See https://ai.google.dev/gemini-api/docs/google-search
 */
export interface DiscoverApiUsage {
  input_tokens: number;
  output_tokens: number;
  /** Proxy for ledger: count of web search queries (or 1 if grounded with chunks only). */
  web_search_requests: number;
}

/**
 * USD/MTok for Gemini Flash-class usage — override via env when Google changes pricing.
 * Defaults approximate Gemini 3 Flash / Flash-class list pricing; verify on https://ai.google.dev/pricing
 */
function geminiInputUsdPerMtok(): number {
  const v = process.env.GEMINI_INPUT_USD_PER_MTOK;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 0.3;
}

function geminiOutputUsdPerMtok(): number {
  const v = process.env.GEMINI_OUTPUT_USD_PER_MTOK;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 2.5;
}

/** Google Search grounding: rough $/request for estimates (override from your invoice). */
function geminiGroundingUsdPerRequest(): number {
  const v = process.env.GEMINI_GROUNDING_USD_PER_REQUEST;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 0.01;
}

/**
 * Estimate USD from discover usage (Gemini tokens + grounding).
 */
export function estimateUsdFromDiscoverUsage(usage: DiscoverApiUsage): number {
  const inTok = usage.input_tokens ?? 0;
  const outTok = usage.output_tokens ?? 0;
  let usd =
    (inTok / 1_000_000) * geminiInputUsdPerMtok() + (outTok / 1_000_000) * geminiOutputUsdPerMtok();
  usd += (usage.web_search_requests ?? 0) * geminiGroundingUsdPerRequest();
  return Math.round(usd * 1_000_000) / 1_000_000;
}

/**
 * Stagehand reports prompt/completion tokens; map to same Gemini $/Mtok as discover.
 */
export function estimateUsdFromStagehandMetrics(m: StagehandMetrics): number {
  const prompt = m.totalPromptTokens ?? 0;
  const completion = m.totalCompletionTokens ?? 0;
  const usd =
    (prompt / 1_000_000) * geminiInputUsdPerMtok() + (completion / 1_000_000) * geminiOutputUsdPerMtok();
  return Math.round(usd * 1_000_000) / 1_000_000;
}
