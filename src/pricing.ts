import type { Usage } from "@anthropic-ai/sdk/resources/messages/messages.js";
import type { StagehandMetrics } from "@browserbasehq/stagehand";

/**
 * USD/MTok for Claude Sonnet-class models — override via env when Anthropic changes pricing.
 */
function sonnetInputUsdPerMtok(): number {
  const v = process.env.CLAUDE_SONNET_INPUT_USD_PER_MTOK;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 3;
}

function sonnetOutputUsdPerMtok(): number {
  const v = process.env.CLAUDE_SONNET_OUTPUT_USD_PER_MTOK;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 15;
}

/** Web search tool: USD per request (billed separately from tokens). */
function webSearchUsdPerRequest(): number {
  const v = process.env.CLAUDE_WEB_SEARCH_USD_PER_REQUEST;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 0.01;
}

function cacheWriteUsdPerMtok(): number {
  const v = process.env.CLAUDE_CACHE_WRITE_USD_PER_MTOK;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return sonnetInputUsdPerMtok() * 1.25;
}

function cacheReadUsdPerMtok(): number {
  const v = process.env.CLAUDE_CACHE_READ_USD_PER_MTOK;
  if (v && !Number.isNaN(parseFloat(v))) return parseFloat(v);
  return 0.3;
}

/**
 * Estimate USD from a Messages API `usage` object (Sonnet pricing + web search requests).
 */
export function estimateUsdFromMessageUsage(usage: Usage): number {
  const inTok = usage.input_tokens ?? 0;
  const outTok = usage.output_tokens ?? 0;
  const cacheCreate = usage.cache_creation_input_tokens ?? 0;
  const cacheRead = usage.cache_read_input_tokens ?? 0;

  let usd =
    (inTok / 1_000_000) * sonnetInputUsdPerMtok() +
    (outTok / 1_000_000) * sonnetOutputUsdPerMtok() +
    (cacheCreate / 1_000_000) * cacheWriteUsdPerMtok() +
    (cacheRead / 1_000_000) * cacheReadUsdPerMtok();

  const ws = usage.server_tool_use?.web_search_requests ?? 0;
  usd += ws * webSearchUsdPerRequest();

  return Math.round(usd * 1_000_000) / 1_000_000;
}

/**
 * Stagehand reports OpenAI-style prompt/completion tokens; map to same Sonnet $/Mtok.
 */
export function estimateUsdFromStagehandMetrics(m: StagehandMetrics): number {
  const prompt = m.totalPromptTokens ?? 0;
  const completion = m.totalCompletionTokens ?? 0;
  const usd =
    (prompt / 1_000_000) * sonnetInputUsdPerMtok() + (completion / 1_000_000) * sonnetOutputUsdPerMtok();
  return Math.round(usd * 1_000_000) / 1_000_000;
}
