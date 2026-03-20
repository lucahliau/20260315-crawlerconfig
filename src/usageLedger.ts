import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "./jsonFs.js";

const LEDGER_PATH =
  process.env.USAGE_LEDGER_PATH ?? path.join(process.cwd(), "usage-ledger.json");

const LEDGER_VERSION = 1 as const;

export interface DiscoverLedgerEvent {
  kind: "discover";
  t: string;
  estimatedUsd: number;
  inputTokens: number;
  outputTokens: number;
  webSearchRequests: number;
  e2eJobId?: string;
}

export interface ExploreLedgerEvent {
  kind: "explore";
  t: string;
  retailer: string;
  estimatedUsd: number;
  promptTokens: number;
  completionTokens: number;
  inferenceTimeMs: number;
  e2eJobId?: string;
}

export type UsageLedgerEvent = DiscoverLedgerEvent | ExploreLedgerEvent;

interface LedgerFile {
  version: typeof LEDGER_VERSION;
  events: UsageLedgerEvent[];
}

function loadLedger(): LedgerFile {
  try {
    const raw = fs.readFileSync(LEDGER_PATH, "utf-8");
    const data = JSON.parse(raw) as LedgerFile;
    if (data.version !== LEDGER_VERSION || !Array.isArray(data.events)) {
      return { version: LEDGER_VERSION, events: [] };
    }
    return data;
  } catch {
    return { version: LEDGER_VERSION, events: [] };
  }
}

function appendEvent(event: UsageLedgerEvent): void {
  try {
    const ledger = loadLedger();
    ledger.events.push(event);
    const max = parseInt(process.env.USAGE_LEDGER_MAX_EVENTS ?? "2000", 10);
    if (ledger.events.length > max) {
      ledger.events.splice(0, ledger.events.length - max);
    }
    writeJsonAtomic(LEDGER_PATH, ledger);
  } catch {
    // non-fatal
  }
}

export function recordDiscoverUsage(args: {
  estimatedUsd: number;
  inputTokens: number;
  outputTokens: number;
  webSearchRequests: number;
  e2eJobId?: string;
}): void {
  appendEvent({
    kind: "discover",
    t: new Date().toISOString(),
    estimatedUsd: args.estimatedUsd,
    inputTokens: args.inputTokens,
    outputTokens: args.outputTokens,
    webSearchRequests: args.webSearchRequests,
    ...(args.e2eJobId ? { e2eJobId: args.e2eJobId } : {}),
  });
}

export function recordExploreUsage(args: {
  retailer: string;
  estimatedUsd: number;
  promptTokens: number;
  completionTokens: number;
  inferenceTimeMs: number;
  e2eJobId?: string;
}): void {
  appendEvent({
    kind: "explore",
    t: new Date().toISOString(),
    retailer: args.retailer,
    estimatedUsd: args.estimatedUsd,
    promptTokens: args.promptTokens,
    completionTokens: args.completionTokens,
    inferenceTimeMs: args.inferenceTimeMs,
    ...(args.e2eJobId ? { e2eJobId: args.e2eJobId } : {}),
  });
}

function mean(nums: number[]): number | null {
  if (nums.length === 0) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

export interface CostMetricsResponse {
  /** Sum of estimated USD from all recorded events (discover + explore). */
  totalEstimatedUsd: number;
  /** Average cost per niche-brand discovery run. */
  avgDiscoverUsd: number | null;
  /** Average cost per config crawl (explore) run. */
  avgExploreUsd: number | null;
  /** Average estimated total per end-to-end job (grouped by e2eJobId). */
  avgE2eUsd: number | null;
  /** Number of recorded discover / explore / e2e runs (e2e = distinct job IDs). */
  counts: { discover: number; explore: number; e2e: number };
  /** Last event timestamps (ISO), if any. */
  lastDiscoverAt: string | null;
  lastExploreAt: string | null;
}

export function getCostMetrics(): CostMetricsResponse {
  const ledger = loadLedger();
  const discovers = ledger.events.filter((e): e is DiscoverLedgerEvent => e.kind === "discover");
  const explores = ledger.events.filter((e): e is ExploreLedgerEvent => e.kind === "explore");

  const discoverUsd = discovers.map((e) => e.estimatedUsd);
  const exploreUsd = explores.map((e) => e.estimatedUsd);

  const e2eTotals = new Map<string, number>();
  for (const e of ledger.events) {
    if (!e.e2eJobId) continue;
    const prev = e2eTotals.get(e.e2eJobId) ?? 0;
    e2eTotals.set(e.e2eJobId, prev + e.estimatedUsd);
  }
  const e2eValues = [...e2eTotals.values()];

  const totalEstimatedUsd = [...discoverUsd, ...exploreUsd].reduce((a, b) => a + b, 0);

  return {
    totalEstimatedUsd: Math.round(totalEstimatedUsd * 1_000_000) / 1_000_000,
    avgDiscoverUsd: mean(discoverUsd),
    avgExploreUsd: mean(exploreUsd),
    avgE2eUsd: mean(e2eValues),
    counts: {
      discover: discovers.length,
      explore: explores.length,
      e2e: e2eTotals.size,
    },
    lastDiscoverAt: discovers.length ? discovers[discovers.length - 1]!.t : null,
    lastExploreAt: explores.length ? explores[explores.length - 1]!.t : null,
  };
}
