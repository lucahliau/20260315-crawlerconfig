import { StagehandHttpError } from "@browserbasehq/stagehand";

export interface ExploreFailureInfo {
  retryable: boolean;
  code:
    | "stagehand_payment_required"
    | "stagehand_rate_limited"
    | "timeout"
    | "network"
    | "navigation"
    | "browser_session"
    | "unknown";
  reason: string;
}

/** Effective Stagehand browser env (defaults to LOCAL). */
export function getResolvedStagehandEnv(): "LOCAL" | "BROWSERBASE" {
  const raw = process.env.STAGEHAND_ENV?.trim();
  if (raw === "BROWSERBASE") return "BROWSERBASE";
  return "LOCAL";
}

/**
 * Options shared by all `new Stagehand({ ... })` call sites.
 * For BROWSERBASE, disables the hosted Stagehand API by default so LLM calls use
 * your provider key (e.g. GOOGLE_GENERATIVE_AI_API_KEY) and avoid HTTP 402 from
 * the Stagehand cloud API. Set STAGEHAND_USE_API=true to use the hosted API.
 */
export function getStagehandBrowserOptions(): {
  env: "LOCAL" | "BROWSERBASE";
  disableAPI?: boolean;
} {
  const env = getResolvedStagehandEnv();
  if (env !== "BROWSERBASE") {
    return { env };
  }
  const useHostedApi = process.env.STAGEHAND_USE_API?.trim().toLowerCase() === "true";
  return {
    env,
    disableAPI: !useHostedApi,
  };
}

/** Log line for explore / upload when initializing Stagehand. */
export function formatStagehandInitMessage(localHeadless: boolean): string {
  const env = getResolvedStagehandEnv();
  if (env === "BROWSERBASE") {
    const useHostedApi = process.env.STAGEHAND_USE_API?.trim().toLowerCase() === "true";
    return `Initializing Stagehand (BROWSERBASE, hosted Stagehand API ${useHostedApi ? "on" : "off"})...`;
  }
  return `Initializing Stagehand (LOCAL, ${localHeadless ? "headless" : "headed"} browser)...`;
}

/** Clearer error when the Stagehand HTTP API returns 402. */
export function augmentStagehandInitError(err: unknown): Error {
  if (err instanceof StagehandHttpError) {
    const msg = err.message;
    if (/\b402\b/.test(msg)) {
      return new Error(
        "Stagehand API returned HTTP 402 (Payment Required). If you use Browserbase: add credits, set STAGEHAND_USE_API=false (default) to call Gemini with your key only, or set STAGEHAND_ENV=LOCAL for a local browser. See .env.example.",
        { cause: err },
      );
    }
  }
  return err instanceof Error ? err : new Error(String(err));
}

export function classifyExploreFailure(err: unknown): ExploreFailureInfo {
  const message = err instanceof Error ? err.message : String(err);
  const lower = message.toLowerCase();

  if (/\b402\b/.test(message) || lower.includes("payment required")) {
    return {
      retryable: true,
      code: "stagehand_payment_required",
      reason: "Browser/session provider could not start because the plan or credits need attention.",
    };
  }

  if (
    /\b429\b/.test(message) ||
    lower.includes("rate limit") ||
    lower.includes("too many requests")
  ) {
    return {
      retryable: true,
      code: "stagehand_rate_limited",
      reason: "The browser or model provider rate-limited the request.",
    };
  }

  if (
    lower.includes("timeout") ||
    lower.includes("timed out") ||
    lower.includes("etimedout") ||
    lower.includes("aborterror")
  ) {
    return {
      retryable: true,
      code: "timeout",
      reason: "The explore run timed out before it could finish.",
    };
  }

  if (
    lower.includes("net::") ||
    lower.includes("econnreset") ||
    lower.includes("econnrefused") ||
    lower.includes("ehostunreach") ||
    lower.includes("enotfound") ||
    lower.includes("dns") ||
    lower.includes("socket hang up") ||
    lower.includes("network")
  ) {
    return {
      retryable: true,
      code: "network",
      reason: "A transient network failure interrupted the explore run.",
    };
  }

  if (
    lower.includes("navigation") ||
    lower.includes("page crashed") ||
    lower.includes("target page, context or browser has been closed")
  ) {
    return {
      retryable: true,
      code: "navigation",
      reason: "The browser session failed during page navigation.",
    };
  }

  if (
    lower.includes("browserbase") ||
    lower.includes("stagehand") ||
    lower.includes("session") ||
    lower.includes("websocket")
  ) {
    return {
      retryable: true,
      code: "browser_session",
      reason: "The managed browser session failed for operational reasons.",
    };
  }

  return {
    retryable: false,
    code: "unknown",
    reason: "The explore run failed in a way that does not look transient.",
  };
}
