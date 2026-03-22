import { StagehandHttpError } from "@browserbasehq/stagehand";

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
