/**
 * Conveyor auto-chain: approve a brand on the phone and the pipeline carries
 * it the rest of the way — explore (free rungs) → crawl → upload (queue) →
 * processing — with no further taps.
 *
 * Stage handlers are injected by server.ts at startup (configureAutoChain) to
 * avoid a circular import with the server's job machinery. Every hook is
 * fire-and-forget: a chain failure logs but never fails the parent job.
 *
 * Gates, evaluated in order:
 *   1. auto_pipeline kill switch (pipeline_settings KV, default on)
 *   2. per-stage gate (gate_crawl / gate_upload / gate_processing)
 *   3. per-retailer opt-out (autopilot_optout:<retailer>)
 *   4. stage rules: explore→crawl needs a recommended/usable config, fires at
 *      most once per retailer per day, and respects a concurrency cap.
 */
import { getSetting, isAutoPipelineEnabled, setSetting } from "./pipelineStore.js";
import { enqueueProcessing } from "./queue.js";

export type ChainStage = "explore-completed" | "crawl-completed" | "upload-completed";

export interface AutoChainContext {
  retailer: string;
  recommendation?: string;
  log?: (msg: string) => void;
}

interface AutoChainDeps {
  startCrawlJob: (retailer: string, opts?: { auto?: boolean }) => Promise<{ jobId?: string; error?: string }>;
  enqueueUploadForRetailer: (retailer: string) => Promise<{ enqueued?: number; error?: string }>;
  /** Currently running crawl jobs started by the auto-chain (concurrency cap). */
  countRunningAutoCrawls: () => number;
}

let deps: AutoChainDeps | null = null;

export function configureAutoChain(d: AutoChainDeps): void {
  deps = d;
}

const AUTO_CRAWL_MAX_CONCURRENT = Math.max(
  1,
  parseInt(process.env.AUTO_CRAWL_MAX_CONCURRENT ?? "2", 10),
);

const STAGE_GATE: Record<ChainStage, string> = {
  "explore-completed": "gate_crawl",
  "crawl-completed": "gate_upload",
  "upload-completed": "gate_processing",
};

export async function maybeChain(stage: ChainStage, ctx: AutoChainContext): Promise<void> {
  const log = (msg: string) => {
    console.log(`[autochain] ${msg}`);
    ctx.log?.(`[autochain] ${msg}`);
  };
  try {
    if (!deps) return;
    if (!(await isAutoPipelineEnabled())) return;
    if (!(await getSetting<boolean>(STAGE_GATE[stage], true))) {
      log(`${ctx.retailer}: ${STAGE_GATE[stage]} is off — not chaining after ${stage}.`);
      return;
    }
    if (await getSetting<boolean>(`autopilot_optout:${ctx.retailer}`, false)) {
      log(`${ctx.retailer}: autopilot opt-out set — not chaining.`);
      return;
    }

    switch (stage) {
      case "explore-completed": {
        const rec = (ctx.recommendation ?? "").toLowerCase();
        if (rec !== "recommended" && rec !== "usable") {
          log(`${ctx.retailer}: recommendation "${ctx.recommendation}" — leaving crawl manual.`);
          return;
        }
        const today = new Date().toISOString().slice(0, 10);
        const stampKey = `auto_crawl_stamp:${ctx.retailer}`;
        if ((await getSetting<string>(stampKey, "")) === today) {
          log(`${ctx.retailer}: already auto-crawled today — skipping.`);
          return;
        }
        if (deps.countRunningAutoCrawls() >= AUTO_CRAWL_MAX_CONCURRENT) {
          log(`${ctx.retailer}: ${AUTO_CRAWL_MAX_CONCURRENT} auto-crawls already running — skipping (re-runs via sweep).`);
          return;
        }
        await setSetting(stampKey, today);
        const result = await deps.startCrawlJob(ctx.retailer, { auto: true });
        if (result.error) log(`${ctx.retailer}: auto-crawl failed to start: ${result.error}`);
        else log(`${ctx.retailer}: auto-crawl started (job ${result.jobId}).`);
        return;
      }
      case "crawl-completed": {
        const result = await deps.enqueueUploadForRetailer(ctx.retailer);
        if (result.error) log(`${ctx.retailer}: auto-upload enqueue failed: ${result.error}`);
        else log(`${ctx.retailer}: auto-enqueued ${result.enqueued ?? 0} upload jobs.`);
        return;
      }
      case "upload-completed": {
        const limit = Math.max(1, parseInt(process.env.PROCESS_NOBG_DEFAULT_LIMIT ?? "100", 10));
        const hourBucket = new Date().toISOString().slice(0, 13);
        const id = await enqueueProcessing(
          { kind: "nobg", limit, sweep: true },
          { singletonKey: `nobg:upload-done:${hourBucket}` },
        );
        if (id) log(`${ctx.retailer}: upload done — enqueued nobg processing batch ${id}.`);
        return;
      }
    }
  } catch (err) {
    console.error(`[autochain] ${stage} hook failed for ${ctx.retailer}:`, err);
  }
}
