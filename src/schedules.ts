/**
 * pg-boss cron schedules — registered by the always-on Railway server; the
 * resulting jobs are claimed by whichever worker owns each queue (the home
 * M1 for processing, the server itself for the weekly re-crawl sweep).
 *
 * Pause behavior: sweep jobs carry `sweep: true`; the claiming worker checks
 * the pipeline_settings kill switch at claim time, so a paused sweep
 * completes as a logged no-op rather than being conditionally scheduled.
 */
import { getBoss, QUEUES } from "./queue.js";

const TZ = process.env.SCHEDULE_TZ ?? "Europe/Paris";

export async function registerSchedules(): Promise<void> {
  const boss = await getBoss();
  if (!boss) {
    console.warn("[schedules] queue disabled — schedules not registered.");
    return;
  }
  const nobgLimit = Math.max(1, parseInt(process.env.PROCESS_NOBG_SWEEP_LIMIT ?? "200", 10));
  const embedLimit = Math.max(1, parseInt(process.env.PROCESS_EMBED_SWEEP_LIMIT ?? "5000", 10));
  const personLimit = Math.max(1, parseInt(process.env.PROCESS_PERSON_SWEEP_LIMIT ?? "4000", 10));

  // boss.schedule upserts the cron per queue — idempotent across restarts.
  await boss.schedule(
    QUEUES.PROCESS_NOBG,
    "0 1 * * *",
    { kind: "nobg", limit: nobgLimit, sweep: true },
    { tz: TZ },
  );
  await boss.schedule(
    QUEUES.PROCESS_EMBED,
    "0 4 * * *",
    { kind: "embed", limit: embedLimit, sweep: true },
    { tz: TZ },
  );
  // People-photo scan twice daily (02:00 + 14:00) — catches model/lifestyle
  // shots in newly-added brands without waiting a full day. The M1 worker also
  // picks up unscanned items continuously via its idle-backlog loop.
  await boss.schedule(
    QUEUES.PROCESS_PERSON,
    "0 2,14 * * *",
    { kind: "person", limit: personLimit, sweep: true },
    { tz: TZ },
  );
  await boss.schedule(QUEUES.PIPELINE_SWEEP, "0 6 * * 1", { kind: "weekly-recrawl" }, { tz: TZ });
  // Daily price/sale refresh: cheap /products.json listing sweep (no image
  // traffic) so catalog prices + sales track the storefronts within a day.
  await boss.schedule(QUEUES.PRICE_REFRESH, "0 5 * * *", { sweep: true }, { tz: TZ });
  console.log(
    `[schedules] registered: nobg nightly 01:00 (limit ${nobgLimit}), embed nightly 04:00 (limit ${embedLimit}), person-scan 02:00+14:00 (limit ${personLimit}), weekly re-crawl Mon 06:00, daily price refresh 05:00 (${TZ}).`,
  );
}
