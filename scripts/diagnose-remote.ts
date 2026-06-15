/**
 * Remote diagnosis of the M1 home worker — reads the shared Supabase Postgres
 * that the worker mirrors all its state into (worker_heartbeats, worker_issues,
 * scrape_errors). Run from ANY machine with DATABASE_URL in .env; you do not
 * need to be on the M1 or know DASHBOARD_PASSWORD.
 *
 *   npx tsx scripts/diagnose-remote.ts
 */
import "dotenv/config";
import { getPool } from "../src/pipelineStore.js";

function ago(ts: unknown): string {
  if (!ts) return "never";
  const ms = Date.now() - new Date(String(ts)).getTime();
  const s = Math.round(ms / 1000);
  if (s < 90) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 90) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 48) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

function hr(title: string) {
  console.log(`\n${"=".repeat(72)}\n${title}\n${"=".repeat(72)}`);
}

async function main() {
  const pg = getPool();
  if (!pg) {
    console.error("DATABASE_URL not set — cannot reach the shared Postgres.");
    process.exit(1);
  }

  // ---- 1. Worker liveness + telemetry -----------------------------------
  hr("WORKER HEARTBEATS (who is online + machine telemetry)");
  const hb = await pg.query(
    `SELECT worker_id, hostname, pid, concurrency, metadata, first_seen_at, last_seen_at
       FROM worker_heartbeats ORDER BY last_seen_at DESC`,
  );
  if (hb.rows.length === 0) console.log("(no heartbeats — no worker has ever checked in)");
  for (const r of hb.rows) {
    const m = (r.metadata ?? {}) as Record<string, any>;
    const t = (m.telemetry ?? m) as Record<string, any>;
    const stale = Date.now() - new Date(String(r.last_seen_at)).getTime() > 90_000;
    console.log(
      `\n  ${stale ? "🔴 STALE" : "🟢 LIVE "} ${r.worker_id}  (${r.hostname ?? "?"}, pid ${r.pid ?? "?"}, conc ${r.concurrency})`,
    );
    console.log(`     last seen: ${ago(r.last_seen_at)}   first: ${ago(r.first_seen_at)}`);
    const fields = [
      ["queues", t.queues ?? m.queues],
      ["activeJobs", t.activeJobs],
      ["backlog", t.backlog],
      ["commit", t.commit],
      ["updateAvailable", t.updateAvailable],
      ["freeMemMb", t.freeMemMb ?? t.mem],
      ["diskFreeGb", t.diskFreeGb ?? t.disk],
      ["thermal", t.thermal ?? t.cpuSpeedLimit],
      ["power", t.power],
      ["battery", t.battery ?? t.batteryPct],
      ["load", t.load],
      ["recentIssues", Array.isArray(t.recentIssues) ? t.recentIssues.length : t.recentIssues],
    ].filter(([, v]) => v !== undefined && v !== null);
    for (const [k, v] of fields) {
      console.log(`     ${String(k).padEnd(16)} ${typeof v === "object" ? JSON.stringify(v) : v}`);
    }
  }

  // ---- 2. Worker operational issues -------------------------------------
  for (const days of [1, 7]) {
    hr(`WORKER ISSUES — grouped, last ${days}d (durable mirror of localhost:4577 feed)`);
    const grouped = await pg.query(
      `SELECT source, severity, message, COUNT(*)::int AS n,
              MAX(occurred_at) AS last_at,
              (ARRAY_AGG(metadata ORDER BY occurred_at DESC))[1] AS sample
         FROM worker_issues
        WHERE occurred_at > NOW() - ($1 || ' days')::interval
        GROUP BY source, severity, message
        ORDER BY n DESC LIMIT 25`,
      [String(days)],
    );
    if (grouped.rows.length === 0) {
      console.log(`(no worker issues in the last ${days}d)`);
      continue;
    }
    for (const r of grouped.rows) {
      console.log(
        `\n  [${r.severity}] ×${r.n}  ${r.source ? `(${r.source}) ` : ""}${ago(r.last_at)}\n    ${r.message}`,
      );
      const s = r.sample as Record<string, any> | null;
      if (s && Object.keys(s).length) {
        const str = JSON.stringify(s);
        console.log(`    meta: ${str.length > 400 ? str.slice(0, 400) + "…" : str}`);
      }
    }
    break; // 7d view is a superset; only print one window of grouped detail
  }

  // ---- 3. Scrape errors (per-product faults) ----------------------------
  for (const days of [1, 7]) {
    hr(`SCRAPE ERRORS — last ${days}d`);
    const total = await pg.query(
      `SELECT COUNT(*)::int AS n FROM scrape_errors WHERE occurred_at > NOW() - ($1 || ' days')::interval`,
      [String(days)],
    );
    console.log(`  total: ${total.rows[0].n}`);
    if (total.rows[0].n === 0) continue;
    for (const dim of ["code", "stage", "retailer"]) {
      const g = await pg.query(
        `SELECT COALESCE(${dim}::text,'(null)') AS k, COUNT(*)::int AS n
           FROM scrape_errors WHERE occurred_at > NOW() - ($1 || ' days')::interval
          GROUP BY ${dim} ORDER BY n DESC LIMIT 12`,
        [String(days)],
      );
      console.log(`\n  by ${dim}:`);
      for (const r of g.rows) console.log(`     ${String(r.n).padStart(5)}  ${r.k}`);
    }
  }

  hr("RECENT SCRAPE ERRORS (latest 15, raw)");
  const recent = await pg.query(
    `SELECT occurred_at, code, stage, retailer, attempt, url, detail
       FROM scrape_errors ORDER BY occurred_at DESC LIMIT 15`,
  );
  for (const r of recent.rows) {
    console.log(
      `\n  ${ago(r.occurred_at)}  [${r.code}] ${r.stage}${r.retailer ? ` · ${r.retailer}` : ""} (try ${r.attempt})`,
    );
    if (r.url) console.log(`    url: ${r.url}`);
    if (r.detail) console.log(`    ${String(r.detail).slice(0, 300)}`);
  }

  await pg.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
