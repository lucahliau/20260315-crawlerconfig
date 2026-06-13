import { useCallback, useEffect, useState } from "react";
import {
  api,
  type ProcessingResponse,
  type QueueStat,
  type SystemsResponse,
  type WorkerTelemetry,
} from "../api.ts";
import { Card, ErrorBanner, StatusDot } from "./ui.tsx";

/** Simple cross-service health: Supabase, backend API, R2, queue, this app. */
export function SystemsView() {
  const [data, setData] = useState<SystemsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setData(await api.getSystems());
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void load();
    const t = setInterval(() => void load(), 30000);
    return () => clearInterval(t);
  }, [load]);

  if (!data && !error) return <p className="text-sm text-gray-500">Loading…</p>;

  const uptime = data ? formatUptime(data.crawler.uptimeSeconds) : "";
  const dbPct =
    data?.database.sizeMb && data.database.limitMb
      ? Math.round((data.database.sizeMb / data.database.limitMb) * 100)
      : null;

  return (
    <section className="space-y-6">
      <header>
        <h1 className="text-lg font-semibold tracking-tight">Systems</h1>
        <p className="mt-1 text-sm text-gray-500">
          Health and usage across the services this pipeline depends on. Checked every 30 seconds
          {data && <span className="text-gray-400"> · last {new Date(data.checkedAt).toLocaleTimeString()}</span>}.
        </p>
      </header>

      {error && <ErrorBanner>{error}</ErrorBanner>}

      <HomeServerCard />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <ServiceCard
          name="Supabase Postgres"
          ok={data?.database.ok ?? false}
          lines={[
            data?.database.ok
              ? `${data.database.sizeMb} MB used${data.database.limitMb ? ` of ~${data.database.limitMb} MB` : ""}${dbPct !== null ? ` (${dbPct}%)` : ""}`
              : (data?.database.error ?? "unreachable"),
            data?.database.ok ? `${Number(data.database.items).toLocaleString()} catalog items · ${data.database.latencyMs}ms` : "",
          ]}
          progress={dbPct ?? undefined}
        />
        <ServiceCard
          name="Backend API (Railway)"
          ok={data?.backend.ok ?? false}
          lines={[
            data?.backend.ok
              ? `healthy · ${data.backend.latencyMs}ms`
              : (data?.backend.error ?? `HTTP ${data?.backend.status ?? "?"}`),
          ]}
        />
        <ServiceCard
          name="Crawler (this app)"
          ok={data?.crawler.ok ?? false}
          lines={[`up ${uptime}`, `${data?.crawler.rssMb ?? 0} MB memory`]}
        />
        <ServiceCard
          name="Cloudflare R2"
          ok={data?.r2.ok ?? false}
          lines={[
            data?.r2.ok
              ? `bucket ${data.r2.bucket} reachable · ${data.r2.latencyMs}ms`
              : (data?.r2.error ?? "unreachable"),
          ]}
        />
        <ServiceCard
          name="Job queue (pg-boss)"
          ok={data?.queue.ok ?? false}
          lines={
            data?.queue.ok
              ? summarizeQueues(data.queue.queues ?? [])
              : [data?.queue.error ?? "unreachable"]
          }
        />
      </div>
    </section>
  );
}

/**
 * The home Mac that runs post-processing — health rides its 15s heartbeat
 * (worker_heartbeats.metadata.telemetry), so this works from any browser. Fetches
 * its own processing snapshot so it can live alongside the service health cards.
 */
function HomeServerCard() {
  const [data, setData] = useState<ProcessingResponse | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        setData(await api.getProcessing());
      } catch {
        // Health of the home server is best-effort; the service cards below own error display.
      }
    };
    void load();
    const t = setInterval(() => void load(), 30000);
    return () => clearInterval(t);
  }, []);

  const worker = data?.workers?.find((w) => (w.metadata.queues ?? []).includes("process-nobg"));
  const online = data?.homeServerOnline ?? false;
  const t: WorkerTelemetry | undefined = worker?.metadata.telemetry;
  const vital = (label: string, value: string, warn = false) => (
    <div key={label}>
      <p className="text-[11px] font-medium uppercase tracking-wide text-gray-500">{label}</p>
      <p className={`tnum mt-0.5 text-sm ${warn ? "text-amber-600" : "text-gray-900"}`}>{value}</p>
    </div>
  );
  return (
    <Card className="p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-3">
          <p className="text-sm font-semibold text-gray-900">Home server</p>
          <StatusDot
            tone={online ? "ok" : "warn"}
            label={
              online
                ? `online — ${worker?.hostname ?? worker?.workerId ?? "worker"} · seen ${worker?.ageSeconds ?? "?"}s ago`
                : "offline — queued jobs will wait until it's back"
            }
          />
        </div>
        {t?.updateAvailable && (
          <span className="text-xs text-amber-600">code update available — run update.sh on the Mac</span>
        )}
      </div>
      {online && t && (
        <div className="mt-3 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          {t.freeMemMb !== undefined && t.totalMemMb
            ? vital(
                "memory free",
                `${((t.freeMemMb ?? 0) / 1024).toFixed(1)} / ${(t.totalMemMb / 1024).toFixed(0)} GB`,
                (t.freeMemMb ?? 0) < 500,
              )
            : null}
          {t.diskFreeGb != null ? vital("disk free", `${t.diskFreeGb} GB`, t.diskFreeGb < 15) : null}
          {t.loadAvg1m !== undefined && t.cpuCount
            ? vital("cpu load", `${t.loadAvg1m} / ${t.cpuCount}`, t.loadAvg1m > t.cpuCount)
            : null}
          {t.cpuSpeedLimitPct != null
            ? vital("thermal", `${t.cpuSpeedLimitPct}% speed`, t.cpuSpeedLimitPct < 70)
            : null}
          {t.onACPower != null
            ? vital(
                "power",
                `${t.onACPower ? "plugged in" : "on battery"}${t.batteryPct != null ? ` · ${t.batteryPct}%` : ""}`,
                !t.onACPower,
              )
            : null}
          {vital("activity", (t.activeJobs?.length ?? 0) > 0 ? t.activeJobs!.join(", ") : "idle")}
        </div>
      )}
      {online && t && (t.recentIssues ?? 0) > 0 && (
        <p className="mt-2 text-xs text-red-600">
          {t.recentIssues} recent issue(s) — details on the Mac at localhost:4577
        </p>
      )}
    </Card>
  );
}

function summarizeQueues(queues: QueueStat[]): string[] {
  if (queues.length === 0) return ["empty"];
  return queues.slice(0, 4).map((q) => {
    const parts = [
      q.waiting > 0 ? `${q.waiting} waiting` : "",
      q.active > 0 ? `${q.active} active` : "",
      q.failed > 0 ? `${q.failed} failed` : "",
    ].filter(Boolean);
    return `${q.name}: ${parts.length ? parts.join(", ") : "idle"}`;
  });
}

function formatUptime(secs: number): string {
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
  return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`;
}

function ServiceCard({
  name,
  ok,
  lines,
  progress,
}: {
  name: string;
  ok: boolean;
  lines: string[];
  progress?: number;
}) {
  return (
    <Card className="p-4">
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium text-gray-900">{name}</p>
        <StatusDot tone={ok ? "ok" : "failed"} label={ok ? "Up" : "Down"} />
      </div>
      <div className="mt-2 space-y-0.5">
        {lines.filter(Boolean).map((l, i) => (
          <p key={i} className="tnum text-xs text-gray-500">
            {l}
          </p>
        ))}
      </div>
      {progress !== undefined && (
        <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-gray-100">
          <div
            className={`h-full rounded-full ${progress > 85 ? "bg-red-500" : progress > 70 ? "bg-amber-500" : "bg-gray-900"}`}
            style={{ width: `${Math.min(100, progress)}%` }}
          />
        </div>
      )}
    </Card>
  );
}
