import { useCallback, useEffect, useState } from "react";
import { api, type QueueStat, type SystemsResponse } from "../api.ts";
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
