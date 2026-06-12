import { useCallback, useEffect, useState } from "react";
import { api, type ProcessingResponse, type WorkerTelemetry } from "../api.ts";
import { Card, ErrorBanner, StatusDot } from "./ui.tsx";

/**
 * Stage 4 — Process. After upload, the MacBook workers take over: background
 * removal (writes -nobg.png to R2, sets ClothingItem.hasNobg) and CLIP
 * embeddings (ItemEmbedding rows). Both write to the shared Postgres, so this
 * view reads coverage and throughput straight from the database — the laptop
 * needs no integration beyond what it already does.
 */
export function ProcessView() {
  const [data, setData] = useState<ProcessingResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setData(await api.getProcessing());
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

  const totals = data?.totals ?? { total: 0, nobg: 0, embedded: 0 };
  const rates = data?.rates ?? {
    nobg: { last1h: 0, last24h: 0 },
    embeddings: { last1h: 0, last24h: 0 },
  };
  const pct = (n: number, d: number) => (d > 0 ? Math.round((n / d) * 100) : 0);

  return (
    <section className="space-y-6">
      <header>
        <h1 className="text-lg font-semibold tracking-tight">Post-processing</h1>
        <p className="mt-1 max-w-2xl text-sm text-gray-500">
          Background removal and CLIP embeddings run on the MacBook against the shared database —
          progress here is read live from Postgres. Embeddings only run on items that already have a
          clean image.
        </p>
      </header>

      {error && <ErrorBanner>{error}</ErrorBanner>}

      <HomeServerCard data={data} />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <StatCard
          title="Catalog items"
          value={totals.total.toLocaleString()}
          sub="uploaded by the crawler"
        />
        <StatCard
          title="Background removed"
          value={`${totals.nobg.toLocaleString()} · ${pct(totals.nobg, totals.total)}%`}
          sub={`${rates.nobg.last1h.toLocaleString()} in the last hour · ${rates.nobg.last24h.toLocaleString()} in 24h`}
          progress={pct(totals.nobg, totals.total)}
        />
        <StatCard
          title="Embedded"
          value={`${totals.embedded.toLocaleString()} · ${pct(totals.embedded, totals.nobg)}%`}
          sub={`of clean images · ${rates.embeddings.last1h.toLocaleString()} in the last hour · ${rates.embeddings.last24h.toLocaleString()} in 24h`}
          progress={pct(totals.embedded, totals.nobg)}
        />
      </div>

      <Card className="overflow-x-auto">
        <table className="w-full min-w-[640px] text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50 text-left text-[11px] font-medium uppercase tracking-wide text-gray-500">
              <th className="px-4 py-2.5">Retailer</th>
              <th className="px-4 py-2.5 text-right">Items</th>
              <th className="px-4 py-2.5 text-right">BG removed</th>
              <th className="px-4 py-2.5 text-right">Embedded</th>
              <th className="w-44 px-4 py-2.5">Coverage</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {(data?.perRetailer ?? []).map((r) => (
              <tr key={r.retailer} className="transition-colors hover:bg-gray-50">
                <td className="px-4 py-2 text-sm font-medium text-gray-900">{r.retailer}</td>
                <td className="tnum px-4 py-2 text-right text-xs text-gray-700">
                  {r.total.toLocaleString()}
                </td>
                <td className="tnum px-4 py-2 text-right text-xs text-gray-700">
                  {r.nobg.toLocaleString()}{" "}
                  <span className="text-gray-400">({pct(r.nobg, r.total)}%)</span>
                </td>
                <td className="tnum px-4 py-2 text-right text-xs text-gray-700">
                  {r.embedded.toLocaleString()}{" "}
                  <span className="text-gray-400">({pct(r.embedded, r.total)}%)</span>
                </td>
                <td className="px-4 py-2">
                  <MiniBar segments={[pct(r.embedded, r.total), pct(r.nobg, r.total)]} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </section>
  );
}

/**
 * The home Mac that runs processing — health rides its 15s heartbeat
 * (worker_heartbeats.metadata.telemetry), so this works from any browser.
 */
function HomeServerCard({ data }: { data: ProcessingResponse | null }) {
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
          {vital(
            "activity",
            (t.activeJobs?.length ?? 0) > 0 ? t.activeJobs!.join(", ") : "idle",
          )}
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

function StatCard({
  title,
  value,
  sub,
  progress,
}: {
  title: string;
  value: string;
  sub: string;
  progress?: number;
}) {
  return (
    <Card className="p-4">
      <p className="text-[11px] font-medium uppercase tracking-wide text-gray-500">{title}</p>
      <p className="tnum mt-1 text-xl font-semibold text-gray-900">{value}</p>
      <p className="mt-0.5 text-xs text-gray-400">{sub}</p>
      {progress !== undefined && (
        <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-gray-100">
          <div className="h-full rounded-full bg-gray-900" style={{ width: `${progress}%` }} />
        </div>
      )}
    </Card>
  );
}

/** Two stacked coverage bars: dark = embedded, light = bg-removed. */
function MiniBar({ segments }: { segments: [number, number] }) {
  const [embedded, nobg] = segments;
  return (
    <div className="relative h-1.5 overflow-hidden rounded-full bg-gray-100" title={`bg ${nobg}% · embedded ${embedded}%`}>
      <div className="absolute inset-y-0 left-0 rounded-full bg-gray-300" style={{ width: `${nobg}%` }} />
      <div className="absolute inset-y-0 left-0 rounded-full bg-gray-900" style={{ width: `${embedded}%` }} />
    </div>
  );
}
