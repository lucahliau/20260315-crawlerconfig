import { useCallback, useEffect, useState } from "react";
import { api, type ProcessingResponse } from "../api.ts";
import { Card, ErrorBanner } from "./ui.tsx";

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
  const [running, setRunning] = useState(false);
  const [actionMsg, setActionMsg] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setData(await api.getProcessing());
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const runPersonScan = useCallback(async () => {
    setRunning(true);
    setActionMsg(null);
    try {
      const { jobIds } = await api.runProcessing("person");
      setActionMsg(
        jobIds.person
          ? `Queued people-photo scan (job ${jobIds.person}). The home server picks it up within seconds.`
          : "Could not queue — is the queue (DATABASE_URL) configured?",
      );
      void load();
    } catch (e: unknown) {
      setActionMsg(`Failed to queue: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setRunning(false);
    }
  }, [load]);

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
  const person = data?.person ?? { scanned: 0, hidden: 0, needsScan: 0 };
  const personRate = data?.rates?.person ?? { last1h: 0, last24h: 0 };
  const pct = (n: number, d: number) => (d > 0 ? Math.round((n / d) * 100) : 0);

  return (
    <section className="space-y-6">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Post-processing</h1>
          <p className="mt-1 max-w-2xl text-sm text-gray-500">
            Background removal, CLIP embeddings, and the people-photo scan run on the MacBook against
            the shared database — progress here is read live from Postgres. Embeddings only run on
            items that already have a clean image.
          </p>
        </div>
        <button
          type="button"
          onClick={() => void runPersonScan()}
          disabled={running}
          className="shrink-0 rounded-md bg-gray-900 px-3 py-2 text-sm font-medium text-white hover:bg-gray-700 disabled:opacity-50"
          title="Scan original product photos for people/models, strip them, and hide person-only products."
        >
          {running ? "Queuing…" : "Run people-photo scan"}
        </button>
      </header>

      {actionMsg && <p className="text-xs text-gray-500">{actionMsg}</p>}
      {error && <ErrorBanner>{error}</ErrorBanner>}

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
        <StatCard
          title="People-photo scan"
          value={`${person.scanned.toLocaleString()} · ${pct(person.scanned, totals.total)}%`}
          sub={`${person.hidden.toLocaleString()} hidden (person-only) · ${person.needsScan.toLocaleString()} to scan · ${personRate.last1h.toLocaleString()}/h`}
          progress={pct(person.scanned, totals.total)}
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
