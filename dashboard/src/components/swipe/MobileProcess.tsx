import { useCallback, useEffect, useState } from "react";
import { api, type ProcessingResponse } from "../../api.ts";
import { cx } from "../ui.tsx";

/**
 * Phone view of post-processing: backlog + coverage stat cards, the home
 * server (M1) heartbeat, queue depth, and Run/Cancel buttons that enqueue
 * bounded batches for the home worker to claim.
 */
export function MobileProcess() {
  const [data, setData] = useState<ProcessingResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const load = useCallback(async (silent = false) => {
    try {
      setData(await api.getProcessing());
      if (!silent) setLoadError(null);
    } catch (e: unknown) {
      if (!silent) setLoadError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void load();
    const t = setInterval(() => void load(true), 15000);
    return () => clearInterval(t);
  }, [load]);

  const run = useCallback(
    async (key: string, fn: () => Promise<string>) => {
      if (busy) return;
      setBusy(key);
      setNotice(null);
      setLoadError(null);
      try {
        setNotice(await fn());
        void load(true);
      } catch (e: unknown) {
        setLoadError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(null);
      }
    },
    [busy, load],
  );

  const totals = data?.totals;
  const backlog = data?.backlog;
  const online = data?.homeServerOnline ?? false;
  const homeWorker = data?.workers?.find((w) =>
    (w.metadata.queues ?? []).includes("process-nobg"),
  );
  const queueDepth = (name: string) =>
    data?.queues?.find((q) => q.name === name);

  return (
    <div className="flex h-full flex-col gap-4 overflow-y-auto px-5 pb-4">
      {/* Home server status */}
      <div className="flex items-center justify-between rounded-xl border border-gray-800 bg-gray-900 px-3.5 py-3">
        <div>
          <p className="text-sm font-semibold text-gray-100">Home server</p>
          <p className="text-[11px] text-gray-500">
            {online && homeWorker
              ? `${homeWorker.hostname ?? homeWorker.workerId} · seen ${homeWorker.ageSeconds}s ago`
              : "offline — queued jobs will wait"}
          </p>
        </div>
        <span
          className={cx(
            "flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-medium",
            online
              ? "border-emerald-800 bg-emerald-950/50 text-emerald-300"
              : "border-amber-800 bg-amber-950/40 text-amber-300",
          )}
        >
          <span className={cx("size-2 rounded-full", online ? "bg-emerald-500" : "bg-amber-500")} />
          {online ? "online" : "offline"}
        </span>
      </div>

      {loadError && (
        <div className="rounded-xl border border-red-900 bg-red-950/60 px-3.5 py-2.5 text-xs text-red-300">
          {loadError}
        </div>
      )}
      {notice && (
        <div className="rounded-xl border border-emerald-800 bg-emerald-950/50 px-3.5 py-2.5 text-xs text-emerald-300">
          {notice}
        </div>
      )}

      {/* Coverage cards */}
      <div className="grid grid-cols-2 gap-2">
        <StatCard
          title="Backgrounds removed"
          value={totals ? `${totals.nobg}` : "—"}
          sub={
            totals
              ? `of ${totals.total} · ${backlog ? `${backlog.needsNobg} to go` : ""}`
              : "loading…"
          }
          pct={totals && totals.total > 0 ? totals.nobg / totals.total : 0}
        />
        <StatCard
          title="Embedded"
          value={totals ? `${totals.embedded}` : "—"}
          sub={
            totals
              ? `of ${totals.total} · ${backlog ? `${backlog.needsEmbed} ready` : ""}`
              : "loading…"
          }
          pct={totals && totals.total > 0 ? totals.embedded / totals.total : 0}
        />
        <StatCard
          title="nobg rate"
          value={data ? `${data.rates.nobg.last1h}/h` : "—"}
          sub={data ? `${data.rates.nobg.last24h} in 24h` : ""}
        />
        <StatCard
          title="Embed rate"
          value={data ? `${data.rates.embeddings.last1h}/h` : "—"}
          sub={data ? `${data.rates.embeddings.last24h} in 24h` : ""}
        />
      </div>

      {/* Queue depth */}
      <div className="space-y-1.5">
        <p className="text-xs font-medium text-gray-400">Processing queues</p>
        {(["process-nobg", "process-embed"] as const).map((name) => {
          const q = queueDepth(name);
          return (
            <div
              key={name}
              className="flex items-center justify-between rounded-xl border border-gray-800 bg-gray-900 px-3 py-2.5 text-[13px]"
            >
              <span className="text-gray-200">{name === "process-nobg" ? "Background removal" : "Embeddings"}</span>
              <span className="tnum text-gray-400">
                {q ? `${q.waiting} waiting · ${q.active} active · ${q.failed} failed` : "—"}
              </span>
            </div>
          );
        })}
      </div>

      {/* Actions */}
      <div className="space-y-2">
        <button
          disabled={busy !== null}
          onClick={() =>
            void run("all", async () => {
              await api.runProcessing("all");
              return "Queued: background-removal + embedding batches for the home server.";
            })
          }
          className="flex h-12 w-full items-center justify-center rounded-xl bg-white text-[15px] font-semibold text-gray-900 active:bg-gray-200 disabled:opacity-50"
        >
          {busy === "all" ? "Queueing…" : "Run all processing"}
        </button>
        <div className="grid grid-cols-2 gap-2">
          <button
            disabled={busy !== null}
            onClick={() =>
              void run("nobg", async () => {
                await api.runProcessing("nobg");
                return "Background-removal batch queued.";
              })
            }
            className="flex h-11 items-center justify-center rounded-xl border border-gray-700 bg-gray-900 text-sm font-medium text-gray-200 active:bg-gray-800 disabled:opacity-50"
          >
            {busy === "nobg" ? "Queueing…" : "Remove backgrounds"}
          </button>
          <button
            disabled={busy !== null}
            onClick={() =>
              void run("embed", async () => {
                await api.runProcessing("embed");
                return "Embedding batch queued.";
              })
            }
            className="flex h-11 items-center justify-center rounded-xl border border-gray-700 bg-gray-900 text-sm font-medium text-gray-200 active:bg-gray-800 disabled:opacity-50"
          >
            {busy === "embed" ? "Queueing…" : "Embed items"}
          </button>
        </div>
        <div className="grid grid-cols-2 gap-2">
          {(["nobg", "embed"] as const).map((kind) => (
            <button
              key={kind}
              disabled={busy !== null}
              onClick={() =>
                void run(`cancel-${kind}`, async () => {
                  const r = await api.cancelProcessing(kind);
                  return `Cancelled ${r.cancelled} queued ${kind} job(s).`;
                })
              }
              className="flex h-10 items-center justify-center rounded-xl border border-gray-800 bg-gray-950 text-xs font-medium text-gray-500 active:bg-gray-900 disabled:opacity-50"
            >
              Cancel queued {kind}
            </button>
          ))}
        </div>
        <p className="text-center text-[11px] text-gray-600">
          Jobs run on the M1 at home; the nightly sweep also picks up any backlog.
        </p>
      </div>
    </div>
  );
}

function StatCard({
  title,
  value,
  sub,
  pct,
}: {
  title: string;
  value: string;
  sub?: string;
  pct?: number;
}) {
  return (
    <div className="space-y-1 rounded-xl border border-gray-800 bg-gray-900 p-3">
      <p className="text-[11px] font-medium text-gray-500">{title}</p>
      <p className="tnum text-lg font-semibold text-gray-100">{value}</p>
      {sub && <p className="truncate text-[11px] text-gray-500">{sub}</p>}
      {pct !== undefined && (
        <div className="h-1.5 overflow-hidden rounded-full bg-gray-800">
          <div
            className="h-full rounded-full bg-emerald-500"
            style={{ width: `${Math.round(Math.min(1, Math.max(0, pct)) * 100)}%` }}
          />
        </div>
      )}
    </div>
  );
}
