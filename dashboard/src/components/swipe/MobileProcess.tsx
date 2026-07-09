import { useCallback, useEffect, useState } from "react";
import { api, type ProcessKind, type ProcessingResponse, type WorkerTelemetry } from "../../api.ts";
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
      {/* Home server status + machine telemetry from its heartbeat */}
      <div className="space-y-2.5 rounded-xl border border-gray-800 bg-gray-900 px-3.5 py-3">
        <div className="flex items-center justify-between">
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
        {online && homeWorker?.metadata.telemetry && (
          <MachineVitals t={homeWorker.metadata.telemetry} />
        )}
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
        <StatCard
          title="People scanned"
          value={data?.person ? `${data.person.scanned}` : "—"}
          sub={
            data?.person
              ? `${data.person.hidden} hidden · ${data.person.needsScan} to scan`
              : "loading…"
          }
          pct={
            data?.person && totals && totals.total > 0 ? data.person.scanned / totals.total : 0
          }
        />
      </div>

      {/* Per-process control rows */}
      <div className="space-y-2">
        <p className="text-xs font-medium text-gray-400">Processes</p>
        {PROCESS_ROWS.map((row) => (
          <ProcessRow
            key={row.kind}
            def={row}
            data={data}
            queue={queueDepth(row.queue)}
            activeJobs={homeWorker?.metadata.telemetry?.activeJobs ?? []}
            busy={busy}
            run={run}
          />
        ))}
        <p className="text-[11px] text-gray-600">
          <span className="text-gray-500">On</span> = runs automatically (nightly sweeps, idle
          backlog, chained batches). <span className="text-gray-500">Start</span> queues a batch now
          regardless. <span className="text-gray-500">Prioritise</span> pauses the other processes
          until this one's backlog drains (then clears itself).
        </p>
      </div>

      {/* Bulk action */}
      <div className="space-y-2">
        <button
          disabled={busy !== null}
          onClick={() =>
            void run("all", async () => {
              await api.runProcessing("all");
              return "Queued: nobg + embed + person batches for the home server.";
            })
          }
          className="flex h-12 w-full items-center justify-center rounded-xl bg-white text-[15px] font-semibold text-gray-900 active:bg-gray-200 disabled:opacity-50"
        >
          {busy === "all" ? "Queueing…" : "Run all processing"}
        </button>
        <p className="text-center text-[11px] text-gray-600">
          Jobs run on the M1 at home; the nightly sweep also picks up any backlog.
        </p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-process rows
// ---------------------------------------------------------------------------

interface ProcessRowDef {
  kind: ProcessKind;
  queue: "process-nobg" | "process-embed" | "process-person";
  label: string;
  backlogOf: (d: ProcessingResponse) => number | undefined;
  rateOf: (d: ProcessingResponse) => string | undefined;
}

const PROCESS_ROWS: ProcessRowDef[] = [
  {
    kind: "nobg",
    queue: "process-nobg",
    label: "Background removal",
    backlogOf: (d) => d.backlog?.needsNobg,
    rateOf: (d) => `${d.rates.nobg.last1h}/h`,
  },
  {
    kind: "embed",
    queue: "process-embed",
    label: "Embeddings",
    backlogOf: (d) => d.backlog?.needsEmbed,
    rateOf: (d) => `${d.rates.embeddings.last1h}/h`,
  },
  {
    kind: "person",
    queue: "process-person",
    label: "People-photo scan",
    backlogOf: (d) => d.backlog?.needsPerson ?? d.person?.needsScan,
    rateOf: (d) => (d.rates.person ? `${d.rates.person.last1h}/h` : undefined),
  },
];

function ProcessRow({
  def,
  data,
  queue,
  activeJobs,
  busy,
  run,
}: {
  def: ProcessRowDef;
  data: ProcessingResponse | null;
  queue: { waiting: number; active: number; failed: number } | undefined;
  activeJobs: string[];
  busy: string | null;
  run: (key: string, fn: () => Promise<string>) => Promise<void>;
}) {
  const controls = data?.controls;
  const enabled = controls?.enabled?.[def.kind] ?? true;
  const prioritised = controls?.priority === def.kind;
  const otherPrioritised = !!controls?.priority && !prioritised;
  // "Actively processing" = the home worker is executing a batch of this kind
  // right now (heartbeat), or pg-boss shows a claimed job on its queue.
  const processing = activeJobs.includes(def.kind) || (queue?.active ?? 0) > 0;
  const waiting = queue?.waiting ?? 0;
  const backlog = data ? def.backlogOf(data) : undefined;
  const rate = data ? def.rateOf(data) : undefined;

  return (
    <div
      className={cx(
        "space-y-2 rounded-xl border px-3 py-2.5",
        prioritised ? "border-amber-700 bg-amber-950/20" : "border-gray-800 bg-gray-900",
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <p className="truncate text-[13px] font-semibold text-gray-100">{def.label}</p>
            {processing ? (
              <span className="flex items-center gap-1 rounded-full border border-blue-800 bg-blue-950/60 px-2 py-0.5 text-[10px] font-medium text-blue-300">
                <span className="size-1.5 animate-pulse rounded-full bg-blue-400" />
                processing
              </span>
            ) : waiting > 0 ? (
              <span className="rounded-full border border-gray-700 bg-gray-950 px-2 py-0.5 text-[10px] font-medium text-gray-400">
                queued
              </span>
            ) : null}
            {prioritised && (
              <span className="rounded-full border border-amber-700 bg-amber-950/60 px-2 py-0.5 text-[10px] font-medium text-amber-300">
                priority
              </span>
            )}
            {otherPrioritised && (
              <span className="rounded-full border border-gray-800 bg-gray-950 px-2 py-0.5 text-[10px] font-medium text-gray-600">
                deferred
              </span>
            )}
          </div>
          <p className="tnum mt-0.5 text-[11px] text-gray-500">
            {waiting} queued · {queue?.active ?? 0} active
            {backlog !== undefined ? ` · ${backlog} backlog` : ""}
            {rate ? ` · ${rate}` : ""}
            {(queue?.failed ?? 0) > 0 ? ` · ${queue!.failed} failed` : ""}
          </p>
        </div>
        {/* On/off: gates automatic runs (sweep/idle/chaining) for this process */}
        <button
          disabled={busy !== null || !controls}
          onClick={() =>
            void run(`toggle-${def.kind}`, async () => {
              await api.toggleProcessing(def.kind, !enabled);
              return `${def.label} automation ${!enabled ? "ON" : "OFF"}.`;
            })
          }
          className={cx(
            "relative h-6 w-11 shrink-0 rounded-full transition-colors disabled:opacity-50",
            enabled ? "bg-emerald-600" : "bg-gray-700",
          )}
          aria-label={`${def.label} automation ${enabled ? "on" : "off"}`}
        >
          <span
            className={cx(
              "absolute top-0.5 size-5 rounded-full bg-white transition-all",
              enabled ? "left-[22px]" : "left-0.5",
            )}
          />
        </button>
      </div>

      <div className="grid grid-cols-3 gap-1.5">
        <button
          disabled={busy !== null}
          onClick={() =>
            void run(`start-${def.kind}`, async () => {
              await api.runProcessing(def.kind);
              return `${def.label} batch queued.`;
            })
          }
          className="flex h-9 items-center justify-center rounded-lg border border-gray-700 bg-gray-950 text-xs font-semibold text-gray-100 active:bg-gray-800 disabled:opacity-50"
        >
          {busy === `start-${def.kind}` ? "…" : "Start"}
        </button>
        <button
          disabled={busy !== null || (waiting === 0 && !processing)}
          onClick={() =>
            void run(`stop-${def.kind}`, async () => {
              const r = await api.cancelProcessing(def.kind);
              return `Stopped ${def.label.toLowerCase()}: ${r.cancelled} queued job(s) cancelled${
                processing ? " — the running batch finishes, then no more chain" : ""
              }.`;
            })
          }
          className="flex h-9 items-center justify-center rounded-lg border border-gray-800 bg-gray-950 text-xs font-medium text-gray-400 active:bg-gray-900 disabled:opacity-40"
        >
          {busy === `stop-${def.kind}` ? "…" : "Stop"}
        </button>
        <button
          disabled={busy !== null || !controls}
          onClick={() =>
            void run(`prio-${def.kind}`, async () => {
              const r = await api.setProcessingPriority(prioritised ? null : def.kind);
              return r.priority
                ? `${def.label} prioritised — other processes wait until its backlog drains.`
                : "Priority cleared — all processes resume.";
            })
          }
          className={cx(
            "flex h-9 items-center justify-center rounded-lg border text-xs font-semibold disabled:opacity-50",
            prioritised
              ? "border-amber-600 bg-amber-900/40 text-amber-200 active:bg-amber-900/60"
              : "border-gray-700 bg-gray-950 text-gray-300 active:bg-gray-800",
          )}
        >
          {busy === `prio-${def.kind}` ? "…" : prioritised ? "Unprioritise" : "Prioritise"}
        </button>
      </div>
    </div>
  );
}

/** Compact machine-vitals grid for the home server (memory/disk/load/thermal/power). */
function MachineVitals({ t }: { t: WorkerTelemetry }) {
  const vitals: { label: string; value: string; warn?: boolean }[] = [];
  if (t.freeMemMb !== undefined && t.totalMemMb) {
    vitals.push({
      label: "memory free",
      value: `${((t.freeMemMb ?? 0) / 1024).toFixed(1)} / ${(t.totalMemMb / 1024).toFixed(0)} GB`,
      warn: (t.freeMemMb ?? 0) < 500,
    });
  }
  if (t.diskFreeGb != null) {
    vitals.push({ label: "disk free", value: `${t.diskFreeGb} GB`, warn: t.diskFreeGb < 15 });
  }
  if (t.loadAvg1m !== undefined && t.cpuCount) {
    vitals.push({
      label: "cpu load",
      value: `${t.loadAvg1m} / ${t.cpuCount}`,
      warn: t.loadAvg1m > t.cpuCount,
    });
  }
  if (t.cpuSpeedLimitPct != null) {
    vitals.push({
      label: "thermal",
      value: `${t.cpuSpeedLimitPct}% speed`,
      warn: t.cpuSpeedLimitPct < 70,
    });
  }
  if (t.onACPower != null) {
    vitals.push({
      label: "power",
      value: t.onACPower
        ? `plugged in${t.batteryPct != null ? ` · ${t.batteryPct}%` : ""}`
        : `on battery${t.batteryPct != null ? ` · ${t.batteryPct}%` : ""}`,
      warn: !t.onACPower,
    });
  }
  return (
    <div className="space-y-1.5">
      <div className="grid grid-cols-2 gap-1.5">
        {vitals.map((v) => (
          <div key={v.label} className="rounded-lg bg-gray-950 px-2.5 py-1.5">
            <p className="text-[10px] text-gray-600">{v.label}</p>
            <p className={cx("text-[12px]", v.warn ? "text-amber-300" : "text-gray-300")}>{v.value}</p>
          </div>
        ))}
      </div>
      {(t.activeJobs?.length ?? 0) > 0 && (
        <p className="text-[11px] text-blue-300">working on: {t.activeJobs!.join(", ")}</p>
      )}
      {(t.recentIssues ?? 0) > 0 && (
        <p className="text-[11px] text-red-400">{t.recentIssues} recent issue(s) — see localhost:4577 on the M1</p>
      )}
      {t.updateAvailable && (
        <p className="text-[11px] text-amber-300">code update available — run update.sh on the M1</p>
      )}
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
