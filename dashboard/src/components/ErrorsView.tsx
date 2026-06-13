import { useCallback, useEffect, useMemo, useState } from "react";
import {
  api,
  type ErrorsResponse,
  type ScrapeError,
  type WorkerIssue,
} from "../api.ts";
import { Button, Card, EmptyState, ErrorBanner, Segmented, StatusDot, Spinner, type Tone } from "./ui.tsx";

/**
 * Errors tab — one place to diagnose what's breaking, from any machine.
 *
 * Two durable feeds, both read from the shared Postgres so they're visible
 * remotely (not just on the M1's localhost:4577):
 *   • Pipeline errors  → scrape_errors  (per-product explore/crawl/upload faults)
 *   • Worker issues    → worker_issues  (operational: heartbeat, batch failures,
 *                         config purges, auto-update — mirrored from the home worker)
 */

type Feed = "pipeline" | "worker";

const WINDOWS: { key: string; label: string; minutes: number }[] = [
  { key: "1h", label: "1h", minutes: 60 },
  { key: "24h", label: "24h", minutes: 1440 },
  { key: "7d", label: "7d", minutes: 10080 },
];

/** Warn-tone (recoverable / external) vs failed-tone (a real fault). */
function codeTone(code: string): Tone {
  const c = code.toUpperCase();
  if (c.includes("RATE_LIMITED") || c.includes("BLOCKED") || c.includes("TIMEOUT")) return "warn";
  if (c.includes("CONFIG_INVALID") || c.includes("NO_") || c.includes("NETWORK")) return "warn";
  return "failed";
}

export function ErrorsView() {
  const [feed, setFeed] = useState<Feed>("pipeline");
  const [windowKey, setWindowKey] = useState("24h");
  const [errors, setErrors] = useState<ErrorsResponse | null>(null);
  const [issues, setIssues] = useState<WorkerIssue[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [stageFilter, setStageFilter] = useState<string | null>(null);

  const minutes = WINDOWS.find((w) => w.key === windowKey)?.minutes ?? 1440;

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [e, w] = await Promise.all([
        api.getErrors({ sinceMinutes: minutes, limit: 300 }),
        api.getWorkerIssues({ sinceMinutes: minutes, limit: 300 }),
      ]);
      setErrors(e);
      setIssues(w.issues);
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [minutes]);

  useEffect(() => {
    void load();
    const t = setInterval(() => void load(), 20000);
    return () => clearInterval(t);
  }, [load]);

  // Reset the stage filter when the window changes or it no longer applies.
  useEffect(() => setStageFilter(null), [windowKey, feed]);

  const stages = useMemo(() => {
    const counts = errors?.counts.byStage ?? {};
    return Object.entries(counts).sort((a, b) => b[1] - a[1]);
  }, [errors]);

  const visibleErrors = useMemo(() => {
    const list = errors?.errors ?? [];
    return stageFilter ? list.filter((e) => e.stage === stageFilter) : list;
  }, [errors, stageFilter]);

  return (
    <section className="space-y-6">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Errors</h1>
          <p className="mt-1 text-sm text-gray-500">
            Everything that broke, from any machine. Auto-refreshes every 20s
            {loading && (
              <span className="ml-2 inline-flex items-center gap-1 text-gray-400">
                <Spinner /> refreshing
              </span>
            )}
            .
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Segmented
            items={WINDOWS.map((w) => ({ key: w.key, label: w.label }))}
            active={windowKey}
            onChange={setWindowKey}
          />
          <Button variant="secondary" size="sm" onClick={() => void load()}>
            Refresh
          </Button>
        </div>
      </header>

      {error && <ErrorBanner onDismiss={() => setError(null)}>{error}</ErrorBanner>}

      <Segmented
        items={[
          { key: "pipeline", label: "Pipeline errors", count: errors?.counts.total ?? undefined },
          { key: "worker", label: "Worker issues", count: issues?.length ?? undefined },
        ]}
        active={feed}
        onChange={(k) => setFeed(k as Feed)}
      />

      {feed === "pipeline" ? (
        <div className="space-y-4">
          {stages.length > 0 && (
            <div className="flex flex-wrap items-center gap-2">
              <button
                onClick={() => setStageFilter(null)}
                className={chipClass(stageFilter === null)}
              >
                All <span className="tnum text-gray-400">{errors?.counts.total ?? 0}</span>
              </button>
              {stages.map(([stage, n]) => (
                <button
                  key={stage}
                  onClick={() => setStageFilter(stage === stageFilter ? null : stage)}
                  className={chipClass(stageFilter === stage)}
                >
                  {stage} <span className="tnum text-gray-400">{n}</span>
                </button>
              ))}
            </div>
          )}
          {visibleErrors.length === 0 ? (
            <EmptyState>No pipeline errors in this window. 🎉</EmptyState>
          ) : (
            <Card>
              <ul className="divide-y divide-gray-100">
                {visibleErrors.map((e, i) => (
                  <ScrapeErrorRow key={`${e.occurredAt}-${i}`} e={e} />
                ))}
              </ul>
            </Card>
          )}
        </div>
      ) : (issues?.length ?? 0) === 0 ? (
        <EmptyState>
          No worker issues in this window. If the home worker is offline, nothing reports here —
          check the Process tab.
        </EmptyState>
      ) : (
        <Card>
          <ul className="divide-y divide-gray-100">
            {issues!.map((w, i) => (
              <WorkerIssueRow key={`${w.occurredAt}-${i}`} w={w} />
            ))}
          </ul>
        </Card>
      )}
    </section>
  );
}

function chipClass(active: boolean): string {
  return [
    "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition-colors",
    active
      ? "border-gray-900 bg-gray-900 text-white"
      : "border-gray-200 bg-white text-gray-600 hover:border-gray-300 hover:text-gray-900",
  ].join(" ");
}

function ScrapeErrorRow({ e }: { e: ScrapeError }) {
  return (
    <li className="px-4 py-3">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <StatusDot tone={codeTone(e.code)} label={<span className="font-medium">{e.code}</span>} />
        <span className="tnum text-xs text-gray-400">{fmtTime(e.occurredAt)}</span>
        {e.retailer && (
          <span className="rounded bg-gray-100 px-1.5 py-0.5 text-xs text-gray-600">{e.retailer}</span>
        )}
        <span className="text-xs text-gray-400">{e.stage}</span>
        {e.attempt > 1 && <span className="text-xs text-gray-400">attempt {e.attempt}</span>}
      </div>
      <p className="mt-1 break-words font-mono text-xs text-gray-700">{e.detail || "—"}</p>
      {e.url && (
        <a
          href={e.url}
          target="_blank"
          rel="noreferrer"
          className="mt-0.5 inline-block max-w-full truncate break-all text-xs text-blue-600 hover:underline"
        >
          {e.url}
        </a>
      )}
    </li>
  );
}

function WorkerIssueRow({ w }: { w: WorkerIssue }) {
  const tone: Tone = w.severity === "info" ? "info" : w.severity === "warn" ? "warn" : "failed";
  return (
    <li className="px-4 py-3">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <StatusDot tone={tone} label={<span className="font-medium">{w.source || "worker"}</span>} />
        <span className="tnum text-xs text-gray-400">{fmtTime(w.occurredAt)}</span>
        {w.workerId && <span className="text-xs text-gray-400">{w.workerId}</span>}
      </div>
      <p className="mt-1 break-words font-mono text-xs text-gray-700">{w.message || "—"}</p>
    </li>
  );
}

function fmtTime(iso?: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
