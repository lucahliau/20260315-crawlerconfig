import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  subscribeStream,
  type ErrorsResponse,
  type Recommendation,
  type RetailerRow,
  type RetailersOverviewResponse,
} from "../api.ts";

/**
 * Retailer operations: one dense row per retailer covering the whole
 * explore → crawl → upload pipeline, built to stay usable at 100+ sites.
 * Filter chips + search narrow the table; every long-running action streams
 * its log into the bottom drawer over SSE.
 */

type StageFilter = "all" | "running" | "failed" | "needs_crawl" | "needs_upload" | "done";

const FILTERS: { key: StageFilter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "running", label: "Running" },
  { key: "failed", label: "Failed" },
  { key: "needs_crawl", label: "Needs crawl" },
  { key: "needs_upload", label: "Needs upload" },
  { key: "done", label: "Up to date" },
];

const REC_OPTIONS: Recommendation[] = ["recommended", "usable", "not recommended"];

function hostOf(url: string | undefined): string {
  if (!url) return "";
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

function timeAgo(iso: string | null | undefined): string {
  if (!iso) return "";
  const then = new Date(iso).getTime();
  if (!Number.isFinite(then)) return "";
  const secs = Math.max(0, Math.floor((Date.now() - then) / 1000));
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

/** Where a retailer sits in the pipeline — drives the filter chips and row hints. */
function stageOf(r: RetailerRow): StageFilter {
  if (
    r.exploreStatus === "running" ||
    r.crawlLive?.status === "running" ||
    r.uploadLive?.status === "running"
  ) {
    return "running";
  }
  if (
    r.exploreStatus === "failed" ||
    r.exploreStatus === "needs_retry" ||
    r.exploreStatus === "queued_retry" ||
    r.crawlLive?.status === "failed" ||
    r.uploadLive?.status === "failed"
  ) {
    return "failed";
  }
  if (!r.crawl) return "needs_crawl";
  if (!r.upload || !r.uploadMatchesCurrentCrawl) return "needs_upload";
  return "done";
}

export function RetailersView() {
  const [data, setData] = useState<RetailersOverviewResponse | null>(null);
  const [errors, setErrors] = useState<ErrorsResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<StageFilter>("all");
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [showBacklog, setShowBacklog] = useState(true);

  // Live job log drawer.
  const [activeJob, setActiveJob] = useState<{ id: string; title: string } | null>(null);
  const [jobLines, setJobLines] = useState<string[]>([]);
  const [jobDone, setJobDone] = useState(false);
  const closeStreamRef = useRef<(() => void) | null>(null);
  const logEndRef = useRef<HTMLDivElement | null>(null);

  const load = useCallback(async (silent = false) => {
    if (!silent) setLoadError(null);
    try {
      const [overview, errs] = await Promise.all([
        api.getRetailersOverview(),
        api.getErrors({ sinceMinutes: 1440, limit: 1 }).catch(() => null),
      ]);
      setData(overview);
      if (errs) setErrors(errs);
    } catch (e: unknown) {
      if (!silent) setLoadError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void load();
    // Light polling keeps in-flight crawls/uploads moving on screen without SSE per row.
    const t = setInterval(() => void load(true), 15000);
    return () => clearInterval(t);
  }, [load]);

  useEffect(() => () => closeStreamRef.current?.(), []);
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [jobLines]);

  const watchJob = useCallback(
    (jobId: string, title: string) => {
      closeStreamRef.current?.();
      setActiveJob({ id: jobId, title });
      setJobLines([]);
      setJobDone(false);
      closeStreamRef.current = subscribeStream(api.jobStreamUrl(jobId), {
        onLog: (line) => setJobLines((ls) => (ls.length > 500 ? [...ls.slice(-400), line] : [...ls, line])),
        onDone: (event) => {
          setJobDone(true);
          if (typeof event.error === "string") {
            setJobLines((ls) => [...ls, `✕ ${event.error}`]);
          } else {
            setJobLines((ls) => [...ls, "✓ done"]);
          }
          void load(true);
        },
        onError: () => setJobDone(true),
      });
    },
    [load],
  );

  /** Run an action that returns a jobId, stream its log, refresh on completion. */
  const runJob = useCallback(
    async (key: string, title: string, fn: () => Promise<{ jobId: string }>) => {
      setBusyKey(key);
      setActionError(null);
      try {
        const { jobId } = await fn();
        watchJob(jobId, title);
        void load(true);
      } catch (e: unknown) {
        setActionError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusyKey(null);
      }
    },
    [watchJob, load],
  );

  const retailers = useMemo(() => data?.retailers ?? [], [data]);
  const counts = useMemo(() => {
    const c: Record<StageFilter, number> = {
      all: retailers.length,
      running: 0,
      failed: 0,
      needs_crawl: 0,
      needs_upload: 0,
      done: 0,
    };
    for (const r of retailers) c[stageOf(r)]++;
    return c;
  }, [retailers]);

  const visible = useMemo(() => {
    const q = search.trim().toLowerCase();
    return retailers.filter((r) => {
      if (filter !== "all" && stageOf(r) !== filter) return false;
      if (!q) return true;
      const name = String(r.config?.retailerDisplayName ?? r.retailer).toLowerCase();
      return (
        name.includes(q) ||
        r.retailer.toLowerCase().includes(q) ||
        hostOf(r.config?.baseUrl).toLowerCase().includes(q)
      );
    });
  }, [retailers, filter, search]);

  const failedRetailers = useMemo(
    () => retailers.filter((r) => stageOf(r) === "failed").map((r) => r.retailer),
    [retailers],
  );

  async function setRec(retailer: string, rec: Recommendation) {
    setActionError(null);
    const prev = data;
    setData((d) =>
      d
        ? {
            ...d,
            retailers: d.retailers.map((r) =>
              r.retailer === retailer ? { ...r, recommendation: rec, storedRecommendation: rec } : r,
            ),
          }
        : d,
    );
    try {
      await api.setRecommendation(retailer, rec);
    } catch (e: unknown) {
      setData(prev);
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  const backlog = data?.identifiedWithoutConfig ?? [];

  if (!data && !loadError) {
    return <p className="text-sm text-neutral-400">Loading retailers…</p>;
  }

  return (
    <section className="space-y-4 pb-72">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold text-neutral-100">Retailers</h2>
          <p className="text-sm text-neutral-400">
            Explore → crawl → upload, per site. {counts.done}/{counts.all} fully up to date.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {errors && errors.counts.total > 0 && (
            <span
              className="rounded-full border border-red-500/30 px-2.5 py-1 text-xs text-red-300"
              title={Object.entries(errors.counts.byStage)
                .map(([s, n]) => `${s}: ${n}`)
                .join("  ·  ")}
            >
              {errors.counts.total} errors · 24h
            </span>
          )}
          {failedRetailers.length > 0 && (
            <button
              disabled={busyKey !== null}
              onClick={() =>
                void runJob("retry-failed", `Retry ${failedRetailers.length} failed explores`, () =>
                  api.exploreRetry(failedRetailers),
                )
              }
              className="rounded-md border border-amber-600/40 bg-amber-600/10 px-3 py-1.5 text-xs font-medium text-amber-300 hover:bg-amber-600/20 disabled:opacity-50"
            >
              Retry {failedRetailers.length} failed
            </button>
          )}
          <button
            disabled={busyKey !== null}
            onClick={() => {
              if (
                window.confirm(
                  "Run the full pipeline (discover → explore → crawl → upload) for everything? This spends Gemini/Stagehand credits.",
                )
              ) {
                void runJob("e2e", "Full pipeline run", () => api.runE2E());
              }
            }}
            className="rounded-md bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
          >
            ▶ Run full pipeline
          </button>
        </div>
      </header>

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex gap-1 rounded-lg bg-neutral-900 p-1">
          {FILTERS.map((f) => (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                filter === f.key
                  ? "bg-neutral-700 text-neutral-100"
                  : "text-neutral-400 hover:text-neutral-200"
              }`}
            >
              {f.label} <span className="ml-1 text-neutral-500">{counts[f.key]}</span>
            </button>
          ))}
        </div>
        <input
          type="search"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search retailer or domain…"
          className="ml-auto w-64 rounded-lg border border-neutral-700 bg-neutral-950 px-3 py-1.5 text-sm text-neutral-100 placeholder:text-neutral-600 focus:border-neutral-500 focus:outline-none"
        />
      </div>

      {loadError && (
        <div className="rounded-md border border-red-900 bg-red-950/50 px-4 py-3 text-sm text-red-300">
          {loadError}
        </div>
      )}
      {actionError && (
        <div className="flex items-start justify-between gap-3 rounded-md border border-red-900 bg-red-950/50 px-4 py-3 text-sm text-red-300">
          <span>{actionError}</span>
          <button onClick={() => setActionError(null)} className="text-red-400 hover:text-red-200">
            ✕
          </button>
        </div>
      )}

      <div className="overflow-x-auto rounded-xl border border-neutral-800">
        <table className="w-full min-w-[860px] text-sm">
          <thead>
            <tr className="border-b border-neutral-800 bg-neutral-900/60 text-left text-[11px] uppercase tracking-wide text-neutral-500">
              <th className="px-3 py-2 font-medium">Retailer</th>
              <th className="px-3 py-2 font-medium">Explore</th>
              <th className="px-3 py-2 font-medium">Quality</th>
              <th className="px-3 py-2 font-medium">Crawl</th>
              <th className="px-3 py-2 font-medium">Upload</th>
              <th className="px-3 py-2 text-right font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-10 text-center text-sm text-neutral-500">
                  No retailers match this filter.
                </td>
              </tr>
            )}
            {visible.map((r) => (
              <Row
                key={r.retailer}
                row={r}
                busy={busyKey !== null}
                onExplore={() =>
                  void runJob(`explore:${r.retailer}`, `Re-explore ${r.retailer}`, async () => {
                    const baseUrl = r.config?.baseUrl;
                    if (r.exploreStatus === "failed" || r.exploreStatus === "needs_retry" || r.exploreStatus === "queued_retry") {
                      return api.exploreRetry([r.retailer]);
                    }
                    if (!baseUrl) throw new Error("No base URL on config.");
                    return api.explore([baseUrl], false);
                  })
                }
                onCrawl={() => void runJob(`crawl:${r.retailer}`, `Crawl ${r.retailer}`, () => api.crawl(r.retailer))}
                onUpload={() => void runJob(`upload:${r.retailer}`, `Upload ${r.retailer}`, () => api.upload(r.retailer))}
                onWatch={() => r.latestJobId && watchJob(r.latestJobId, `${r.retailer} — latest job`)}
                onSetRec={(rec) => void setRec(r.retailer, rec)}
              />
            ))}
          </tbody>
        </table>
      </div>

      {/* Backlog: approved brands with no explore config yet. */}
      {backlog.length > 0 && (
        <div className="rounded-xl border border-neutral-800 bg-neutral-900/40">
          <button
            onClick={() => setShowBacklog((s) => !s)}
            className="flex w-full items-center justify-between px-4 py-3 text-left"
          >
            <span className="text-sm font-medium text-neutral-200">
              Backlog — approved brands without a crawl config yet{" "}
              <span className="text-neutral-500">({backlog.length})</span>
            </span>
            <span className="flex items-center gap-3">
              <span
                role="button"
                tabIndex={0}
                onClick={(e) => {
                  e.stopPropagation();
                  if (busyKey !== null) return;
                  if (window.confirm(`Explore all ${backlog.length} backlog sites? This spends Stagehand/Gemini credits.`)) {
                    void runJob("explore-backlog", `Explore ${backlog.length} backlog sites`, () =>
                      api.explore(backlog.map((b) => b.url)),
                    );
                  }
                }}
                className="rounded-md border border-neutral-700 px-2.5 py-1 text-xs text-neutral-300 hover:bg-neutral-800"
              >
                Explore all
              </span>
              <span className="text-neutral-500">{showBacklog ? "▾" : "▸"}</span>
            </span>
          </button>
          {showBacklog && (
            <ul className="divide-y divide-neutral-800/60 border-t border-neutral-800">
              {backlog.map((b) => (
                <li key={b.url} className="flex items-center justify-between gap-3 px-4 py-2">
                  <div className="min-w-0">
                    <span className="font-medium text-neutral-200">{b.name}</span>{" "}
                    <a
                      href={b.url}
                      target="_blank"
                      rel="noreferrer"
                      className="text-xs text-neutral-500 hover:text-neutral-300 hover:underline"
                    >
                      {hostOf(b.url)}
                    </a>
                  </div>
                  <button
                    disabled={busyKey !== null}
                    onClick={() =>
                      void runJob(`explore:${b.url}`, `Explore ${b.name}`, () => api.explore([b.url]))
                    }
                    className="rounded-md bg-neutral-800 px-3 py-1 text-xs font-medium text-neutral-200 hover:bg-neutral-700 disabled:opacity-50"
                  >
                    Explore
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {/* Live job log drawer. */}
      {activeJob && (
        <div className="fixed inset-x-0 bottom-0 z-20 border-t border-neutral-800 bg-neutral-950/95 backdrop-blur">
          <div className="mx-auto max-w-7xl px-6 py-3">
            <div className="mb-2 flex items-center justify-between">
              <span className="flex items-center gap-2 text-sm font-medium text-neutral-200">
                {!jobDone && <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-400" />}
                {activeJob.title}
                {jobDone && <span className="text-xs text-neutral-500">finished</span>}
              </span>
              <button
                onClick={() => {
                  closeStreamRef.current?.();
                  setActiveJob(null);
                }}
                className="rounded-md border border-neutral-800 px-2.5 py-1 text-xs text-neutral-400 hover:bg-neutral-800"
              >
                Close
              </button>
            </div>
            <div className="max-h-48 overflow-y-auto rounded-lg border border-neutral-800 bg-neutral-950 p-3 font-mono text-[11px] leading-relaxed text-neutral-300">
              {jobLines.length === 0 ? (
                <span className="text-neutral-600">Waiting for log output…</span>
              ) : (
                jobLines.map((l, i) => (
                  <div key={i} className="whitespace-pre-wrap break-words">
                    {l}
                  </div>
                ))
              )}
              <div ref={logEndRef} />
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

// --- Row ---

const EXPLORE_BADGE: Record<string, string> = {
  completed: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
  running: "bg-sky-500/15 text-sky-300 border-sky-500/30",
  failed: "bg-red-500/15 text-red-300 border-red-500/30",
  needs_retry: "bg-amber-500/15 text-amber-300 border-amber-500/30",
  queued_retry: "bg-amber-500/15 text-amber-300 border-amber-500/30",
  idle: "bg-neutral-700/30 text-neutral-400 border-neutral-600/40",
};

const REC_COLOR: Record<Recommendation, string> = {
  recommended: "text-emerald-300",
  usable: "text-sky-300",
  "not recommended": "text-neutral-500",
  unknown: "text-neutral-500",
};

function Row({
  row: r,
  busy,
  onExplore,
  onCrawl,
  onUpload,
  onWatch,
  onSetRec,
}: {
  row: RetailerRow;
  busy: boolean;
  onExplore: () => void;
  onCrawl: () => void;
  onUpload: () => void;
  onWatch: () => void;
  onSetRec: (rec: Recommendation) => void;
}) {
  const name = String(r.config?.retailerDisplayName ?? r.retailer);
  const host = hostOf(r.config?.baseUrl);
  const exploreRunning = r.exploreStatus === "running";
  const crawlRunning = r.crawlLive?.status === "running";
  const uploadRunning = r.uploadLive?.status === "running";
  const anyRunning = exploreRunning || crawlRunning || uploadRunning;
  const exploreFailedish =
    r.exploreStatus === "failed" || r.exploreStatus === "needs_retry" || r.exploreStatus === "queued_retry";
  const exploreTitle =
    [r.exploreFailureCode, r.exploreFailureReason ?? r.exploreError].filter(Boolean).join(" — ") || undefined;

  return (
    <tr className="border-b border-neutral-800/60 last:border-b-0 hover:bg-neutral-900/40">
      <td className="max-w-[220px] px-3 py-2.5">
        <div className="truncate font-medium text-neutral-100">{name}</div>
        {host && (
          <a
            href={r.config?.baseUrl}
            target="_blank"
            rel="noreferrer"
            className="truncate text-xs text-neutral-500 hover:text-neutral-300 hover:underline"
          >
            {host}
          </a>
        )}
      </td>

      <td className="px-3 py-2.5">
        <span
          title={exploreTitle}
          className={`inline-block rounded-full border px-2 py-0.5 text-[11px] ${EXPLORE_BADGE[r.exploreStatus] ?? EXPLORE_BADGE.idle}`}
        >
          {r.exploreStatus.replace("_", " ")}
          {exploreFailedish && r.exploreAttempt != null && r.exploreMaxAttempts != null && (
            <span className="ml-1 opacity-70">
              {r.exploreAttempt}/{r.exploreMaxAttempts}
            </span>
          )}
        </span>
      </td>

      <td className="px-3 py-2.5">
        <select
          value={REC_OPTIONS.includes(r.recommendation) ? r.recommendation : "not recommended"}
          disabled={r.exploreStatus !== "completed"}
          onChange={(e) => onSetRec(e.target.value as Recommendation)}
          className={`rounded-md border border-transparent bg-transparent py-0.5 pr-1 text-xs hover:border-neutral-700 focus:border-neutral-600 focus:outline-none disabled:opacity-40 ${REC_COLOR[r.recommendation] ?? "text-neutral-400"}`}
        >
          {REC_OPTIONS.map((o) => (
            <option key={o} value={o} className="bg-neutral-900 text-neutral-200">
              {o}
            </option>
          ))}
        </select>
      </td>

      <td className="px-3 py-2.5 text-xs">
        {crawlRunning ? (
          <span className="text-sky-300">
            ⟳ {r.crawlLive?.totalUrls ?? 0} URLs…
            <span className="ml-1 text-neutral-500">{timeAgo(r.crawlLive?.lastCheckpointAt)}</span>
          </span>
        ) : r.crawlLive?.status === "failed" ? (
          <span className="text-red-300" title={r.crawlLive?.error ?? undefined}>
            ✕ failed{r.crawlLive?.totalUrls ? ` · ${r.crawlLive.totalUrls} URLs saved` : ""}
          </span>
        ) : r.crawlLive?.status === "interrupted" && !r.crawl ? (
          <span className="text-amber-300" title="Interrupted by a restart — re-run Crawl to resume from the checkpoint.">
            ⏸ interrupted{r.crawlLive?.totalUrls ? ` · ${r.crawlLive.totalUrls} URLs saved` : ""}
          </span>
        ) : r.crawl ? (
          <span className="text-neutral-300">
            {r.crawl.totalUrls.toLocaleString()} URLs
            <span className="ml-1.5 text-neutral-500">{timeAgo(r.crawl.crawledAt)}</span>
          </span>
        ) : (
          <span className="text-neutral-600">—</span>
        )}
      </td>

      <td className="px-3 py-2.5 text-xs">
        {uploadRunning ? (
          <span className="text-sky-300">
            ⟳ {(r.uploadLive?.uploaded ?? 0) + (r.uploadLive?.skipped ?? 0)}/{r.uploadLive?.total ?? 0}
            {r.uploadLive?.failed ? <span className="ml-1 text-red-400">({r.uploadLive.failed} failed)</span> : null}
          </span>
        ) : r.uploadLive?.status === "failed" ? (
          <span className="text-red-300" title={r.uploadLive?.error ?? undefined}>
            ✕ failed
          </span>
        ) : r.uploadLive?.status === "interrupted" ? (
          <span
            className="text-amber-300"
            title="Interrupted by a restart — re-run Upload to resume (already-uploaded URLs are skipped)."
          >
            ⏸ {(r.uploadLive?.uploaded ?? 0) + (r.uploadLive?.skipped ?? 0)}/{r.uploadLive?.total ?? 0} · resume
          </span>
        ) : r.upload ? (
          <span className={r.uploadMatchesCurrentCrawl ? "text-neutral-300" : "text-amber-300"}>
            {Number(r.upload.uploaded ?? 0).toLocaleString()}
            {r.upload.failed ? <span className="ml-1 text-red-400">({Number(r.upload.failed)} failed)</span> : null}
            <span className="ml-1.5 text-neutral-500">{timeAgo(String(r.upload.uploadedAt ?? ""))}</span>
            {!r.uploadMatchesCurrentCrawl && (
              <span className="ml-1.5" title="Uploaded from an older crawl — re-upload to sync.">
                stale
              </span>
            )}
          </span>
        ) : (
          <span className="text-neutral-600">—</span>
        )}
      </td>

      <td className="px-3 py-2.5">
        <div className="flex items-center justify-end gap-1.5">
          <RowButton
            label={exploreFailedish ? "Retry" : "Explore"}
            title={exploreFailedish ? "Retry the failed explore" : "Re-run site exploration (spends credits)"}
            disabled={busy || anyRunning}
            accent={exploreFailedish ? "amber" : undefined}
            onClick={onExplore}
          />
          <RowButton
            label="Crawl"
            title="Collect all product URLs using the stored config"
            disabled={busy || anyRunning || r.exploreStatus !== "completed"}
            onClick={onCrawl}
          />
          <RowButton
            label="Upload"
            title="Extract products + push to R2/Supabase (resumes automatically)"
            disabled={busy || anyRunning || !r.crawl}
            onClick={onUpload}
          />
          {r.latestJobId && (
            <RowButton label="Log" title="Watch the latest job log" disabled={false} onClick={onWatch} />
          )}
        </div>
      </td>
    </tr>
  );
}

function RowButton({
  label,
  title,
  disabled,
  accent,
  onClick,
}: {
  label: string;
  title: string;
  disabled: boolean;
  accent?: "amber";
  onClick: () => void;
}) {
  const cls =
    accent === "amber"
      ? "border-amber-600/40 bg-amber-600/10 text-amber-300 hover:bg-amber-600/20"
      : "border-neutral-700 bg-neutral-800/60 text-neutral-300 hover:bg-neutral-700";
  return (
    <button
      title={title}
      disabled={disabled}
      onClick={onClick}
      className={`rounded-md border px-2.5 py-1 text-xs font-medium transition disabled:cursor-not-allowed disabled:opacity-40 ${cls}`}
    >
      {label}
    </button>
  );
}
