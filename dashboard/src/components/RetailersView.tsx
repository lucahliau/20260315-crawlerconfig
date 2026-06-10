import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  subscribeStream,
  type ErrorsResponse,
  type Recommendation,
  type RetailerRow,
  type RetailersOverviewResponse,
} from "../api.ts";
import {
  Button,
  Card,
  ChevronIcon,
  ErrorBanner,
  PlayIcon,
  SearchIcon,
  Segmented,
  Spinner,
  StatusDot,
  XIcon,
  cx,
  type Tone,
} from "./ui.tsx";

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
        onLog: (line) =>
          setJobLines((ls) => (ls.length > 500 ? [...ls.slice(-400), line] : [...ls, line])),
        onDone: (event) => {
          setJobDone(true);
          if (typeof event.error === "string") {
            setJobLines((ls) => [...ls, `Failed: ${event.error}`]);
          } else {
            setJobLines((ls) => [...ls, "Done."]);
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
    return <p className="text-sm text-gray-500">Loading retailers…</p>;
  }

  return (
    <section className="space-y-6 pb-72">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Retailers</h1>
          <p className="mt-1 text-sm text-gray-500">
            Explore → crawl → upload, per site.{" "}
            <span className="tnum">
              {counts.done}/{counts.all}
            </span>{" "}
            fully up to date.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {errors && errors.counts.total > 0 && (
            <span
              className="mr-1"
              title={Object.entries(errors.counts.byStage)
                .map(([s, n]) => `${s}: ${n}`)
                .join("  ·  ")}
            >
              <StatusDot
                tone="failed"
                label={`${errors.counts.total} errors in 24h`}
              />
            </span>
          )}
          {failedRetailers.length > 0 && (
            <Button
              variant="warn"
              size="sm"
              disabled={busyKey !== null}
              onClick={() =>
                void runJob("retry-failed", `Retry ${failedRetailers.length} failed explores`, () =>
                  api.exploreRetry(failedRetailers),
                )
              }
            >
              Retry {failedRetailers.length} failed
            </Button>
          )}
          <Button
            variant="primary"
            size="sm"
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
          >
            <PlayIcon />
            Run full pipeline
          </Button>
        </div>
      </header>

      <div className="flex flex-wrap items-center gap-3">
        <Segmented
          items={FILTERS.map((f) => ({ ...f, count: counts[f.key] }))}
          active={filter}
          onChange={(k) => setFilter(k as StageFilter)}
        />
        <div className="relative ml-auto">
          <SearchIcon className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-gray-400" />
          <input
            type="search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search retailer or domain"
            className="h-8 w-64 rounded-md border border-gray-300 bg-white pl-8 pr-3 text-[13px] text-gray-900 placeholder:text-gray-400 focus:border-gray-400 focus:outline-none"
          />
        </div>
      </div>

      {loadError && <ErrorBanner>{loadError}</ErrorBanner>}
      {actionError && <ErrorBanner onDismiss={() => setActionError(null)}>{actionError}</ErrorBanner>}

      <Card className="overflow-x-auto">
        <table className="w-full min-w-[880px] text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50 text-left text-[11px] font-medium uppercase tracking-wide text-gray-500">
              <th className="px-4 py-2.5">Retailer</th>
              <th className="px-4 py-2.5">Explore</th>
              <th className="px-4 py-2.5">Quality</th>
              <th className="px-4 py-2.5">Crawl</th>
              <th className="px-4 py-2.5">Upload</th>
              <th className="px-4 py-2.5 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {visible.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-12 text-center text-sm text-gray-400">
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
                    if (
                      r.exploreStatus === "failed" ||
                      r.exploreStatus === "needs_retry" ||
                      r.exploreStatus === "queued_retry"
                    ) {
                      return api.exploreRetry([r.retailer]);
                    }
                    if (!baseUrl) throw new Error("No base URL on config.");
                    return api.explore([baseUrl], false);
                  })
                }
                onCrawl={() =>
                  void runJob(`crawl:${r.retailer}`, `Crawl ${r.retailer}`, () => api.crawl(r.retailer))
                }
                onUpload={() =>
                  void runJob(`upload:${r.retailer}`, `Upload ${r.retailer}`, () =>
                    api.upload(r.retailer),
                  )
                }
                onWatch={() => r.latestJobId && watchJob(r.latestJobId, `${r.retailer} — latest job`)}
                onSetRec={(rec) => void setRec(r.retailer, rec)}
              />
            ))}
          </tbody>
        </table>
      </Card>

      {/* Backlog: approved brands with no explore config yet. */}
      {backlog.length > 0 && (
        <Card>
          <button
            onClick={() => setShowBacklog((s) => !s)}
            className="flex w-full items-center justify-between px-4 py-3 text-left"
          >
            <span className="text-sm font-medium text-gray-900">
              Backlog — approved brands without a crawl config yet{" "}
              <span className="tnum font-normal text-gray-400">({backlog.length})</span>
            </span>
            <span className="flex items-center gap-3">
              <span
                role="button"
                tabIndex={0}
                onClick={(e) => {
                  e.stopPropagation();
                  if (busyKey !== null) return;
                  if (
                    window.confirm(
                      `Explore all ${backlog.length} backlog sites? This spends Stagehand/Gemini credits on non-Shopify sites.`,
                    )
                  ) {
                    void runJob("explore-backlog", `Explore ${backlog.length} backlog sites`, () =>
                      api.explore(backlog.map((b) => b.url)),
                    );
                  }
                }}
                className="inline-flex h-7 items-center rounded-md border border-gray-300 bg-white px-2.5 text-xs font-medium text-gray-700 hover:bg-gray-50"
              >
                Explore all
              </span>
              <ChevronIcon open={showBacklog} className="text-gray-400" />
            </span>
          </button>
          {showBacklog && (
            <ul className="divide-y divide-gray-100 border-t border-gray-200">
              {backlog.map((b) => (
                <li
                  key={b.url}
                  className="flex items-center justify-between gap-3 px-4 py-2 transition-colors hover:bg-gray-50"
                >
                  <div className="flex min-w-0 items-baseline gap-2">
                    <span className="text-sm font-medium text-gray-900">{b.name}</span>
                    <a
                      href={b.url}
                      target="_blank"
                      rel="noreferrer"
                      className="truncate text-xs text-gray-400 hover:text-gray-700 hover:underline"
                    >
                      {hostOf(b.url)}
                    </a>
                  </div>
                  <Button
                    size="sm"
                    disabled={busyKey !== null}
                    onClick={() =>
                      void runJob(`explore:${b.url}`, `Explore ${b.name}`, () => api.explore([b.url]))
                    }
                  >
                    Explore
                  </Button>
                </li>
              ))}
            </ul>
          )}
        </Card>
      )}

      {/* Live job log drawer. */}
      {activeJob && (
        <div className="fixed inset-x-0 bottom-0 z-40 border-t border-gray-200 bg-white shadow-[0_-8px_24px_rgba(0,0,0,0.08)]">
          <div className="mx-auto max-w-7xl px-6 py-3">
            <div className="mb-2 flex items-center justify-between">
              <span className="flex items-center gap-2 text-sm font-medium text-gray-900">
                {jobDone ? (
                  <StatusDot tone="ok" label="" />
                ) : (
                  <Spinner className="text-blue-500" />
                )}
                {activeJob.title}
                {jobDone && <span className="text-xs font-normal text-gray-400">finished</span>}
              </span>
              <button
                onClick={() => {
                  closeStreamRef.current?.();
                  setActiveJob(null);
                }}
                className="inline-flex size-7 items-center justify-center rounded-md text-gray-400 hover:bg-gray-100 hover:text-gray-700"
                aria-label="Close log"
              >
                <XIcon />
              </button>
            </div>
            <div className="max-h-48 overflow-y-auto rounded-md bg-gray-950 p-3 font-mono text-[11px] leading-relaxed text-gray-300">
              {jobLines.length === 0 ? (
                <span className="text-gray-500">Waiting for log output…</span>
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

const EXPLORE_TONE: Record<string, { tone: Tone; label: string }> = {
  completed: { tone: "ok", label: "Completed" },
  running: { tone: "running", label: "Running" },
  failed: { tone: "failed", label: "Failed" },
  needs_retry: { tone: "warn", label: "Needs retry" },
  queued_retry: { tone: "warn", label: "Retry queued" },
  idle: { tone: "idle", label: "Not explored" },
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
    r.exploreStatus === "failed" ||
    r.exploreStatus === "needs_retry" ||
    r.exploreStatus === "queued_retry";
  const exploreTitle =
    [r.exploreFailureCode, r.exploreFailureReason ?? r.exploreError].filter(Boolean).join(" — ") ||
    undefined;
  const exploreBadge = EXPLORE_TONE[r.exploreStatus] ?? EXPLORE_TONE.idle;

  return (
    <tr className="transition-colors hover:bg-gray-50">
      <td className="max-w-[220px] px-4 py-2.5">
        <div className="truncate text-sm font-medium text-gray-900">{name}</div>
        {host && (
          <a
            href={r.config?.baseUrl}
            target="_blank"
            rel="noreferrer"
            className="truncate text-xs text-gray-400 hover:text-gray-700 hover:underline"
          >
            {host}
          </a>
        )}
      </td>

      <td className="px-4 py-2.5">
        <StatusDot
          tone={exploreBadge.tone}
          title={exploreTitle}
          muted={r.exploreStatus === "idle"}
          label={
            <>
              {exploreBadge.label}
              {exploreFailedish && r.exploreAttempt != null && r.exploreMaxAttempts != null && (
                <span className="tnum ml-1 text-gray-400">
                  {r.exploreAttempt}/{r.exploreMaxAttempts}
                </span>
              )}
            </>
          }
        />
      </td>

      <td className="px-4 py-2.5">
        <select
          value={REC_OPTIONS.includes(r.recommendation) ? r.recommendation : "not recommended"}
          disabled={r.exploreStatus !== "completed"}
          onChange={(e) => onSetRec(e.target.value as Recommendation)}
          className={cx(
            "h-7 rounded-md border border-transparent bg-transparent pr-1 text-xs",
            "hover:border-gray-300 focus:border-gray-400 focus:outline-none disabled:opacity-40",
            r.recommendation === "recommended"
              ? "text-emerald-700"
              : r.recommendation === "usable"
                ? "text-blue-700"
                : "text-gray-500",
          )}
        >
          {REC_OPTIONS.map((o) => (
            <option key={o} value={o} className="text-gray-900">
              {o}
            </option>
          ))}
        </select>
      </td>

      <td className="whitespace-nowrap px-4 py-2.5 text-xs">
        {crawlRunning ? (
          <span className="inline-flex items-center gap-1.5 text-blue-700">
            <Spinner className="text-blue-500" />
            <span className="tnum">{r.crawlLive?.totalUrls ?? 0} URLs</span>
            <span className="text-gray-400">{timeAgo(r.crawlLive?.lastCheckpointAt)}</span>
          </span>
        ) : r.crawlLive?.status === "failed" ? (
          <StatusDot
            tone="failed"
            title={r.crawlLive?.error ?? undefined}
            label={`Failed${r.crawlLive?.totalUrls ? ` · ${r.crawlLive.totalUrls} URLs saved` : ""}`}
          />
        ) : r.crawlLive?.status === "interrupted" && !r.crawl ? (
          <StatusDot
            tone="warn"
            title="Interrupted by a restart — re-run Crawl to resume from the checkpoint."
            label={`Interrupted${r.crawlLive?.totalUrls ? ` · ${r.crawlLive.totalUrls} saved` : ""}`}
          />
        ) : r.crawl ? (
          <span className="text-gray-700">
            <span className="tnum">{r.crawl.totalUrls.toLocaleString()}</span> URLs
            <span className="ml-1.5 text-gray-400">{timeAgo(r.crawl.crawledAt)}</span>
          </span>
        ) : (
          <span className="text-gray-300">—</span>
        )}
      </td>

      <td className="whitespace-nowrap px-4 py-2.5 text-xs">
        {uploadRunning ? (
          <span className="inline-flex items-center gap-1.5 text-blue-700">
            <Spinner className="text-blue-500" />
            <span className="tnum">
              {(r.uploadLive?.uploaded ?? 0) + (r.uploadLive?.skipped ?? 0)}/{r.uploadLive?.total ?? 0}
            </span>
            {r.uploadLive?.failed ? (
              <span className="tnum text-red-600">({r.uploadLive.failed} failed)</span>
            ) : null}
          </span>
        ) : r.uploadLive?.status === "failed" ? (
          <StatusDot tone="failed" title={r.uploadLive?.error ?? undefined} label="Failed" />
        ) : r.uploadLive?.status === "interrupted" ? (
          <StatusDot
            tone="warn"
            title="Interrupted by a restart — re-run Upload to resume (already-uploaded URLs are skipped)."
            label={
              <span className="tnum">
                {(r.uploadLive?.uploaded ?? 0) + (r.uploadLive?.skipped ?? 0)}/
                {r.uploadLive?.total ?? 0} · resume
              </span>
            }
          />
        ) : r.upload ? (
          <span className={r.uploadMatchesCurrentCrawl ? "text-gray-700" : "text-amber-700"}>
            <span className="tnum">{Number(r.upload.uploaded ?? 0).toLocaleString()}</span>
            {r.upload.failed ? (
              <span className="tnum ml-1 text-red-600">({Number(r.upload.failed)} failed)</span>
            ) : null}
            <span className="ml-1.5 text-gray-400">{timeAgo(String(r.upload.uploadedAt ?? ""))}</span>
            {!r.uploadMatchesCurrentCrawl && (
              <span className="ml-1.5" title="Uploaded from an older crawl — re-upload to sync.">
                stale
              </span>
            )}
          </span>
        ) : (
          <span className="text-gray-300">—</span>
        )}
      </td>

      <td className="px-4 py-2.5">
        <div className="flex items-center justify-end gap-1">
          <Button
            size="sm"
            variant={exploreFailedish ? "warn" : "ghost"}
            title={exploreFailedish ? "Retry the failed explore" : "Re-run site exploration (spends credits on non-Shopify sites)"}
            disabled={busy || anyRunning}
            onClick={onExplore}
          >
            {exploreFailedish ? "Retry" : "Explore"}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            title="Collect all product URLs using the stored config"
            disabled={busy || anyRunning || r.exploreStatus !== "completed"}
            onClick={onCrawl}
          >
            Crawl
          </Button>
          <Button
            size="sm"
            variant="ghost"
            title="Extract products and push to R2/Supabase (resumes automatically)"
            disabled={busy || anyRunning || !r.crawl}
            onClick={onUpload}
          >
            Upload
          </Button>
          {r.latestJobId && (
            <Button size="sm" variant="ghost" title="Watch the latest job log" onClick={onWatch}>
              Log
            </Button>
          )}
        </div>
      </td>
    </tr>
  );
}
