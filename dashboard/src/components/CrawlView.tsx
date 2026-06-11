import { useMemo, useState } from "react";
import { api, type Recommendation, type RetailerRow } from "../api.ts";
import {
  Button,
  Card,
  ErrorBanner,
  PlayIcon,
  SearchIcon,
  Segmented,
  Spinner,
  StatusDot,
  type Tone,
} from "./ui.tsx";
import {
  JobDrawer,
  hostOf,
  retailerName,
  timeAgo,
  useJobRunner,
  useRetailerSearch,
  useRetailersOverview,
} from "./pipelineShared.tsx";

/**
 * Stage 3 — Crawl & upload. Sites with an identified config get their product
 * URLs collected (crawl) and their products extracted + pushed to R2/Supabase
 * (upload). Rows show live progress; both steps resume automatically after
 * interruptions.
 */

type Filter = "all" | "needs_crawl" | "needs_upload" | "running" | "failed" | "done";

function filterOf(r: RetailerRow): Exclude<Filter, "all"> {
  if (r.crawlLive?.status === "running" || r.uploadLive?.status === "running") return "running";
  if (r.crawlLive?.status === "failed" || r.uploadLive?.status === "failed") return "failed";
  if (!r.crawl) return "needs_crawl";
  if (!r.upload || !r.uploadMatchesCurrentCrawl) return "needs_upload";
  return "done";
}

// Sort priority for "likely to succeed". Lower = better. Crawled rows go last
// so the top of the table is always "ready to crawl, high-confidence first".
const RECOMMENDATION_RANK: Record<Recommendation, number> = {
  recommended: 0,
  usable: 1,
  unknown: 2,
  "not recommended": 3,
};

function confidenceTone(rec: Recommendation): Tone {
  if (rec === "recommended") return "ok";
  if (rec === "usable") return "info";
  if (rec === "not recommended") return "warn";
  return "idle";
}

function confidenceLabel(rec: Recommendation): string {
  if (rec === "recommended") return "High";
  if (rec === "usable") return "Medium";
  if (rec === "not recommended") return "Low";
  return "Unknown";
}

export function CrawlView() {
  const { data, errors, loadError, load } = useRetailersOverview();
  const { busyKey, actionError, setActionError, runJob, watchJob, drawer } = useJobRunner(load);
  const [filter, setFilter] = useState<Filter>("all");
  const [search, setSearch] = useState("");

  // Only sites whose config identification completed AND whose stored config
  // is schema-valid belong in this stage. Invalid configs (e.g. legacy stubs
  // with `discovery.method: null`) would 400 on crawl — they live in Configure.
  // Sort: uncrawled high-confidence first, then crawled rows (alphabetical
  // within each group, since the API returns rows sorted).
  const retailers = useMemo(
    () =>
      (data?.retailers ?? [])
        .filter((r) => r.exploreStatus === "completed" && r.configValid !== false)
        .sort((a, b) => {
          const aCrawled = a.crawl ? 1 : 0;
          const bCrawled = b.crawl ? 1 : 0;
          if (aCrawled !== bCrawled) return aCrawled - bCrawled;
          if (!aCrawled) {
            const ra = RECOMMENDATION_RANK[a.recommendation] ?? 2;
            const rb = RECOMMENDATION_RANK[b.recommendation] ?? 2;
            if (ra !== rb) return ra - rb;
          }
          return 0;
        }),
    [data],
  );
  const crawledCount = useMemo(() => retailers.filter((r) => !!r.crawl).length, [retailers]);
  const counts = useMemo(() => {
    const c: Record<Filter, number> = {
      all: retailers.length,
      needs_crawl: 0,
      needs_upload: 0,
      running: 0,
      failed: 0,
      done: 0,
    };
    for (const r of retailers) c[filterOf(r)]++;
    return c;
  }, [retailers]);

  const searched = useRetailerSearch(retailers, search);
  const visible = useMemo(
    () => searched.filter((r) => filter === "all" || filterOf(r) === filter),
    [searched, filter],
  );

  if (!data && !loadError) return <p className="text-sm text-gray-500">Loading…</p>;

  return (
    <section className="space-y-6 pb-72">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Crawl &amp; upload</h1>
          <p className="mt-1 text-sm text-gray-500">
            Collect product URLs, then extract and push products to the catalog.{" "}
            <span className="tnum">
              {crawledCount}/{counts.all}
            </span>{" "}
            crawled ·{" "}
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
              <StatusDot tone="failed" label={`${errors.counts.total} errors in 24h`} />
            </span>
          )}
          <Button
            variant="primary"
            size="sm"
            disabled={busyKey !== null}
            onClick={() => {
              if (
                window.confirm(
                  "Run the full pipeline (discover → configure → crawl → upload) for everything? This spends AI credits.",
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
          items={[
            { key: "all", label: "All", count: counts.all },
            { key: "running", label: "Running", count: counts.running },
            { key: "failed", label: "Failed", count: counts.failed },
            { key: "needs_crawl", label: "Needs crawl", count: counts.needs_crawl },
            { key: "needs_upload", label: "Needs upload", count: counts.needs_upload },
            { key: "done", label: "Up to date", count: counts.done },
          ]}
          active={filter}
          onChange={(k) => setFilter(k as Filter)}
        />
        <div className="relative ml-auto">
          <SearchIcon className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-gray-400" />
          <input
            type="search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search site or domain"
            className="h-8 w-64 rounded-md border border-gray-300 bg-white pl-8 pr-3 text-[13px] text-gray-900 placeholder:text-gray-400 focus:border-gray-400 focus:outline-none"
          />
        </div>
      </div>

      {loadError && <ErrorBanner>{loadError}</ErrorBanner>}
      {actionError && <ErrorBanner onDismiss={() => setActionError(null)}>{actionError}</ErrorBanner>}

      <Card className="overflow-x-auto">
        <table className="w-full min-w-[820px] text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50 text-left text-[11px] font-medium uppercase tracking-wide text-gray-500">
              <th className="px-4 py-2.5">Site</th>
              <th className="px-4 py-2.5">Confidence</th>
              <th className="px-4 py-2.5">Crawl</th>
              <th className="px-4 py-2.5">Upload</th>
              <th className="px-4 py-2.5 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {visible.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-12 text-center text-sm text-gray-400">
                  No sites match this filter.
                </td>
              </tr>
            )}
            {visible.map((r) => (
              <Row
                key={r.retailer}
                row={r}
                busy={busyKey !== null}
                onCrawl={() =>
                  void runJob(`crawl:${r.retailer}`, `Crawl ${r.retailer}`, () => api.crawl(r.retailer))
                }
                onUpload={() =>
                  void runJob(`upload:${r.retailer}`, `Upload ${r.retailer}`, () => api.upload(r.retailer))
                }
                onWatch={() => r.latestJobId && watchJob(r.latestJobId, `${r.retailer} — latest job`)}
              />
            ))}
          </tbody>
        </table>
      </Card>

      <JobDrawer drawer={drawer} />
    </section>
  );
}

function Row({
  row: r,
  busy,
  onCrawl,
  onUpload,
  onWatch,
}: {
  row: RetailerRow;
  busy: boolean;
  onCrawl: () => void;
  onUpload: () => void;
  onWatch: () => void;
}) {
  const crawlRunning = r.crawlLive?.status === "running";
  const uploadRunning = r.uploadLive?.status === "running";
  const anyRunning = crawlRunning || uploadRunning;

  return (
    <tr className="transition-colors hover:bg-gray-50">
      <td className="max-w-[240px] px-4 py-2.5">
        <div className="truncate text-sm font-medium text-gray-900">{retailerName(r)}</div>
        <a
          href={r.config?.baseUrl}
          target="_blank"
          rel="noreferrer"
          className="truncate text-xs text-gray-400 hover:text-gray-700 hover:underline"
        >
          {hostOf(r.config?.baseUrl)}
        </a>
      </td>

      <td className="whitespace-nowrap px-4 py-2.5 text-xs">
        <StatusDot
          tone={confidenceTone(r.recommendation)}
          title={
            r.configMethod
              ? `Discovery: ${r.configMethod} · ${r.recommendation}`
              : r.recommendation
          }
          label={
            <>
              <span>{confidenceLabel(r.recommendation)}</span>
              {r.configMethod && (
                <span className="ml-1.5 text-gray-400">{r.configMethod}</span>
              )}
            </>
          }
        />
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
          <StatusDot
            tone="ok"
            label={
              <>
                Crawled · <span className="tnum">{r.crawl.totalUrls.toLocaleString()}</span> URLs
                <span className="ml-1.5 text-gray-400">{timeAgo(r.crawl.crawledAt)}</span>
              </>
            }
          />
        ) : (
          <StatusDot tone="idle" muted label="Not crawled yet" />
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
                {(r.uploadLive?.uploaded ?? 0) + (r.uploadLive?.skipped ?? 0)}/{r.uploadLive?.total ?? 0}{" "}
                · resume
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
            variant="ghost"
            title="Collect all product URLs using the stored config"
            disabled={busy || anyRunning}
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
