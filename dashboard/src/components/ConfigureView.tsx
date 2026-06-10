import { useMemo, useState } from "react";
import { api, type Recommendation, type RetailerRow } from "../api.ts";
import {
  Button,
  Card,
  ChevronIcon,
  ErrorBanner,
  SearchIcon,
  Segmented,
  StatusDot,
  cx,
  type Tone,
} from "./ui.tsx";
import {
  JobDrawer,
  hostOf,
  isExploreFailedish,
  retailerName,
  useJobRunner,
  useRetailerSearch,
  useRetailersOverview,
} from "./pipelineShared.tsx";

/**
 * Stage 2 — Configure. Every approved brand needs a crawl config: the cheap
 * platform detectors handle Shopify/WooCommerce/sitemap sites automatically,
 * the AI explore covers the rest. This view is the queue for that work: the
 * backlog (no config yet), what's identifying right now, what failed, and the
 * quality verdict for everything identified.
 */

type Filter = "all" | "identified" | "running" | "failed";

const REC_OPTIONS: Recommendation[] = ["recommended", "usable", "not recommended"];

const EXPLORE_TONE: Record<string, { tone: Tone; label: string }> = {
  completed: { tone: "ok", label: "Identified" },
  running: { tone: "running", label: "Identifying" },
  failed: { tone: "failed", label: "Failed" },
  needs_retry: { tone: "warn", label: "Needs retry" },
  queued_retry: { tone: "warn", label: "Retry queued" },
  idle: { tone: "idle", label: "Not identified" },
};

function filterOf(r: RetailerRow): Exclude<Filter, "all"> {
  if (r.exploreStatus === "running") return "running";
  if (isExploreFailedish(r)) return "failed";
  return "identified";
}

export function ConfigureView() {
  const { data, loadError, load } = useRetailersOverview();
  const { busyKey, actionError, setActionError, runJob, watchJob, drawer } = useJobRunner(load);
  const [filter, setFilter] = useState<Filter>("all");
  const [search, setSearch] = useState("");
  const [showBacklog, setShowBacklog] = useState(true);

  const retailers = useMemo(() => data?.retailers ?? [], [data]);
  const counts = useMemo(() => {
    const c: Record<Filter, number> = { all: retailers.length, identified: 0, running: 0, failed: 0 };
    for (const r of retailers) c[filterOf(r)]++;
    return c;
  }, [retailers]);

  const searched = useRetailerSearch(retailers, search);
  const visible = useMemo(
    () => searched.filter((r) => filter === "all" || filterOf(r) === filter),
    [searched, filter],
  );
  const failedRetailers = useMemo(
    () => retailers.filter((r) => isExploreFailedish(r)).map((r) => r.retailer),
    [retailers],
  );
  const backlog = data?.identifiedWithoutConfig ?? [];

  async function setRec(retailer: string, rec: Recommendation) {
    setActionError(null);
    try {
      await api.setRecommendation(retailer, rec);
      load(true);
    } catch (e: unknown) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  if (!data && !loadError) return <p className="text-sm text-gray-500">Loading…</p>;

  return (
    <section className="space-y-6 pb-72">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Configure</h1>
          <p className="mt-1 max-w-2xl text-sm text-gray-500">
            Each approved brand needs a crawl config. Shopify, WooCommerce and sitemap sites are
            identified automatically for free; tougher sites use one AI call, then a browser session
            as a last resort.
          </p>
        </div>
        {failedRetailers.length > 0 && (
          <Button
            variant="warn"
            size="sm"
            disabled={busyKey !== null}
            onClick={() =>
              void runJob("retry-failed", `Retry ${failedRetailers.length} failed identifications`, () =>
                api.exploreRetry(failedRetailers),
              )
            }
          >
            Retry {failedRetailers.length} failed
          </Button>
        )}
      </header>

      {/* Backlog first — it's the work queue for this stage. */}
      {backlog.length > 0 && (
        <Card>
          <button
            onClick={() => setShowBacklog((s) => !s)}
            className="flex w-full items-center justify-between px-4 py-3 text-left"
          >
            <span className="text-sm font-medium text-gray-900">
              Waiting for a config{" "}
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
                      `Identify configs for all ${backlog.length} sites? Non-Shopify sites may spend AI credits.`,
                    )
                  ) {
                    void runJob("explore-backlog", `Identify ${backlog.length} site configs`, () =>
                      api.explore(backlog.map((b) => b.url)),
                    );
                  }
                }}
                className="inline-flex h-7 items-center rounded-md border border-gray-300 bg-white px-2.5 text-xs font-medium text-gray-700 hover:bg-gray-50"
              >
                Identify all
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
                      void runJob(`explore:${b.url}`, `Identify config for ${b.name}`, () =>
                        api.explore([b.url]),
                      )
                    }
                  >
                    Identify
                  </Button>
                </li>
              ))}
            </ul>
          )}
        </Card>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <Segmented
          items={[
            { key: "all", label: "All", count: counts.all },
            { key: "identified", label: "Identified", count: counts.identified },
            { key: "running", label: "Running", count: counts.running },
            { key: "failed", label: "Failed", count: counts.failed },
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
        <table className="w-full min-w-[760px] text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50 text-left text-[11px] font-medium uppercase tracking-wide text-gray-500">
              <th className="px-4 py-2.5">Site</th>
              <th className="px-4 py-2.5">Config</th>
              <th className="px-4 py-2.5">Method</th>
              <th className="px-4 py-2.5">Quality</th>
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
            {visible.map((r) => {
              const badge = EXPLORE_TONE[r.exploreStatus] ?? EXPLORE_TONE.idle;
              const failedish = isExploreFailedish(r);
              const method = (r.config?.discovery as { method?: string } | undefined)?.method ?? "—";
              const exploreTitle =
                [r.exploreFailureCode, r.exploreFailureReason ?? r.exploreError]
                  .filter(Boolean)
                  .join(" — ") || undefined;
              return (
                <tr key={r.retailer} className="transition-colors hover:bg-gray-50">
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
                  <td className="whitespace-nowrap px-4 py-2.5">
                    <StatusDot
                      tone={badge.tone}
                      title={exploreTitle}
                      muted={r.exploreStatus === "idle"}
                      label={
                        <>
                          {badge.label}
                          {failedish && r.exploreAttempt != null && r.exploreMaxAttempts != null && (
                            <span className="tnum ml-1 text-gray-400">
                              {r.exploreAttempt}/{r.exploreMaxAttempts}
                            </span>
                          )}
                        </>
                      }
                    />
                  </td>
                  <td className="whitespace-nowrap px-4 py-2.5 text-xs text-gray-500">{method}</td>
                  <td className="px-4 py-2.5">
                    <select
                      value={REC_OPTIONS.includes(r.recommendation) ? r.recommendation : "not recommended"}
                      disabled={r.exploreStatus !== "completed"}
                      onChange={(e) => void setRec(r.retailer, e.target.value as Recommendation)}
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
                  <td className="px-4 py-2.5">
                    <div className="flex items-center justify-end gap-1">
                      <Button
                        size="sm"
                        variant={failedish ? "warn" : "ghost"}
                        title={
                          failedish
                            ? "Retry the failed identification"
                            : "Re-identify this site's config (free for Shopify/WooCommerce/sitemap sites)"
                        }
                        disabled={busyKey !== null || r.exploreStatus === "running"}
                        onClick={() =>
                          void runJob(`explore:${r.retailer}`, `Re-identify ${r.retailer}`, async () => {
                            if (failedish) return api.exploreRetry([r.retailer]);
                            const baseUrl = r.config?.baseUrl;
                            if (!baseUrl) throw new Error("No base URL on config.");
                            return api.explore([baseUrl], false);
                          })
                        }
                      >
                        {failedish ? "Retry" : "Re-identify"}
                      </Button>
                      {r.latestJobId && (
                        <Button
                          size="sm"
                          variant="ghost"
                          title="Watch the latest job log"
                          onClick={() => watchJob(r.latestJobId!, `${r.retailer} — latest job`)}
                        >
                          Log
                        </Button>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>

      <JobDrawer drawer={drawer} />
    </section>
  );
}
