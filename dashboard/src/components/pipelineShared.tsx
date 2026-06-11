import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  subscribeStream,
  type ErrorsResponse,
  type RetailerRow,
  type RetailersOverviewResponse,
} from "../api.ts";
import { Spinner, StatusDot, XIcon } from "./ui.tsx";

/**
 * Shared state for the pipeline tabs (Configure + Crawl): the retailers
 * overview poll, the jobId-returning action runner, and the bottom log drawer
 * that streams any job over SSE. One instance per tab keeps the views simple.
 */

export function hostOf(url: string | undefined): string {
  if (!url) return "";
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

export function timeAgo(iso: string | null | undefined): string {
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

export function retailerName(r: RetailerRow): string {
  return String(r.config?.retailerDisplayName ?? r.retailer);
}

export function isExploreFailedish(r: RetailerRow): boolean {
  return (
    r.exploreStatus === "failed" ||
    r.exploreStatus === "needs_retry" ||
    r.exploreStatus === "queued_retry"
  );
}

export function useRetailersOverview(pollMs = 15000) {
  const [data, setData] = useState<RetailersOverviewResponse | null>(null);
  const [errors, setErrors] = useState<ErrorsResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

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
    const t = setInterval(() => void load(true), pollMs);
    return () => clearInterval(t);
  }, [load, pollMs]);

  return { data, errors, loadError, load };
}

export interface JobDrawerState {
  activeJob: { id: string; title: string } | null;
  jobLines: string[];
  jobDone: boolean;
  close: () => void;
}

export function useJobRunner(reload: (silent?: boolean) => void) {
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [activeJob, setActiveJob] = useState<{ id: string; title: string } | null>(null);
  const [jobLines, setJobLines] = useState<string[]>([]);
  const [jobDone, setJobDone] = useState(false);
  const closeStreamRef = useRef<(() => void) | null>(null);

  useEffect(() => () => closeStreamRef.current?.(), []);

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
          setJobLines((ls) => [
            ...ls,
            typeof event.error === "string" ? `Failed: ${event.error}` : "Done.",
          ]);
          reload(true);
        },
        onError: () => setJobDone(true),
      });
    },
    [reload],
  );

  const runJob = useCallback(
    async (key: string, title: string, fn: () => Promise<{ jobId: string }>) => {
      setBusyKey(key);
      setActionError(null);
      try {
        const { jobId } = await fn();
        watchJob(jobId, title);
        reload(true);
      } catch (e: unknown) {
        setActionError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusyKey(null);
      }
    },
    [watchJob, reload],
  );

  const drawer: JobDrawerState = {
    activeJob,
    jobLines,
    jobDone,
    close: () => {
      closeStreamRef.current?.();
      setActiveJob(null);
    },
  };

  return { busyKey, actionError, setActionError, runJob, watchJob, drawer };
}

export function JobDrawer({ drawer }: { drawer: JobDrawerState }) {
  const logRef = useRef<HTMLDivElement | null>(null);
  // Only auto-scroll when the user is already pinned to the bottom. If they
  // scroll up to read older lines, we leave the viewport alone. We also
  // scroll the log container directly (not via scrollIntoView, which would
  // shift the whole page).
  const pinnedRef = useRef(true);
  const lineCount = drawer.jobLines.length;

  const onScroll = () => {
    const el = logRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    pinnedRef.current = distanceFromBottom < 40;
  };

  useEffect(() => {
    const el = logRef.current;
    if (!el) return;
    if (pinnedRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [lineCount]);

  if (!drawer.activeJob) return null;
  return (
    <div className="fixed inset-x-0 bottom-0 z-40 border-t border-gray-200 bg-white shadow-[0_-8px_24px_rgba(0,0,0,0.08)]">
      <div className="mx-auto max-w-7xl px-6 py-3">
        <div className="mb-2 flex items-center justify-between">
          <span className="flex items-center gap-2 text-sm font-medium text-gray-900">
            {drawer.jobDone ? <StatusDot tone="ok" label="" /> : <Spinner className="text-blue-500" />}
            {drawer.activeJob.title}
            {drawer.jobDone && <span className="text-xs font-normal text-gray-400">finished</span>}
          </span>
          <button
            onClick={drawer.close}
            className="inline-flex size-7 items-center justify-center rounded-md text-gray-400 hover:bg-gray-100 hover:text-gray-700"
            aria-label="Close log"
          >
            <XIcon />
          </button>
        </div>
        <div
          ref={logRef}
          onScroll={onScroll}
          className="max-h-72 overflow-y-auto overscroll-contain rounded-md bg-gray-950 p-3 font-mono text-[11px] leading-relaxed text-gray-300"
        >
          {drawer.jobLines.length === 0 ? (
            <span className="text-gray-500">Waiting for log output…</span>
          ) : (
            drawer.jobLines.map((l, i) => (
              <div key={i} className="whitespace-pre-wrap break-words">
                {l}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

/** Memoized search filter over retailer rows. */
export function useRetailerSearch(rows: RetailerRow[], search: string) {
  return useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(
      (r) =>
        retailerName(r).toLowerCase().includes(q) ||
        r.retailer.toLowerCase().includes(q) ||
        hostOf(r.config?.baseUrl).toLowerCase().includes(q),
    );
  }, [rows, search]);
}
