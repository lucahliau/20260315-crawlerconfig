import { useCallback, useEffect, useRef, useState } from "react";
import { api, subscribeStream, type DiscoveryRun } from "../api.ts";
import { Button, Card, ErrorBanner, PlayIcon, Spinner, StatusDot, cx } from "./ui.tsx";

type RunKind = "discover" | "mine" | "probe";

const KIND_LABEL: Record<RunKind, string> = {
  discover: "Discovery",
  mine: "Stockist mining",
  probe: "Price re-check",
};

/**
 * Pipeline action bar: kick off discovery / stockist mining / price re-checks from
 * the dashboard (instead of the CLI) and watch progress live over SSE. Calls
 * `onChanged` when a run finishes so the brand list can refresh.
 */
const FOCUS_EXAMPLES = [
  "so-cal surfer brands",
  "French workwear",
  "gorpcore / technical outdoor",
  "Japanese minimalist basics",
  "NYC skate-adjacent",
  "Scandinavian knitwear",
];

/** Compact "3d ago" / "2h ago" relative time for the recent-searches list. */
function timeAgo(iso: string): string {
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

export function PipelineActions({ onChanged }: { onChanged?: () => void }) {
  const [running, setRunning] = useState<RunKind | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [finishedKind, setFinishedKind] = useState<RunKind | null>(null);
  const [focus, setFocus] = useState("");
  const [runs, setRuns] = useState<DiscoveryRun[]>([]);
  const closeRef = useRef<(() => void) | null>(null);
  const logEndRef = useRef<HTMLDivElement | null>(null);

  // Auto-scroll the log to the newest line.
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  // Tear down any live stream on unmount.
  useEffect(() => () => closeRef.current?.(), []);

  // Load recent discovery searches (so we don't re-spend credits on dupes).
  const loadRuns = useCallback(() => {
    api
      .getDiscoveryRuns(30)
      .then((r) => setRuns(r.runs))
      .catch(() => setRuns([]));
  }, []);
  useEffect(() => loadRuns(), [loadRuns]);

  // Has the current focus already been searched? (case-insensitive match)
  const focusKey = focus.trim().toLowerCase().replace(/\s+/g, " ");
  const priorRun = focusKey
    ? runs.find((r) => (r.category ?? "").trim().toLowerCase().replace(/\s+/g, " ") === focusKey)
    : undefined;

  const start = useCallback(
    async (kind: RunKind) => {
      if (running) return;
      setRunning(kind);
      setError(null);
      setFinishedKind(null);
      setLines([`Starting ${KIND_LABEL[kind].toLowerCase()}…`]);
      try {
        const streamUrl =
          kind === "discover"
            ? api.jobStreamUrl((await api.discover(focus.trim() || undefined)).jobId)
            : kind === "mine"
              ? api.taskStreamUrl((await api.mineStockists()).taskId)
              : api.taskStreamUrl((await api.probeBrands({ onlyCandidates: true })).taskId);

        closeRef.current = subscribeStream(streamUrl, {
          onLog: (line) => setLines((ls) => [...ls, line]),
          onDone: (event) => {
            setRunning(null);
            setFinishedKind(kind);
            if (typeof event.error === "string") setError(event.error);
            if (kind === "discover") loadRuns();
            onChanged?.();
          },
          onError: (e) => {
            setRunning(null);
            setError(e.message);
          },
        });
      } catch (e: unknown) {
        setRunning(null);
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    [running, onChanged, focus, loadRuns],
  );

  return (
    <Card>
      <div className="space-y-4 p-4">
        {/* Discovery focus: free-text aesthetic/region/activity that steers the Gemini search. */}
        <div className="space-y-2">
          <label htmlFor="discovery-focus" className="block text-xs font-medium text-gray-700">
            Discovery focus{" "}
            <span className="font-normal text-gray-400">
              — optional aesthetic, region, or activity to steer the search
            </span>
          </label>
          <input
            id="discovery-focus"
            data-testid="discovery-focus"
            type="text"
            value={focus}
            onChange={(e) => setFocus(e.target.value)}
            disabled={running !== null}
            maxLength={500}
            placeholder={`e.g. ${FOCUS_EXAMPLES[0]}, ${FOCUS_EXAMPLES[1]}…`}
            className="h-9 w-full max-w-xl rounded-md border border-gray-300 bg-white px-3 text-sm text-gray-900 placeholder:text-gray-400 focus:border-gray-400 focus:outline-none disabled:cursor-not-allowed disabled:bg-gray-50"
          />
          <div className="flex flex-wrap gap-1.5">
            {FOCUS_EXAMPLES.map((ex) => (
              <button
                key={ex}
                type="button"
                disabled={running !== null}
                onClick={() => setFocus(ex)}
                className={cx(
                  "rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-[11px] text-gray-500 transition-colors",
                  "hover:border-gray-300 hover:text-gray-800 disabled:cursor-not-allowed disabled:opacity-40",
                )}
              >
                {ex}
              </button>
            ))}
            {focus && (
              <button
                type="button"
                disabled={running !== null}
                onClick={() => setFocus("")}
                className="rounded-full px-2 py-1 text-[11px] text-gray-400 hover:text-gray-700 disabled:opacity-40"
              >
                Clear
              </button>
            )}
          </div>
          {priorRun && (
            <p className="text-xs text-amber-700">
              Already searched “{priorRun.category}” {timeAgo(priorRun.ranAt)} (found{" "}
              {priorRun.newCount} new). Running again may spend credits for few new brands.
            </p>
          )}
        </div>

        {/* Recent searches: persisted across redeploys; click to reuse a focus. */}
        {runs.length > 0 && (
          <div className="space-y-1.5">
            <p className="text-xs font-medium text-gray-700">Recent searches</p>
            <div className="flex flex-wrap gap-1.5">
              {runs.slice(0, 12).map((r, i) => (
                <button
                  key={`${r.ranAt}-${i}`}
                  type="button"
                  disabled={running !== null}
                  onClick={() => r.category && setFocus(r.category)}
                  title={`${timeAgo(r.ranAt)} · +${r.newCount} new · ${r.totalCount} total`}
                  className="rounded-full border border-gray-200 bg-white px-2.5 py-1 text-[11px] text-gray-500 transition-colors hover:border-gray-300 hover:text-gray-800 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  {r.category ?? "general sweep"}{" "}
                  <span className="tnum text-gray-400">+{r.newCount}</span>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-2 border-t border-gray-100 bg-gray-50/60 px-4 py-3">
        <Button
          data-testid="action-discover"
          variant="primary"
          disabled={running !== null}
          onClick={() => void start("discover")}
        >
          {running === "discover" ? <Spinner className="text-white" /> : <PlayIcon />}
          Run discovery
        </Button>
        <Button
          data-testid="action-mine"
          disabled={running !== null}
          onClick={() => void start("mine")}
        >
          {running === "mine" && <Spinner />}
          Mine stockists
        </Button>
        <Button
          data-testid="action-probe"
          disabled={running !== null}
          onClick={() => void start("probe")}
        >
          {running === "probe" && <Spinner />}
          Re-check prices
        </Button>
        <span className="ml-1 hidden text-xs text-gray-400 sm:inline">
          Discovery uses Gemini credits; mining and price checks are free.
        </span>
        {running && (
          <span className="ml-auto">
            <StatusDot tone="running" label={`${KIND_LABEL[running]} running…`} />
          </span>
        )}
        {!running && finishedKind && !error && (
          <span className="ml-auto">
            <StatusDot tone="ok" label={`${KIND_LABEL[finishedKind]} done`} />
          </span>
        )}
      </div>

      {(error || lines.length > 0) && (
        <div className="space-y-2 border-t border-gray-100 p-4">
          {error && <ErrorBanner onDismiss={() => setError(null)}>{error}</ErrorBanner>}
          {lines.length > 0 && (
            <div className="max-h-56 overflow-y-auto rounded-md bg-gray-950 p-3 font-mono text-[11px] leading-relaxed text-gray-300">
              {lines.map((l, i) => (
                <div key={i} className="whitespace-pre-wrap break-words">
                  {l}
                </div>
              ))}
              <div ref={logEndRef} />
            </div>
          )}
        </div>
      )}
    </Card>
  );
}
