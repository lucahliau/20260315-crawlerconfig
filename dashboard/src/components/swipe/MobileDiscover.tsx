import { useCallback, useEffect, useRef, useState } from "react";
import { api, subscribeStream, type DiscoveryRun } from "../../api.ts";
import { cx } from "../ui.tsx";

/**
 * Phone-sized mirror of the desktop discovery tools: run an AI brand search
 * (with a steerable focus), mine stockists, or re-check prices — then watch
 * the live log and jump back to Review when new brands land in the queue.
 */

type RunKind = "discover" | "mine" | "probe";

const KIND_LABEL: Record<RunKind, string> = {
  discover: "Discovery",
  mine: "Stockist mining",
  probe: "Price re-check",
};

const FOCUS_EXAMPLES = [
  "so-cal surfer brands",
  "French workwear",
  "gorpcore / technical outdoor",
  "Japanese minimalist basics",
  "NYC skate-adjacent",
  "Scandinavian knitwear",
];

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

export function MobileDiscover({ onChanged }: { onChanged?: () => void }) {
  const [running, setRunning] = useState<RunKind | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [finishedKind, setFinishedKind] = useState<RunKind | null>(null);
  const [focus, setFocus] = useState("");
  const [runs, setRuns] = useState<DiscoveryRun[]>([]);
  const closeRef = useRef<(() => void) | null>(null);
  const logEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);
  useEffect(() => () => closeRef.current?.(), []);

  const loadRuns = useCallback(() => {
    api
      .getDiscoveryRuns(30)
      .then((r) => setRuns(r.runs))
      .catch(() => setRuns([]));
  }, []);
  useEffect(() => loadRuns(), [loadRuns]);

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
    <div className="flex h-full flex-col gap-4 overflow-y-auto px-5 pb-4">
      <div className="space-y-2">
        <label htmlFor="m-discovery-focus" className="block text-xs font-medium text-gray-400">
          Discovery focus <span className="font-normal text-gray-600">— optional steer</span>
        </label>
        <input
          id="m-discovery-focus"
          type="text"
          value={focus}
          onChange={(e) => setFocus(e.target.value)}
          disabled={running !== null}
          maxLength={500}
          placeholder="e.g. French workwear, gorpcore…"
          className="h-11 w-full rounded-xl border border-gray-700 bg-gray-900 px-3.5 text-[15px] text-gray-100 placeholder:text-gray-600 focus:border-gray-500 focus:outline-none disabled:opacity-50"
        />
        <div className="flex flex-wrap gap-1.5">
          {FOCUS_EXAMPLES.map((ex) => (
            <button
              key={ex}
              type="button"
              disabled={running !== null}
              onClick={() => setFocus(ex)}
              className="rounded-full border border-gray-700 bg-gray-900 px-2.5 py-1.5 text-[11px] text-gray-400 active:bg-gray-800 disabled:opacity-40"
            >
              {ex}
            </button>
          ))}
        </div>
        {priorRun && (
          <p className="text-xs text-amber-400/90">
            Already searched “{priorRun.category}” {timeAgo(priorRun.ranAt)} (+{priorRun.newCount}{" "}
            new) — running again may spend credits for little.
          </p>
        )}
      </div>

      {runs.length > 0 && (
        <div className="space-y-1.5">
          <p className="text-xs font-medium text-gray-400">Recent searches</p>
          <div className="flex flex-wrap gap-1.5">
            {runs.slice(0, 8).map((r, i) => (
              <button
                key={`${r.ranAt}-${i}`}
                type="button"
                disabled={running !== null}
                onClick={() => r.category && setFocus(r.category)}
                className="rounded-full border border-gray-800 bg-gray-950 px-2.5 py-1.5 text-[11px] text-gray-500 active:bg-gray-900 disabled:opacity-40"
              >
                {r.category ?? "general sweep"} <span className="text-gray-600">+{r.newCount}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="space-y-2">
        <button
          disabled={running !== null}
          onClick={() => void start("discover")}
          className={cx(
            "flex h-12 w-full items-center justify-center gap-2 rounded-xl text-[15px] font-semibold",
            "bg-white text-gray-900 active:bg-gray-200 disabled:opacity-50",
          )}
        >
          {running === "discover" ? "Searching…" : "Run discovery"}
        </button>
        <div className="grid grid-cols-2 gap-2">
          <button
            disabled={running !== null}
            onClick={() => void start("mine")}
            className="flex h-11 items-center justify-center rounded-xl border border-gray-700 bg-gray-900 text-sm font-medium text-gray-200 active:bg-gray-800 disabled:opacity-50"
          >
            {running === "mine" ? "Mining…" : "Mine stockists"}
          </button>
          <button
            disabled={running !== null}
            onClick={() => void start("probe")}
            className="flex h-11 items-center justify-center rounded-xl border border-gray-700 bg-gray-900 text-sm font-medium text-gray-200 active:bg-gray-800 disabled:opacity-50"
          >
            {running === "probe" ? "Checking…" : "Re-check prices"}
          </button>
        </div>
        <p className="text-center text-[11px] text-gray-600">
          Discovery uses Gemini credits; mining and price checks are free.
        </p>
      </div>

      {error && (
        <div className="rounded-xl border border-red-900 bg-red-950/60 px-3.5 py-2.5 text-xs text-red-300">
          {error}
        </div>
      )}
      {!running && finishedKind && !error && (
        <div className="rounded-xl border border-emerald-800 bg-emerald-950/50 px-3.5 py-2.5 text-xs text-emerald-300">
          {KIND_LABEL[finishedKind]} finished — new brands are in the Review queue.
        </div>
      )}

      {lines.length > 0 && (
        <div className="min-h-32 flex-1 overflow-y-auto rounded-xl border border-gray-800 bg-gray-950 p-3 font-mono text-[11px] leading-relaxed text-gray-400">
          {lines.map((l, i) => (
            <div key={i} className="whitespace-pre-wrap break-words">
              {l}
            </div>
          ))}
          <div ref={logEndRef} />
        </div>
      )}
    </div>
  );
}
