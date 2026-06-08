import { useCallback, useEffect, useRef, useState } from "react";
import { api, subscribeStream } from "../api.ts";

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

export function PipelineActions({ onChanged }: { onChanged?: () => void }) {
  const [running, setRunning] = useState<RunKind | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [finishedKind, setFinishedKind] = useState<RunKind | null>(null);
  const [focus, setFocus] = useState("");
  const closeRef = useRef<(() => void) | null>(null);
  const logEndRef = useRef<HTMLDivElement | null>(null);

  // Auto-scroll the log to the newest line.
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  // Tear down any live stream on unmount.
  useEffect(() => () => closeRef.current?.(), []);

  const start = useCallback(
    async (kind: RunKind) => {
      if (running) return;
      setRunning(kind);
      setError(null);
      setFinishedKind(null);
      setLines([`▶ Starting ${KIND_LABEL[kind].toLowerCase()}…`]);
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
    [running, onChanged, focus],
  );

  return (
    <section className="space-y-3 rounded-xl border border-neutral-800 bg-neutral-900/40 p-4">
      {/* Discovery focus: free-text aesthetic/region/activity that steers the Gemini search. */}
      <div className="space-y-1.5">
        <label htmlFor="discovery-focus" className="text-xs font-medium text-neutral-400">
          Discovery focus{" "}
          <span className="font-normal text-neutral-600">
            (optional — an aesthetic, region, or activity to steer the search)
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
          placeholder={`e.g. ${FOCUS_EXAMPLES[0]}, ${FOCUS_EXAMPLES[1]}, ${FOCUS_EXAMPLES[2]}…`}
          className="w-full rounded-lg border border-neutral-700 bg-neutral-950 px-3 py-2 text-sm text-neutral-100 placeholder:text-neutral-600 focus:border-neutral-500 focus:outline-none disabled:cursor-not-allowed disabled:opacity-50"
        />
        <div className="flex flex-wrap gap-1.5">
          {FOCUS_EXAMPLES.map((ex) => (
            <button
              key={ex}
              type="button"
              disabled={running !== null}
              onClick={() => setFocus(ex)}
              className="rounded-full border border-neutral-800 bg-neutral-900 px-2.5 py-1 text-[11px] text-neutral-400 transition hover:border-neutral-600 hover:text-neutral-200 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {ex}
            </button>
          ))}
          {focus && (
            <button
              type="button"
              disabled={running !== null}
              onClick={() => setFocus("")}
              className="rounded-full border border-neutral-800 bg-neutral-900 px-2.5 py-1 text-[11px] text-neutral-500 transition hover:border-red-800 hover:text-red-300 disabled:cursor-not-allowed disabled:opacity-40"
            >
              clear
            </button>
          )}
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <ActionButton
          testId="action-discover"
          label="Run discovery"
          hint={focus.trim() ? `focus: ${focus.trim()}` : "Gemini + Google Search"}
          busy={running === "discover"}
          disabled={running !== null}
          onClick={() => void start("discover")}
        />
        <ActionButton
          testId="action-mine"
          label="Mine stockists"
          hint="zero-AI lead scrape"
          busy={running === "mine"}
          disabled={running !== null}
          onClick={() => void start("mine")}
        />
        <ActionButton
          testId="action-probe"
          label="Re-check prices"
          hint="probe candidate tiers"
          busy={running === "probe"}
          disabled={running !== null}
          onClick={() => void start("probe")}
        />
        {running && (
          <span className="ml-auto flex items-center gap-2 text-xs text-neutral-400">
            <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-400" />
            {KIND_LABEL[running]} running…
          </span>
        )}
        {!running && finishedKind && !error && (
          <span className="ml-auto text-xs text-emerald-400">✓ {KIND_LABEL[finishedKind]} done</span>
        )}
      </div>

      {error && (
        <div className="rounded-md border border-red-900 bg-red-950/50 px-3 py-2 text-xs text-red-300">
          {error}
        </div>
      )}

      {lines.length > 0 && (
        <div className="max-h-56 overflow-y-auto rounded-lg border border-neutral-800 bg-neutral-950 p-3 font-mono text-[11px] leading-relaxed text-neutral-300">
          {lines.map((l, i) => (
            <div key={i} className="whitespace-pre-wrap break-words">
              {l}
            </div>
          ))}
          <div ref={logEndRef} />
        </div>
      )}
    </section>
  );
}

function ActionButton({
  testId,
  label,
  hint,
  busy,
  disabled,
  onClick,
}: {
  testId: string;
  label: string;
  hint: string;
  busy: boolean;
  disabled: boolean;
  onClick: () => void;
}) {
  return (
    <button
      data-testid={testId}
      onClick={onClick}
      disabled={disabled}
      className={`flex flex-col items-start rounded-lg border px-3 py-2 text-left transition disabled:cursor-not-allowed disabled:opacity-50 ${
        busy
          ? "border-emerald-600/50 bg-emerald-600/10"
          : "border-neutral-700 bg-neutral-800/60 hover:border-neutral-600 hover:bg-neutral-800"
      }`}
    >
      <span className="text-sm font-medium text-neutral-100">
        {busy ? "Running…" : label}
      </span>
      <span className="max-w-[12rem] truncate text-[11px] text-neutral-500">{hint}</span>
    </button>
  );
}
