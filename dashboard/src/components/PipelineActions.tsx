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
export function PipelineActions({ onChanged }: { onChanged?: () => void }) {
  const [running, setRunning] = useState<RunKind | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [finishedKind, setFinishedKind] = useState<RunKind | null>(null);
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
            ? api.jobStreamUrl((await api.discover()).jobId)
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
    [running, onChanged],
  );

  return (
    <section className="space-y-3 rounded-xl border border-neutral-800 bg-neutral-900/40 p-4">
      <div className="flex flex-wrap items-center gap-2">
        <ActionButton
          testId="action-discover"
          label="Run discovery"
          hint="Gemini + Google Search"
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
      <span className="text-[11px] text-neutral-500">{hint}</span>
    </button>
  );
}
