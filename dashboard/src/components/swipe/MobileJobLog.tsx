import { useEffect, useRef, useState } from "react";
import { subscribeStream } from "../../api.ts";

/**
 * Full-screen live log sheet for any SSE stream (job or task). Used by the
 * mobile Pipeline tab to watch explores/crawls kicked off from the phone.
 */
export function MobileJobLog({
  streamUrl,
  title,
  onClose,
}: {
  streamUrl: string;
  title: string;
  onClose: () => void;
}) {
  const [lines, setLines] = useState<string[]>([]);
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const boxRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    setLines([]);
    setDone(false);
    setError(null);
    const close = subscribeStream(streamUrl, {
      onLog: (line) => setLines((ls) => [...ls, line]),
      onDone: (event) => {
        setDone(true);
        if (typeof event.error === "string") setError(event.error);
      },
      onError: (e) => setError(e.message),
    });
    return close;
  }, [streamUrl]);

  // Autoscroll only while the user is near the bottom, and scroll the
  // container directly — scrollIntoView would yank the whole page.
  useEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 64;
    if (nearBottom) box.scrollTop = box.scrollHeight;
  }, [lines]);

  return (
    <div
      className="fixed inset-0 z-50 flex flex-col bg-gray-950"
      style={{
        paddingTop: "env(safe-area-inset-top)",
        paddingBottom: "env(safe-area-inset-bottom)",
      }}
    >
      <header className="flex items-center justify-between border-b border-gray-800 px-5 py-3">
        <div className="min-w-0">
          <p className="truncate text-sm font-semibold text-gray-100">{title}</p>
          <p className="text-[11px] text-gray-500">
            {error ? "Failed" : done ? "Finished" : "Running…"}
          </p>
        </div>
        <button
          onClick={onClose}
          className="shrink-0 rounded-full border border-gray-700 bg-gray-900 px-3.5 py-1.5 text-xs font-medium text-gray-200 active:bg-gray-800"
        >
          Close
        </button>
      </header>

      {error && (
        <div className="mx-4 mt-3 rounded-xl border border-red-900 bg-red-950/60 px-3.5 py-2.5 text-xs text-red-300">
          {error}
        </div>
      )}
      {done && !error && (
        <div className="mx-4 mt-3 rounded-xl border border-emerald-800 bg-emerald-950/50 px-3.5 py-2.5 text-xs text-emerald-300">
          Finished.
        </div>
      )}

      <div
        ref={boxRef}
        className="m-4 min-h-0 flex-1 overflow-y-auto overscroll-contain rounded-xl border border-gray-800 bg-gray-950 p-3 font-mono text-[11px] leading-relaxed text-gray-400"
      >
        {lines.length === 0 ? (
          <p className="text-gray-600">Waiting for output…</p>
        ) : (
          lines.map((l, i) => (
            <div key={i} className="whitespace-pre-wrap break-words">
              {l}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
