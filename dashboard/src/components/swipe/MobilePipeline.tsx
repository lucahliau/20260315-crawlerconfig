import { useCallback, useEffect, useState } from "react";
import {
  api,
  type PipelineSettings,
  type RetailerRow,
} from "../../api.ts";
import { useRetailersOverview } from "../pipelineShared.tsx";
import { cx } from "../ui.tsx";
import { MobileJobLog } from "./MobileJobLog.tsx";

/**
 * Phone view of the configure → crawl → upload pipeline. A conveyor card on
 * top controls the autopilot (kill switch + per-stage gates); below it every
 * retailer is a tappable row with explore/crawl/upload status dots — tapping
 * opens an action sheet (Identify / Crawl / Queue upload / Watch log).
 */

type Dot = "ok" | "running" | "failed" | "none";

function exploreDot(r: RetailerRow): Dot {
  switch (r.exploreStatus) {
    case "completed":
      return "ok";
    case "running":
      return "running";
    case "failed":
    case "needs_retry":
    case "queued_retry":
      return "failed";
    default:
      return r.configValid ? "ok" : "none";
  }
}

function crawlDot(r: RetailerRow): Dot {
  const live = r.crawlLive?.status;
  if (live === "running") return "running";
  if (live === "failed") return "failed";
  if (live === "completed" || r.crawl) return "ok";
  return "none";
}

function uploadDot(r: RetailerRow): Dot {
  const live = r.uploadLive?.status;
  if (live === "running") return "running";
  if (live === "failed") return "failed";
  if (live === "completed" || r.upload) return "ok";
  return "none";
}

const DOT_CLASS: Record<Dot, string> = {
  ok: "bg-emerald-500",
  running: "bg-blue-500 animate-pulse",
  failed: "bg-red-500",
  none: "bg-gray-700",
};

function rowPriority(r: RetailerRow): number {
  const dots = [exploreDot(r), crawlDot(r), uploadDot(r)];
  if (dots.includes("running")) return 0;
  if (dots.includes("failed")) return 1;
  if (uploadDot(r) !== "ok") return 2;
  return 3;
}

export function MobilePipeline() {
  const { data, loadError, load } = useRetailersOverview(15000);
  const [settings, setSettings] = useState<PipelineSettings | null>(null);
  const [selected, setSelected] = useState<RetailerRow | null>(null);
  const [selectedOptout, setSelectedOptout] = useState<boolean | null>(null);
  const [logTarget, setLogTarget] = useState<{ url: string; title: string } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .getPipelineSettings()
      .then(setSettings)
      .catch(() => setSettings(null));
  }, []);

  // Opt-out state loads lazily when a row's action sheet opens.
  useEffect(() => {
    if (!selected) {
      setSelectedOptout(null);
      return;
    }
    let cancelled = false;
    api
      .getAutopilotOptout(selected.retailer)
      .then((r) => {
        if (!cancelled) setSelectedOptout(r.optout);
      })
      .catch(() => {
        if (!cancelled) setSelectedOptout(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selected]);

  const toggleSetting = useCallback(
    (key: keyof PipelineSettings) => {
      if (!settings) return;
      const next = { ...settings, [key]: !settings[key] };
      setSettings(next); // optimistic
      api.setPipelineSettings({ [key]: next[key] }).catch((e: unknown) => {
        setSettings(settings);
        setError(e instanceof Error ? e.message : String(e));
      });
    },
    [settings],
  );

  const runAction = useCallback(
    async (key: string, fn: () => Promise<void>) => {
      if (busy) return;
      setBusy(key);
      setError(null);
      setNotice(null);
      try {
        await fn();
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(null);
      }
    },
    [busy],
  );

  const rows = [...(data?.retailers ?? [])].sort(
    (a, b) => rowPriority(a) - rowPriority(b) || a.retailer.localeCompare(b.retailer),
  );
  const backlog = data?.identifiedWithoutConfig ?? [];

  return (
    <div className="flex h-full flex-col gap-4 overflow-y-auto px-5 pb-4">
      {/* Conveyor / autopilot card */}
      <div className="space-y-2.5 rounded-xl border border-gray-800 bg-gray-900 p-3.5">
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <p className="text-sm font-semibold text-gray-100">Autopilot</p>
            <p className="text-[11px] text-gray-500">
              approve → config → crawl → upload → process
            </p>
          </div>
          <Toggle
            on={settings?.auto_pipeline ?? true}
            disabled={!settings}
            onTap={() => toggleSetting("auto_pipeline")}
          />
        </div>
        <div className="flex gap-1.5">
          {(
            [
              ["gate_crawl", "Crawl"],
              ["gate_upload", "Upload"],
              ["gate_processing", "Process"],
            ] as const
          ).map(([key, label]) => (
            <button
              key={key}
              disabled={!settings || !settings.auto_pipeline}
              onClick={() => toggleSetting(key)}
              className={cx(
                "rounded-full border px-2.5 py-1.5 text-[11px] font-medium disabled:opacity-40",
                settings?.[key]
                  ? "border-emerald-800 bg-emerald-950/50 text-emerald-300"
                  : "border-gray-700 bg-gray-900 text-gray-500",
              )}
            >
              {label} {settings?.[key] ? "on" : "off"}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div className="rounded-xl border border-red-900 bg-red-950/60 px-3.5 py-2.5 text-xs text-red-300">
          {error}
        </div>
      )}
      {notice && (
        <div className="rounded-xl border border-emerald-800 bg-emerald-950/50 px-3.5 py-2.5 text-xs text-emerald-300">
          {notice}
        </div>
      )}
      {loadError && (
        <div className="rounded-xl border border-red-900 bg-red-950/60 px-3.5 py-2.5 text-xs text-red-300">
          {loadError}
        </div>
      )}

      {/* Waiting-for-config backlog */}
      {backlog.length > 0 && (
        <div className="space-y-1.5">
          <p className="text-xs font-medium text-gray-400">
            Waiting for a config <span className="text-gray-600">({backlog.length})</span>
          </p>
          <div className="space-y-1.5">
            {backlog.slice(0, 10).map((b) => (
              <div
                key={b.url}
                className="flex items-center justify-between gap-2 rounded-xl border border-gray-800 bg-gray-900 px-3 py-2.5"
              >
                <span className="min-w-0 truncate text-[13px] text-gray-200">{b.name}</span>
                <button
                  disabled={busy !== null}
                  onClick={() =>
                    void runAction(`identify:${b.url}`, async () => {
                      const r = await api.explore([b.url], true);
                      setLogTarget({ url: api.jobStreamUrl(r.jobId), title: `Identify · ${b.name}` });
                    })
                  }
                  className="shrink-0 rounded-full border border-gray-700 bg-gray-800 px-3 py-1.5 text-[11px] font-medium text-gray-200 active:bg-gray-700 disabled:opacity-40"
                >
                  {busy === `identify:${b.url}` ? "Starting…" : "Identify"}
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Retailer rows */}
      <div className="space-y-1.5">
        <p className="text-xs font-medium text-gray-400">
          Sites <span className="text-gray-600">({rows.length})</span>
        </p>
        {!data ? (
          <p className="py-6 text-center text-sm text-gray-500">Loading retailers…</p>
        ) : rows.length === 0 ? (
          <p className="py-6 text-center text-sm text-gray-500">No configured sites yet.</p>
        ) : (
          rows.map((r) => (
            <button
              key={r.retailer}
              onClick={() => setSelected(r)}
              className="flex w-full items-center justify-between gap-2 rounded-xl border border-gray-800 bg-gray-900 px-3 py-2.5 text-left active:bg-gray-800"
            >
              <div className="min-w-0">
                <p className="truncate text-[13px] font-medium text-gray-200">
                  {r.config.retailerDisplayName ?? r.retailer}
                </p>
                <p className="truncate text-[11px] text-gray-500">
                  {r.recommendation}
                  {r.crawl ? ` · ${r.crawl.totalUrls} urls` : ""}
                  {r.uploadLive?.status === "running"
                    ? ` · uploading ${r.uploadLive.uploaded ?? 0}/${r.uploadLive.total ?? "?"}`
                    : ""}
                </p>
              </div>
              <span className="flex shrink-0 items-center gap-1.5">
                {[exploreDot(r), crawlDot(r), uploadDot(r)].map((d, i) => (
                  <span key={i} className={cx("size-2 rounded-full", DOT_CLASS[d])} />
                ))}
              </span>
            </button>
          ))
        )}
        <p className="pt-1 text-center text-[10px] text-gray-600">
          dots: config · crawl · upload — tap a site for actions
        </p>
      </div>

      {/* Action sheet */}
      {selected && (
        <div className="fixed inset-0 z-40 flex flex-col justify-end bg-black/60" onClick={() => setSelected(null)}>
          <div
            className="space-y-2 rounded-t-2xl border-t border-gray-800 bg-gray-950 p-5"
            style={{ paddingBottom: "calc(env(safe-area-inset-bottom) + 20px)" }}
            onClick={(e) => e.stopPropagation()}
          >
            <p className="text-sm font-semibold text-gray-100">
              {selected.config.retailerDisplayName ?? selected.retailer}
            </p>
            <p className="text-[11px] text-gray-500">
              config {selected.exploreStatus} · {selected.recommendation}
            </p>
            <div className="space-y-2 pt-1">
              <SheetButton
                label={busy?.startsWith("identify") ? "Starting…" : "Identify config (AI rungs allowed)"}
                disabled={busy !== null}
                onTap={() =>
                  void runAction(`identify:${selected.retailer}`, async () => {
                    const url = selected.config.baseUrl;
                    if (!url) throw new Error("No base URL stored for this retailer.");
                    const r = await api.explore([url], false);
                    setSelected(null);
                    setLogTarget({ url: api.jobStreamUrl(r.jobId), title: `Identify · ${selected.retailer}` });
                  })
                }
              />
              <SheetButton
                label={busy?.startsWith("crawl") ? "Starting…" : "Crawl product URLs"}
                disabled={busy !== null || !selected.configValid}
                onTap={() =>
                  void runAction(`crawl:${selected.retailer}`, async () => {
                    const r = await api.crawl(selected.retailer);
                    setSelected(null);
                    setLogTarget({ url: api.jobStreamUrl(r.jobId), title: `Crawl · ${selected.retailer}` });
                  })
                }
              />
              <SheetButton
                label={busy?.startsWith("upload") ? "Queueing…" : "Queue upload (home server)"}
                disabled={busy !== null}
                onTap={() =>
                  void runAction(`upload:${selected.retailer}`, async () => {
                    const r = await api.uploadEnqueue(selected.retailer);
                    setSelected(null);
                    setNotice(`${selected.retailer}: ${r.enqueued} URLs queued (${r.duplicates} already queued).`);
                    void load(true);
                  })
                }
              />
              {selected.latestJobId && (
                <SheetButton
                  label="Watch latest job log"
                  disabled={false}
                  onTap={() => {
                    setLogTarget({
                      url: api.jobStreamUrl(selected.latestJobId!),
                      title: `Log · ${selected.retailer}`,
                    });
                    setSelected(null);
                  }}
                />
              )}
              <div className="flex items-center justify-between rounded-xl border border-gray-800 bg-gray-900 px-3.5 py-3">
                <span className="text-sm text-gray-200">Autopilot for this site</span>
                <Toggle
                  on={!(selectedOptout ?? false)}
                  disabled={selectedOptout === null}
                  onTap={() => {
                    const nextOptout = !(selectedOptout ?? false);
                    setSelectedOptout(nextOptout);
                    api.setAutopilotOptout(selected.retailer, nextOptout).catch((e: unknown) => {
                      setSelectedOptout(!nextOptout);
                      setError(e instanceof Error ? e.message : String(e));
                    });
                  }}
                />
              </div>
              <button
                onClick={() => setSelected(null)}
                className="h-11 w-full rounded-xl border border-gray-700 bg-gray-900 text-sm font-medium text-gray-400 active:bg-gray-800"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {logTarget && (
        <MobileJobLog
          streamUrl={logTarget.url}
          title={logTarget.title}
          onClose={() => {
            setLogTarget(null);
            void load(true);
          }}
        />
      )}
    </div>
  );
}

function SheetButton({
  label,
  disabled,
  onTap,
}: {
  label: string;
  disabled: boolean;
  onTap: () => void;
}) {
  return (
    <button
      disabled={disabled}
      onClick={onTap}
      className="h-11 w-full rounded-xl border border-gray-700 bg-gray-900 text-sm font-medium text-gray-200 active:bg-gray-800 disabled:opacity-40"
    >
      {label}
    </button>
  );
}

function Toggle({ on, disabled, onTap }: { on: boolean; disabled: boolean; onTap: () => void }) {
  return (
    <button
      disabled={disabled}
      onClick={onTap}
      aria-pressed={on}
      className={cx(
        "relative h-7 w-12 rounded-full transition-colors disabled:opacity-40",
        on ? "bg-emerald-500" : "bg-gray-700",
      )}
    >
      <span
        className={cx(
          "absolute left-0 top-0.5 size-6 rounded-full bg-white transition-transform",
          on ? "translate-x-[22px]" : "translate-x-0.5",
        )}
      />
    </button>
  );
}
