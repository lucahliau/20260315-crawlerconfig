import { useState } from "react";
import { DiscoverView } from "./components/DiscoverView.tsx";
import { ConfigureView } from "./components/ConfigureView.tsx";
import { CrawlView } from "./components/CrawlView.tsx";
import { ProcessView } from "./components/ProcessView.tsx";
import { SystemsView } from "./components/SystemsView.tsx";
import { AnalyticsView } from "./components/AnalyticsView.tsx";
import { ErrorsView } from "./components/ErrorsView.tsx";
import { cx } from "./components/ui.tsx";

type Tab = "discover" | "configure" | "crawl" | "process" | "systems" | "analytics" | "errors";

/**
 * Tabs mirror the pipeline itself, in order:
 *   Discover (find + approve brands) → Configure (identify a crawl config per
 *   site) → Crawl (collect URLs, upload products) → Process (MacBook
 *   background-removal + embeddings) → Systems (third-party health).
 */
const TABS: { key: Tab; label: string; step?: number }[] = [
  { key: "discover", label: "Discover", step: 1 },
  { key: "configure", label: "Configure", step: 2 },
  { key: "crawl", label: "Crawl", step: 3 },
  { key: "process", label: "Process", step: 4 },
  { key: "systems", label: "System health" },
  { key: "errors", label: "Errors" },
  { key: "analytics", label: "User analytics" },
];

export function App() {
  const [tab, setTab] = useState<Tab>("discover");

  return (
    <div className="min-h-full bg-gray-50 text-gray-900">
      <header className="sticky top-0 z-30 border-b border-gray-200 bg-white/90 backdrop-blur">
        <div className="mx-auto flex h-14 max-w-7xl items-center gap-8 px-6">
          <div className="flex items-center gap-2.5">
            <span className="flex size-6 items-center justify-center rounded-md bg-gray-900 text-[11px] font-semibold text-white">
              CP
            </span>
            <span className="text-sm font-semibold tracking-tight">Clothing Pipeline</span>
          </div>
          <nav className="flex h-full items-stretch gap-5" aria-label="Pipeline stages">
            {TABS.map((t) => (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={cx(
                  "relative inline-flex items-center gap-1.5 text-sm transition-colors",
                  tab === t.key ? "font-medium text-gray-900" : "text-gray-500 hover:text-gray-800",
                  t.key === "systems" && "ml-3",
                )}
              >
                {t.step !== undefined && (
                  <span
                    className={cx(
                      "tnum flex size-4 items-center justify-center rounded-full text-[10px] font-semibold",
                      tab === t.key ? "bg-gray-900 text-white" : "bg-gray-200 text-gray-500",
                    )}
                  >
                    {t.step}
                  </span>
                )}
                {t.label}
                {tab === t.key && (
                  <span className="absolute inset-x-0 -bottom-px h-0.5 rounded-full bg-gray-900" />
                )}
              </button>
            ))}
          </nav>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-6 py-8">
        {tab === "discover" ? (
          <DiscoverView />
        ) : tab === "configure" ? (
          <ConfigureView />
        ) : tab === "crawl" ? (
          <CrawlView />
        ) : tab === "process" ? (
          <ProcessView />
        ) : tab === "analytics" ? (
          <AnalyticsView />
        ) : tab === "errors" ? (
          <ErrorsView />
        ) : (
          <SystemsView />
        )}
      </main>
    </div>
  );
}
