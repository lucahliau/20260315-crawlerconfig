import { useState } from "react";
import { BrandCuration } from "./components/BrandCuration.tsx";
import { LeadsView } from "./components/LeadsView.tsx";
import { RetailersView } from "./components/RetailersView.tsx";
import { cx } from "./components/ui.tsx";

type Tab = "brands" | "leads" | "retailers";

const TABS: { key: Tab; label: string }[] = [
  { key: "brands", label: "Brands" },
  { key: "leads", label: "Leads" },
  { key: "retailers", label: "Retailers" },
];

/**
 * App shell. The unified control panel for the clothing content pipeline.
 * Tabs follow the funnel: Brands (discover + approve), Leads (promote mined
 * stockist names), Retailers (explore → crawl → upload operations per site).
 */
export function App() {
  const [tab, setTab] = useState<Tab>("brands");
  // Bump to force a refresh of dependent views after cross-tab actions.
  const [, setReloadKey] = useState(0);

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
          <nav className="flex h-full items-stretch gap-5" aria-label="Sections">
            {TABS.map((t) => (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={cx(
                  "relative inline-flex items-center text-sm transition-colors",
                  tab === t.key
                    ? "font-medium text-gray-900"
                    : "text-gray-500 hover:text-gray-800",
                )}
              >
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
        {tab === "brands" ? (
          <BrandCuration />
        ) : tab === "leads" ? (
          <LeadsView onAdded={() => setReloadKey((k) => k + 1)} />
        ) : (
          <RetailersView />
        )}
      </main>
    </div>
  );
}
