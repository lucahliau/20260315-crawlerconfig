import { useState } from "react";
import { BrandCuration } from "./components/BrandCuration.tsx";
import { LeadsView } from "./components/LeadsView.tsx";
import { RetailersView } from "./components/RetailersView.tsx";

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
    <div className="min-h-full bg-neutral-950 text-neutral-100">
      <header className="border-b border-neutral-800 px-6 py-4">
        <h1 className="text-lg font-semibold tracking-tight">Clothing Pipeline</h1>
        <p className="text-sm text-neutral-400">Unified control panel</p>
        <nav className="mt-3 flex gap-1">
          {TABS.map((t) => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition ${
                tab === t.key
                  ? "bg-neutral-800 text-neutral-100"
                  : "text-neutral-400 hover:text-neutral-200"
              }`}
            >
              {t.label}
            </button>
          ))}
        </nav>
      </header>

      <main className="mx-auto max-w-7xl p-6">
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
