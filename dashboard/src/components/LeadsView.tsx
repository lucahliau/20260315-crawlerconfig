import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type VendorLead } from "../api.ts";
import {
  Button,
  Card,
  EmptyState,
  ErrorBanner,
  ExternalIcon,
  StatusDot,
  TierLabel,
} from "./ui.tsx";

/** Best-guess homepage URL from a brand name. Almost always needs the user to fix it. */
function guessUrl(name: string): string {
  const slug = name
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "");
  return slug ? `https://www.${slug}.com` : "";
}

function searchUrl(name: string): string {
  return `https://www.google.com/search?q=${encodeURIComponent(name + " official store")}`;
}

type AddState = "idle" | "adding" | "added";

/**
 * Mined stockist leads (brand names only, from /api/brand-leads). For each, the
 * user confirms/edits a homepage URL then promotes it into the master list as a
 * candidate via POST /api/brands/add. `onAdded` lets the parent refresh brands.
 */
export function LeadsView({ onAdded }: { onAdded?: () => void }) {
  const [leads, setLeads] = useState<VendorLead[]>([]);
  const [generatedAt, setGeneratedAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [urls, setUrls] = useState<Record<string, string>>({});
  const [addState, setAddState] = useState<Record<string, AddState>>({});

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await api.getBrandLeads();
      setLeads(r.leads);
      setGeneratedAt(r.generatedAt);
      // seed each editable URL field with a best guess
      setUrls((prev) => {
        const next = { ...prev };
        for (const l of r.leads) if (!(l.name in next)) next[l.name] = guessUrl(l.name);
        return next;
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function add(lead: VendorLead) {
    const url = (urls[lead.name] ?? "").trim();
    if (!url) return;
    setAddState((s) => ({ ...s, [lead.name]: "adding" }));
    try {
      await api.addBrand(url, lead.name);
      setAddState((s) => ({ ...s, [lead.name]: "added" }));
      onAdded?.();
    } catch (e: unknown) {
      setAddState((s) => ({ ...s, [lead.name]: "idle" }));
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  const sorted = useMemo(
    () =>
      [...leads].sort((a, b) => {
        if (b.stockists.length !== a.stockists.length) return b.stockists.length - a.stockists.length;
        return (a.tier === "accessible" ? 0 : 1) - (b.tier === "accessible" ? 0 : 1);
      }),
    [leads],
  );

  return (
    <section className="space-y-6">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">Stockist leads</h1>
          <p className="mt-1 text-sm text-gray-500">
            Brand names mined from cool boutiques. Confirm a homepage URL, then add the good ones.
            {generatedAt && (
              <span className="ml-1 text-gray-400">
                Mined {new Date(generatedAt).toLocaleString()}.
              </span>
            )}
          </p>
        </div>
        <Button size="sm" onClick={() => void load()}>
          Refresh
        </Button>
      </header>

      {error && <ErrorBanner onDismiss={() => setError(null)}>{error}</ErrorBanner>}

      {loading ? (
        <p className="text-sm text-gray-500">Loading leads…</p>
      ) : sorted.length === 0 ? (
        <EmptyState>No leads yet. Run “Mine stockists” from the Brands tab to generate some.</EmptyState>
      ) : (
        <Card>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50 text-left text-[11px] font-medium uppercase tracking-wide text-gray-500">
                <th className="whitespace-nowrap px-4 py-2.5">Brand</th>
                <th className="whitespace-nowrap px-4 py-2.5">Price tier</th>
                <th className="whitespace-nowrap px-4 py-2.5">Carried by</th>
                <th className="whitespace-nowrap px-4 py-2.5">Homepage URL</th>
                <th className="px-4 py-2.5"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {sorted.map((lead) => {
                const state = addState[lead.name] ?? "idle";
                return (
                  <tr
                    key={lead.name}
                    className={`transition-colors hover:bg-gray-50 ${state === "added" ? "opacity-50" : ""}`}
                  >
                    <td className="px-4 py-2.5">
                      <div className="font-medium text-gray-900">{lead.name}</div>
                      <a
                        href={searchUrl(lead.name)}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-xs text-gray-400 hover:text-gray-700 hover:underline"
                      >
                        Find site
                        <ExternalIcon className="size-3" />
                      </a>
                    </td>
                    <td className="px-4 py-2.5">
                      <TierLabel tier={lead.tier} usd={lead.usd} />
                    </td>
                    <td className="tnum px-4 py-2.5 text-xs text-gray-500">
                      {lead.stockists.length} shop{lead.stockists.length === 1 ? "" : "s"}
                    </td>
                    <td className="px-4 py-2.5">
                      <input
                        value={urls[lead.name] ?? ""}
                        onChange={(e) => setUrls((u) => ({ ...u, [lead.name]: e.target.value }))}
                        disabled={state !== "idle"}
                        spellCheck={false}
                        className="h-7 w-full min-w-[220px] rounded-md border border-gray-300 bg-white px-2 text-xs text-gray-900 focus:border-gray-400 focus:outline-none disabled:bg-gray-50 disabled:text-gray-400"
                      />
                    </td>
                    <td className="px-4 py-2.5 text-right">
                      {state === "added" ? (
                        <StatusDot tone="ok" label="Added" />
                      ) : (
                        <Button
                          size="sm"
                          variant="primary"
                          onClick={() => void add(lead)}
                          disabled={state === "adding" || !(urls[lead.name] ?? "").trim()}
                        >
                          {state === "adding" ? "Adding…" : "Add"}
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </Card>
      )}
    </section>
  );
}
