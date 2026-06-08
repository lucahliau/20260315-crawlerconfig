import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type PriceTier, type VendorLead } from "../api.ts";

const TIER_STYLE: Record<PriceTier, { label: string; cls: string }> = {
  accessible: { label: "accessible", cls: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30" },
  too_expensive: { label: "too pricey", cls: "bg-amber-500/15 text-amber-300 border-amber-500/30" },
  too_cheap: { label: "too cheap", cls: "bg-amber-500/15 text-amber-300 border-amber-500/30" },
  unknown: { label: "unknown", cls: "bg-neutral-700/30 text-neutral-400 border-neutral-600/40" },
};

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
    <section className="space-y-4">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold text-neutral-100">Stockist leads</h2>
          <p className="text-sm text-neutral-400">
            Brand names mined from cool boutiques. Confirm a homepage URL, then add the good ones.
            {generatedAt && (
              <span className="ml-1 text-neutral-600">
                · mined {new Date(generatedAt).toLocaleString()}
              </span>
            )}
          </p>
        </div>
        <button
          onClick={() => void load()}
          className="rounded-md border border-neutral-800 px-3 py-1.5 text-xs text-neutral-300 hover:bg-neutral-800"
        >
          Refresh
        </button>
      </header>

      {error && (
        <div className="rounded-md border border-red-900 bg-red-950/50 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-sm text-neutral-400">Loading leads…</p>
      ) : sorted.length === 0 ? (
        <p className="rounded-lg border border-dashed border-neutral-800 px-4 py-10 text-center text-sm text-neutral-500">
          No leads yet. Run “Mine stockists” from the Brands tab to generate some.
        </p>
      ) : (
        <div className="overflow-hidden rounded-xl border border-neutral-800">
          <table className="w-full text-sm">
            <thead className="bg-neutral-900 text-left text-xs text-neutral-400">
              <tr>
                <th className="px-3 py-2 font-medium">Brand</th>
                <th className="px-3 py-2 font-medium">Tier</th>
                <th className="px-3 py-2 font-medium">Carried by</th>
                <th className="px-3 py-2 font-medium">Homepage URL</th>
                <th className="px-3 py-2 font-medium"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800">
              {sorted.map((lead) => {
                const tier = TIER_STYLE[lead.tier];
                const state = addState[lead.name] ?? "idle";
                return (
                  <tr key={lead.name} className={state === "added" ? "opacity-50" : ""}>
                    <td className="px-3 py-2">
                      <div className="font-medium text-neutral-100">{lead.name}</div>
                      <a
                        href={searchUrl(lead.name)}
                        target="_blank"
                        rel="noreferrer"
                        className="text-[11px] text-neutral-500 hover:text-neutral-300 hover:underline"
                      >
                        find site ↗
                      </a>
                    </td>
                    <td className="px-3 py-2">
                      <span className={`rounded-full border px-2 py-0.5 text-[11px] ${tier.cls}`}>
                        {lead.usd > 0 ? `$${lead.usd} · ` : ""}
                        {tier.label}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-xs text-neutral-400">
                      {lead.stockists.length} shop{lead.stockists.length === 1 ? "" : "s"}
                    </td>
                    <td className="px-3 py-2">
                      <input
                        value={urls[lead.name] ?? ""}
                        onChange={(e) =>
                          setUrls((u) => ({ ...u, [lead.name]: e.target.value }))
                        }
                        disabled={state !== "idle"}
                        spellCheck={false}
                        className="w-full min-w-[200px] rounded-md border border-neutral-700 bg-neutral-950 px-2 py-1 text-xs text-neutral-200 outline-none focus:border-neutral-500 disabled:opacity-50"
                      />
                    </td>
                    <td className="px-3 py-2 text-right">
                      {state === "added" ? (
                        <span className="text-xs font-medium text-emerald-400">✓ added</span>
                      ) : (
                        <button
                          onClick={() => void add(lead)}
                          disabled={state === "adding" || !(urls[lead.name] ?? "").trim()}
                          className="rounded-md bg-emerald-600 px-3 py-1 text-xs font-medium text-white transition hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-40"
                        >
                          {state === "adding" ? "Adding…" : "Add"}
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
