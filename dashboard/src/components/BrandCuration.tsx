import { useEffect, useMemo, useState, type ReactNode } from "react";
import { api, type Brand, type BrandStatus, type PriceTier } from "../api.ts";
import { PipelineActions } from "./PipelineActions.tsx";

type StatusFilter = BrandStatus | "all";
type TierFilter = PriceTier | "all";

const STATUS_TABS: { key: StatusFilter; label: string }[] = [
  { key: "candidate", label: "To review" },
  { key: "approved", label: "Approved" },
  { key: "rejected", label: "Rejected" },
  { key: "all", label: "All" },
];

const TIER_TABS: { key: TierFilter; label: string }[] = [
  { key: "all", label: "Any price" },
  { key: "accessible", label: "Accessible" },
  { key: "too_expensive", label: "Too pricey" },
  { key: "unknown", label: "Unknown" },
];

export function BrandCuration() {
  const [brands, setBrands] = useState<Brand[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("candidate");
  const [tierFilter, setTierFilter] = useState<TierFilter>("all");
  const [busyUrl, setBusyUrl] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const r = await api.getBrands();
      setBrands(r.brands);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
  }, []);

  // Counts are computed client-side so the header and tabs update instantly on each action.
  const counts = useMemo(() => {
    const byStatus: Record<BrandStatus, number> = { candidate: 0, approved: 0, rejected: 0 };
    const byTier: Record<PriceTier, number> = {
      too_cheap: 0,
      accessible: 0,
      too_expensive: 0,
      unknown: 0,
    };
    for (const b of brands) {
      byStatus[b.effectiveStatus]++;
      byTier[b.tier]++;
    }
    return { total: brands.length, byStatus, byTier };
  }, [brands]);

  const visible = useMemo(() => {
    return brands.filter((b) => {
      if (statusFilter !== "all" && b.effectiveStatus !== statusFilter) return false;
      if (tierFilter !== "all" && b.tier !== tierFilter) return false;
      return true;
    });
  }, [brands, statusFilter, tierFilter]);

  async function setStatus(url: string, status: BrandStatus) {
    const prev = brands;
    // optimistic update
    setBrands((bs) => bs.map((b) => (b.url === url ? { ...b, status, effectiveStatus: status } : b)));
    setBusyUrl(url);
    try {
      await api.setBrandStatus(url, status);
    } catch (e: unknown) {
      setBrands(prev); // rollback
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyUrl(null);
    }
  }

  return (
    <section className="space-y-5">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold text-neutral-100">Brand curation</h2>
          <p className="text-sm text-neutral-400">
            Approve the cool, accessible ones. Shopify brands get a crawl config built
            automatically (free) and appear in <span className="text-neutral-300">Retailers</span>;
            the rest land in the Retailers backlog for a manual explore.
          </p>
        </div>
        <div className="flex gap-2 text-xs text-neutral-400">
          <Pill>{counts.byStatus.candidate} to review</Pill>
          <Pill accent="emerald">{counts.byStatus.approved} approved</Pill>
          <Pill accent="amber">{counts.byTier.too_expensive} too pricey</Pill>
        </div>
      </header>

      <PipelineActions onChanged={() => void load()} />

      <div className="flex flex-wrap items-center gap-2">
        <TabGroup
          tabs={STATUS_TABS.map((t) => ({
            ...t,
            count:
              t.key === "all" ? counts.total : counts.byStatus[t.key as BrandStatus],
          }))}
          active={statusFilter}
          onChange={(k) => setStatusFilter(k as StatusFilter)}
        />
        <div className="mx-1 h-5 w-px bg-neutral-800" />
        <TabGroup
          tabs={TIER_TABS.map((t) => ({
            ...t,
            count: t.key === "all" ? undefined : counts.byTier[t.key as PriceTier],
          }))}
          active={tierFilter}
          onChange={(k) => setTierFilter(k as TierFilter)}
        />
        <button
          onClick={() => void load()}
          className="ml-auto rounded-md border border-neutral-800 px-3 py-1.5 text-xs text-neutral-300 hover:bg-neutral-800"
        >
          Refresh
        </button>
      </div>

      {error && (
        <div className="rounded-md border border-red-900 bg-red-950/50 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-sm text-neutral-400">Loading brands…</p>
      ) : visible.length === 0 ? (
        <p className="rounded-lg border border-dashed border-neutral-800 px-4 py-10 text-center text-sm text-neutral-500">
          Nothing here. Try a different filter, or run a discovery.
        </p>
      ) : (
        <div className="overflow-hidden rounded-xl border border-neutral-800">
          <ul className="divide-y divide-neutral-800/60">
            {visible.map((b) => (
              <BrandRow key={b.url} brand={b} busy={busyUrl === b.url} onSetStatus={setStatus} />
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}

const TIER_STYLE: Record<PriceTier, { label: string; cls: string }> = {
  accessible: { label: "accessible", cls: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30" },
  too_expensive: { label: "too pricey", cls: "bg-amber-500/15 text-amber-300 border-amber-500/30" },
  too_cheap: { label: "too cheap", cls: "bg-amber-500/15 text-amber-300 border-amber-500/30" },
  unknown: { label: "unknown", cls: "bg-neutral-700/30 text-neutral-400 border-neutral-600/40" },
};

function hostOf(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

/** One compact brand row — scales to hundreds of brands where the old card grid didn't. */
function BrandRow({
  brand,
  busy,
  onSetStatus,
}: {
  brand: Brand;
  busy: boolean;
  onSetStatus: (url: string, status: BrandStatus) => void;
}) {
  const tier = TIER_STYLE[brand.tier];
  const price = brand.priceSample ? `$${Math.round(brand.priceSample.usd)}` : null;
  const status = brand.effectiveStatus;

  return (
    <li
      className={`flex items-center gap-3 px-4 py-2.5 hover:bg-neutral-900/50 ${
        status === "rejected" ? "opacity-50" : ""
      }`}
    >
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline gap-2">
          <span className="truncate font-medium text-neutral-100">{brand.name}</span>
          <a
            href={brand.url}
            target="_blank"
            rel="noreferrer"
            className="hidden truncate text-xs text-neutral-500 hover:text-neutral-300 hover:underline sm:inline"
          >
            {hostOf(brand.url)}
          </a>
        </div>
      </div>

      <div className="hidden shrink-0 items-center gap-1.5 text-[11px] text-neutral-400 md:flex">
        {brand.region && <span className="rounded bg-neutral-800 px-1.5 py-0.5">{brand.region}</span>}
        {brand.source && <span className="rounded bg-neutral-800 px-1.5 py-0.5">via {brand.source}</span>}
      </div>

      <span className={`w-28 shrink-0 rounded-full border px-2 py-0.5 text-center text-[11px] ${tier.cls}`}>
        {price ? `${price} · ` : ""}
        {tier.label}
      </span>

      <div className="flex w-44 shrink-0 items-center justify-end gap-1.5">
        {status === "candidate" ? (
          <>
            <button
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "approved")}
              className="rounded-md bg-emerald-600 px-3 py-1 text-xs font-medium text-white transition hover:bg-emerald-500 disabled:opacity-50"
            >
              Approve
            </button>
            <button
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "rejected")}
              className="rounded-md bg-neutral-800 px-3 py-1 text-xs font-medium text-neutral-200 transition hover:bg-red-600/80 disabled:opacity-50"
            >
              Reject
            </button>
          </>
        ) : (
          <>
            <span
              className={`text-xs font-medium ${status === "approved" ? "text-emerald-400" : "text-red-400"}`}
            >
              {status === "approved" ? "✓ Approved" : "✕ Rejected"}
            </span>
            <button
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "candidate")}
              className="text-xs text-neutral-500 hover:text-neutral-300 disabled:opacity-50"
            >
              Undo
            </button>
          </>
        )}
      </div>
    </li>
  );
}

function Pill({
  children,
  accent,
}: {
  children: ReactNode;
  accent?: "emerald" | "amber";
}) {
  const cls =
    accent === "emerald"
      ? "text-emerald-300 border-emerald-500/30"
      : accent === "amber"
        ? "text-amber-300 border-amber-500/30"
        : "text-neutral-300 border-neutral-700";
  return <span className={`rounded-full border px-2.5 py-1 ${cls}`}>{children}</span>;
}

function TabGroup({
  tabs,
  active,
  onChange,
}: {
  tabs: { key: string; label: string; count?: number }[];
  active: string;
  onChange: (key: string) => void;
}) {
  return (
    <div className="flex gap-1 rounded-lg bg-neutral-900 p-1">
      {tabs.map((t) => (
        <button
          key={t.key}
          onClick={() => onChange(t.key)}
          className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
            active === t.key
              ? "bg-neutral-700 text-neutral-100"
              : "text-neutral-400 hover:text-neutral-200"
          }`}
        >
          {t.label}
          {t.count !== undefined && <span className="ml-1.5 text-neutral-500">{t.count}</span>}
        </button>
      ))}
    </div>
  );
}
