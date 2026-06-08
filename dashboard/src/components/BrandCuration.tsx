import { useEffect, useMemo, useState, type ReactNode } from "react";
import { api, type Brand, type BrandStatus, type PriceTier } from "../api.ts";
import { BrandCard } from "./BrandCard.tsx";
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
            Approve the cool, accessible ones before they cost any crawl budget.
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
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {visible.map((b) => (
            <BrandCard
              key={b.url}
              brand={b}
              busy={busyUrl === b.url}
              onSetStatus={setStatus}
            />
          ))}
        </div>
      )}
    </section>
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
