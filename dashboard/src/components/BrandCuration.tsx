import { useEffect, useMemo, useState } from "react";
import { api, type Brand, type BrandStatus, type PriceTier } from "../api.ts";
import {
  Button,
  Card,
  EmptyState,
  ErrorBanner,
  Segmented,
  StatusDot,
  TierLabel,
} from "./ui.tsx";

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
    <section className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <Segmented
          items={STATUS_TABS.map((t) => ({
            ...t,
            count: t.key === "all" ? counts.total : counts.byStatus[t.key as BrandStatus],
          }))}
          active={statusFilter}
          onChange={(k) => setStatusFilter(k as StatusFilter)}
        />
        <Segmented
          items={TIER_TABS.map((t) => ({
            ...t,
            count: t.key === "all" ? undefined : counts.byTier[t.key as PriceTier],
          }))}
          active={tierFilter}
          onChange={(k) => setTierFilter(k as TierFilter)}
        />
        <Button size="sm" className="ml-auto" onClick={() => void load()}>
          Refresh
        </Button>
      </div>

      {error && <ErrorBanner onDismiss={() => setError(null)}>{error}</ErrorBanner>}

      {loading ? (
        <p className="text-sm text-gray-500">Loading brands…</p>
      ) : visible.length === 0 ? (
        <EmptyState>Nothing here. Try a different filter, or run a discovery.</EmptyState>
      ) : (
        <Card>
          <ul className="divide-y divide-gray-100">
            {visible.map((b) => (
              <BrandRow key={b.url} brand={b} busy={busyUrl === b.url} onSetStatus={setStatus} />
            ))}
          </ul>
        </Card>
      )}
    </section>
  );
}

function hostOf(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

/** One compact brand row — scales to hundreds of brands where a card grid wouldn't. */
function BrandRow({
  brand,
  busy,
  onSetStatus,
}: {
  brand: Brand;
  busy: boolean;
  onSetStatus: (url: string, status: BrandStatus) => void;
}) {
  const status = brand.effectiveStatus;

  return (
    <li
      className={`flex items-center gap-4 px-4 py-2.5 transition-colors hover:bg-gray-50 ${
        status === "rejected" ? "opacity-50" : ""
      }`}
    >
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline gap-2">
          <span className="truncate text-sm font-medium text-gray-900">{brand.name}</span>
          <a
            href={brand.url}
            target="_blank"
            rel="noreferrer"
            className="hidden truncate text-xs text-gray-400 hover:text-gray-700 hover:underline sm:inline"
          >
            {hostOf(brand.url)}
          </a>
        </div>
      </div>

      <div className="hidden shrink-0 items-center gap-2 text-xs text-gray-400 md:flex">
        {brand.region && <span>{brand.region}</span>}
        {brand.region && brand.source && <span className="text-gray-300">·</span>}
        {brand.source && <span>via {brand.source}</span>}
      </div>

      <div className="w-36 shrink-0">
        <TierLabel tier={brand.tier} usd={brand.priceSample?.usd} />
      </div>

      <div className="flex w-44 shrink-0 items-center justify-end gap-1.5">
        {status === "candidate" ? (
          <>
            <Button
              size="sm"
              variant="primary"
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "approved")}
            >
              Approve
            </Button>
            <Button size="sm" disabled={busy} onClick={() => onSetStatus(brand.url, "rejected")}>
              Reject
            </Button>
          </>
        ) : (
          <>
            <StatusDot
              tone={status === "approved" ? "ok" : "idle"}
              label={status === "approved" ? "Approved" : "Rejected"}
            />
            <Button
              size="sm"
              variant="ghost"
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "candidate")}
            >
              Undo
            </Button>
          </>
        )}
      </div>
    </li>
  );
}
