import { useCallback, useEffect, useState, type ReactNode } from "react";
import { api, type AnalyticsRange, type AnalyticsSummary } from "../api.ts";
import { Card, ErrorBanner, Segmented, StatusDot } from "./ui.tsx";

/**
 * App usage analytics. The iOS app emits a few events the database can't
 * otherwise reconstruct (sessions, screen views, item views, searches); the
 * funnel and content/brand performance are derived from the domain tables
 * (User / Swipe / Collection / ClothingItem). All of it is read from the shared
 * Supabase here — nothing extra to host, no third-party analytics SDK.
 */
export function AnalyticsView() {
  const [range, setRange] = useState<AnalyticsRange>("30d");
  const [data, setData] = useState<AnalyticsSummary | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setData(await api.getAnalytics(range));
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [range]);

  useEffect(() => {
    void load();
    const t = setInterval(() => void load(), 30000);
    return () => clearInterval(t);
  }, [load]);

  if (!data && !error) return <p className="text-sm text-gray-500">Loading…</p>;

  const eng = data?.engagement;
  const ret = data?.retention;
  const fn = data?.funnel;
  const ft = data?.features;
  const ct = data?.content;
  const totalSwipes = (ft?.swipeActions ?? []).reduce((s, a) => s + a.count, 0);

  return (
    <section className="space-y-6">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">App analytics</h1>
          <p className="mt-1 max-w-2xl text-sm text-gray-500">
            How people use the iOS app — read live from the shared database. Active-user counts are
            rolling windows; everything else respects the selected range.
          </p>
        </div>
        <Segmented
          items={[
            { key: "7d", label: "7 days" },
            { key: "30d", label: "30 days" },
            { key: "all", label: "All time" },
          ]}
          active={range}
          onChange={(k) => setRange(k as AnalyticsRange)}
        />
      </header>

      {error && <ErrorBanner>{error}</ErrorBanner>}

      {/* Engagement & retention -------------------------------------------- */}
      <div>
        <SectionTitle>Engagement &amp; retention</SectionTitle>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard title="Active users" value={fmt(eng?.dau)} sub={`today · ${fmt(eng?.wau)} this week · ${fmt(eng?.mau)} this month`} />
          <StatCard title="Sessions" value={fmt(eng?.sessions)} sub={`${fmt(eng?.events)} events · ${fmt(eng?.identifiedSessions)} signed-in`} />
          <StatCard title="Median session" value={ms(eng?.medianSessionMs)} sub={`avg ${ms(eng?.avgSessionMs)}`} />
          <StatCard
            title="Day-1 / Day-7 retention"
            value={`${pct(ret?.d1Returned, ret?.d1Eligible)}% / ${pct(ret?.d7Returned, ret?.d7Eligible)}%`}
            sub={`cohort ${fmt(ret?.cohortSize)} · ${fmt(ret?.d1Eligible)}/${fmt(ret?.d7Eligible)} eligible`}
          />
        </div>
      </div>

      {/* Activation funnel ------------------------------------------------- */}
      <div>
        <SectionTitle>Activation funnel</SectionTitle>
        <Card className="p-4">
          <p className="mb-3 text-xs text-gray-400">
            New users who registered in this range, and how far they progressed (from the User,
            Swipe and Collection tables).
          </p>
          <div className="space-y-2">
            <FunnelBar label="Registered" value={fn?.registered ?? 0} base={fn?.registered ?? 0} />
            <FunnelBar label="Completed onboarding" value={fn?.onboarded ?? 0} base={fn?.registered ?? 0} />
            <FunnelBar label="Made a first swipe" value={fn?.swiped ?? 0} base={fn?.registered ?? 0} />
            <FunnelBar label="Saved to a collection" value={fn?.collected ?? 0} base={fn?.registered ?? 0} />
          </div>
        </Card>
      </div>

      {/* Feature usage ----------------------------------------------------- */}
      <div>
        <SectionTitle>Feature usage</SectionTitle>
        <div className="grid gap-4 lg:grid-cols-3">
          <Card className="p-4">
            <CardLabel>Screens opened</CardLabel>
            <BarList rows={(ft?.screens ?? []).map((s) => ({ label: s.screen, value: s.count }))} empty="No screen views yet" />
          </Card>
          <Card className="p-4">
            <CardLabel>Swipe decisions</CardLabel>
            <BarList rows={(ft?.swipeActions ?? []).map((s) => ({ label: s.action, value: s.count }))} total={totalSwipes} empty="No swipes yet" />
          </Card>
          <Card className="p-4">
            <CardLabel>Discovery & social</CardLabel>
            <dl className="mt-1 space-y-1.5 text-sm">
              <MiniStat label="Searches" value={fmt(ft?.searches)} />
              <MiniStat label="Zero-result searches" value={`${pct(ft?.zeroResultSearches, ft?.searches)}%`} warn={(ft?.zeroResultSearches ?? 0) > 0} />
              <MiniStat label="Item views" value={`${fmt(ft?.itemViews)} · ${fmt(ft?.uniqueItemsViewed)} unique`} />
              <MiniStat label="Follows" value={fmt(ft?.follows)} />
              <MiniStat label="Messages sent" value={fmt(ft?.messages)} />
            </dl>
          </Card>
        </div>
      </div>

      {/* Content & brand performance --------------------------------------- */}
      <div>
        <SectionTitle>Content &amp; brand performance</SectionTitle>
        <div className="grid gap-4 lg:grid-cols-3">
          <Card className="p-4">
            <CardLabel>Most loved items</CardLabel>
            <ItemList rows={ct?.topLoved ?? []} empty="No loves yet" />
          </Card>
          <Card className="p-4">
            <CardLabel>Most viewed items</CardLabel>
            <ItemList rows={ct?.topViewed ?? []} empty="No item views yet" />
          </Card>
          <Card className="p-4">
            <CardLabel>Top brands (by loves)</CardLabel>
            <BarList rows={(ct?.topBrands ?? []).map((b) => ({ label: b.brand, value: b.n }))} empty="No loves yet" />
          </Card>
        </div>
      </div>

      {data && (
        <p className="text-xs text-gray-400">
          Updated {new Date(data.checkedAt).toLocaleTimeString()} · refreshes every 30s
        </p>
      )}
    </section>
  );
}

// --- helpers ---------------------------------------------------------------

const fmt = (n?: number) => (n ?? 0).toLocaleString();
const pct = (n?: number, d?: number) => (d && d > 0 ? Math.round(((n ?? 0) / d) * 100) : 0);
const ms = (n?: number) => {
  const s = Math.round((n ?? 0) / 1000);
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m ${s % 60}s`;
};

function SectionTitle({ children }: { children: ReactNode }) {
  return <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">{children}</h2>;
}

function CardLabel({ children }: { children: ReactNode }) {
  return <p className="mb-2 text-[11px] font-medium uppercase tracking-wide text-gray-500">{children}</p>;
}

function StatCard({ title, value, sub }: { title: string; value: string; sub: string }) {
  return (
    <Card className="p-4">
      <p className="text-[11px] font-medium uppercase tracking-wide text-gray-500">{title}</p>
      <p className="tnum mt-1 text-xl font-semibold text-gray-900">{value}</p>
      <p className="mt-0.5 text-xs text-gray-400">{sub}</p>
    </Card>
  );
}

function MiniStat({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <dt className="text-gray-500">{label}</dt>
      <dd className={`tnum font-medium ${warn ? "text-amber-600" : "text-gray-900"}`}>{value}</dd>
    </div>
  );
}

/** A funnel step: count, % of the registered base, and a proportional bar. */
function FunnelBar({ label, value, base }: { label: string; value: number; base: number }) {
  const p = pct(value, base);
  return (
    <div>
      <div className="flex items-baseline justify-between text-sm">
        <span className="text-gray-700">{label}</span>
        <span className="tnum text-gray-500">
          {value.toLocaleString()} <span className="text-gray-400">· {p}%</span>
        </span>
      </div>
      <div className="mt-1 h-2 overflow-hidden rounded-full bg-gray-100">
        <div className="h-full rounded-full bg-gray-900" style={{ width: `${p}%` }} />
      </div>
    </div>
  );
}

/** Ranked label+value rows with proportional bars (relative to total or max). */
function BarList({
  rows,
  total,
  empty,
}: {
  rows: { label: string; value: number }[];
  total?: number;
  empty: string;
}) {
  if (rows.length === 0) return <p className="py-2 text-xs text-gray-400">{empty}</p>;
  const denom = total ?? Math.max(...rows.map((r) => r.value), 1);
  return (
    <div className="space-y-1.5">
      {rows.map((r) => (
        <div key={r.label}>
          <div className="flex items-baseline justify-between text-sm">
            <span className="truncate text-gray-700">{r.label}</span>
            <span className="tnum ml-2 shrink-0 text-gray-500">
              {r.value.toLocaleString()}
              {total ? <span className="text-gray-400"> · {pct(r.value, total)}%</span> : null}
            </span>
          </div>
          <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-gray-100">
            <div className="h-full rounded-full bg-gray-900" style={{ width: `${pct(r.value, denom)}%` }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function ItemList({ rows, empty }: { rows: { name: string; brand: string; n: number }[]; empty: string }) {
  if (rows.length === 0) return <p className="py-2 text-xs text-gray-400">{empty}</p>;
  return (
    <ol className="space-y-1.5 text-sm">
      {rows.map((r, i) => (
        <li key={`${r.brand}-${r.name}-${i}`} className="flex items-center justify-between gap-3">
          <span className="min-w-0">
            <span className="tnum mr-2 text-gray-400">{i + 1}</span>
            <span className="text-gray-800">{r.name}</span>
            <span className="ml-1 text-gray-400">· {r.brand}</span>
          </span>
          <StatusDot tone="info" label={String(r.n)} muted />
        </li>
      ))}
    </ol>
  );
}
