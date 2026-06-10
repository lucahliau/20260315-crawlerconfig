import { useCallback, useEffect, useRef, useState } from "react";
import { api, type BrandStatus, type SwipeBrand } from "../../api.ts";
import { SwipeCard } from "./SwipeCard.tsx";
import { MobileDiscover } from "./MobileDiscover.tsx";
import { cx } from "../ui.tsx";

/**
 * Mobile companion for the Discover stage, served at /swipe. Two tabs:
 *
 *   Review — one candidate brand per screen with real imagery from its site.
 *   Approve/reject via the buttons ONLY; swiping the card just defers it to
 *   the back of the deck ("come back to it later"). Decisions go through the
 *   same POST /api/brands/status as the desktop, so approving here also fires
 *   the Shopify auto-explore hook.
 *
 *   Discover — kick off discovery runs / stockist mining / price checks from
 *   the phone; the queue refreshes when they finish.
 */

interface Decision {
  brand: SwipeBrand;
  status: BrandStatus;
}

export function SwipeView() {
  const [tab, setTab] = useState<"review" | "discover">("review");
  const [deck, setDeck] = useState<SwipeBrand[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastDecision, setLastDecision] = useState<Decision | null>(null);
  // URLs decided this session — excluded from refills until the server catches up.
  const decidedRef = useRef(new Set<string>());
  const fetchingRef = useRef(false);

  const refill = useCallback(async () => {
    if (fetchingRef.current) return;
    fetchingRef.current = true;
    try {
      const r = await api.getSwipeQueue(15);
      setTotal(r.total);
      setDeck((cur) => {
        const have = new Set(cur.map((b) => b.url));
        const fresh = r.brands.filter((b) => !have.has(b.url) && !decidedRef.current.has(b.url));
        return [...cur, ...fresh];
      });
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      fetchingRef.current = false;
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refill();
  }, [refill]);

  // Prefetch the next two cards' images so the stack never shows blanks.
  useEffect(() => {
    for (const b of deck.slice(1, 3)) {
      for (const img of (b.preview?.images ?? []).slice(0, 3)) {
        new Image().src = img.src;
      }
      if (b.preview?.hero) new Image().src = b.preview.hero;
    }
  }, [deck]);

  const decide = useCallback(
    (brand: SwipeBrand, status: BrandStatus) => {
      decidedRef.current.add(brand.url);
      setDeck((cur) => cur.filter((b) => b.url !== brand.url));
      setLastDecision({ brand, status });
      api.setBrandStatus(brand.url, status).catch((e: unknown) => {
        // Roll back: put the card on top again and surface the failure.
        decidedRef.current.delete(brand.url);
        setDeck((cur) => [brand, ...cur]);
        setLastDecision(null);
        setError(e instanceof Error ? e.message : String(e));
      });
      setDeck((cur) => {
        if (cur.length < 5) void refill();
        return cur;
      });
    },
    [refill],
  );

  const undo = useCallback(() => {
    const d = lastDecision;
    if (!d) return;
    setLastDecision(null);
    decidedRef.current.delete(d.brand.url);
    setDeck((cur) => [d.brand, ...cur]);
    api.setBrandStatus(d.brand.url, "candidate").catch((e: unknown) => {
      setError(e instanceof Error ? e.message : String(e));
    });
  }, [lastDecision]);

  /** Swipe = defer: move the top card to the back of the deck, undecided. */
  const skip = useCallback((brand: SwipeBrand) => {
    setDeck((cur) => {
      const rest = cur.filter((b) => b.url !== brand.url);
      return [...rest, brand];
    });
  }, []);

  return (
    <div
      className="flex h-dvh flex-col bg-gray-950 text-gray-100"
      style={{
        paddingTop: "env(safe-area-inset-top)",
        paddingBottom: "env(safe-area-inset-bottom)",
      }}
    >
      <header className="flex items-center justify-between px-5 py-3">
        <div className="flex items-center gap-2">
          <span className="flex size-6 items-center justify-center rounded-md bg-white text-[11px] font-semibold text-gray-900">
            CP
          </span>
          <span className="text-sm font-semibold tracking-tight">Brand swipe</span>
        </div>
        <span className="tnum text-xs text-gray-500">
          {total !== null ? `${total} to review` : ""}
        </span>
      </header>

      {error && (
        <div className="mx-5 mb-2 flex items-start justify-between gap-2 rounded-lg border border-red-900 bg-red-950/60 px-3 py-2 text-xs text-red-300">
          <span className="min-w-0 break-words">{error}</span>
          <button onClick={() => setError(null)} className="shrink-0 text-red-400">
            ✕
          </button>
        </div>
      )}

      {tab === "review" ? (
        <main className="relative flex-1 px-4 pb-3">
          {loading ? (
            <CenterNote>Loading brands…</CenterNote>
          ) : deck.length === 0 ? (
            <CenterNote>
              Queue clear.
              <br />
              <span className="text-gray-500">Find more in the Discover tab below.</span>
            </CenterNote>
          ) : (
            // Top card last in DOM so it stacks above the peeking cards.
            deck
              .slice(0, 3)
              .map((b, i) => (
                <SwipeCard
                  key={b.url}
                  brand={b}
                  depth={i}
                  onDecide={(status) => decide(b, status)}
                  onSkip={() => skip(b)}
                />
              ))
              .reverse()
          )}
        </main>
      ) : (
        <main className="min-h-0 flex-1">
          <MobileDiscover onChanged={() => void refill()} />
        </main>
      )}

      {tab === "review" && lastDecision && (
        <div className="pointer-events-none absolute inset-x-0 bottom-24 z-30 flex justify-center">
          <button
            onClick={undo}
            className="pointer-events-auto rounded-full border border-gray-700 bg-gray-900/95 px-4 py-2 text-xs font-medium text-gray-200 shadow-lg backdrop-blur"
          >
            {lastDecision.status === "approved" ? "Approved" : "Rejected"} {lastDecision.brand.name}
            <span className="ml-2 text-gray-400 underline underline-offset-2">Undo</span>
          </button>
        </div>
      )}

      <nav className="flex shrink-0 border-t border-gray-800 bg-gray-950">
        {(
          [
            { key: "review", label: "Review", icon: "▤" },
            { key: "discover", label: "Discover", icon: "✦" },
          ] as const
        ).map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cx(
              "flex h-14 flex-1 flex-col items-center justify-center gap-0.5 text-[11px] font-medium",
              tab === t.key ? "text-white" : "text-gray-500",
            )}
          >
            <span className="text-base leading-none">{t.icon}</span>
            {t.label}
          </button>
        ))}
      </nav>
    </div>
  );
}

function CenterNote({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-full items-center justify-center text-center text-sm text-gray-400">
      <p>{children}</p>
    </div>
  );
}
