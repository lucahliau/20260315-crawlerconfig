import { useRef, useState } from "react";
import type { BrandStatus, PriceTier, SwipeBrand } from "../../api.ts";
import { cx } from "../ui.tsx";

/**
 * One brand card in the swipe stack. Approve/reject are BUTTON-ONLY —
 * swiping the card (either direction) just skips it for later: it animates
 * away and rejoins the back of the deck undecided, so a commute session can
 * defer the hard calls without losing them.
 */

const SKIP_RATIO = 0.35;

const TIER_DARK: Record<PriceTier, { label: string; cls: string }> = {
  accessible: { label: "Accessible", cls: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30" },
  too_expensive: { label: "Too pricey", cls: "bg-amber-500/15 text-amber-300 border-amber-500/30" },
  too_cheap: { label: "Too cheap", cls: "bg-amber-500/15 text-amber-300 border-amber-500/30" },
  unknown: { label: "Price unknown", cls: "bg-gray-700/40 text-gray-300 border-gray-600/40" },
};

function hostOf(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

export function SwipeCard({
  brand,
  depth,
  onDecide,
  onSkip,
}: {
  brand: SwipeBrand;
  depth: number;
  onDecide: (status: BrandStatus) => void;
  onSkip: () => void;
}) {
  const cardRef = useRef<HTMLDivElement | null>(null);
  const startRef = useRef<{ x: number; y: number; id: number } | null>(null);
  const [dx, setDx] = useState(0);
  const [dragging, setDragging] = useState(false);
  const [exiting, setExiting] = useState<0 | 1 | -1>(0);
  const [failedImgs, setFailedImgs] = useState<Set<string>>(new Set());

  /** Buttons decide; the exit animation direction just matches the verdict. */
  const decide = (status: BrandStatus) => {
    setExiting(status === "approved" ? 1 : -1);
    setDragging(false);
    setTimeout(() => onDecide(status), 220);
  };

  /** A swipe (either direction) defers the brand — no API call, back of the deck. */
  const skip = (dir: 1 | -1) => {
    setExiting(dir);
    setDragging(false);
    setTimeout(() => onSkip(), 220);
  };

  const onPointerDown = (e: React.PointerEvent) => {
    if (depth !== 0 || exiting !== 0) return;
    // Let links and buttons work without starting a drag.
    if ((e.target as HTMLElement).closest("a,button")) return;
    startRef.current = { x: e.clientX, y: e.clientY, id: e.pointerId };
    setDragging(true);
    cardRef.current?.setPointerCapture(e.pointerId);
  };
  const onPointerMove = (e: React.PointerEvent) => {
    if (!startRef.current || e.pointerId !== startRef.current.id) return;
    setDx(e.clientX - startRef.current.x);
  };
  const onPointerUp = (e: React.PointerEvent) => {
    if (!startRef.current || e.pointerId !== startRef.current.id) return;
    const width = cardRef.current?.offsetWidth ?? 320;
    const final = e.clientX - startRef.current.x;
    startRef.current = null;
    if (Math.abs(final) > width * SKIP_RATIO) {
      skip(final > 0 ? 1 : -1);
    } else {
      setDragging(false);
      setDx(0);
    }
  };

  const tier = TIER_DARK[brand.tier] ?? TIER_DARK.unknown;
  const price = brand.priceSample ? `$${Math.round(brand.priceSample.usd)}` : null;
  const images = (brand.preview?.images ?? []).filter((i) => !failedImgs.has(i.src));
  const hero =
    brand.preview?.hero && !failedImgs.has(brand.preview.hero)
      ? brand.preview.hero
      : images[0]?.src ?? null;
  const thumbs = images.filter((i) => i.src !== hero).slice(0, 3);
  const markFailed = (src: string) => setFailedImgs((s) => new Set(s).add(src));

  const exitX = exiting * (typeof window !== "undefined" ? window.innerWidth : 600) * 1.2;
  const x = exiting !== 0 ? exitX : dx;

  return (
    <div
      ref={cardRef}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerCancel={onPointerUp}
      className={cx(
        "absolute inset-0 flex touch-none flex-col overflow-hidden rounded-2xl border border-gray-800 bg-gray-900 select-none",
        depth > 0 && "pointer-events-none",
        !dragging && "transition-transform duration-200 ease-out",
      )}
      style={{
        transform:
          depth === 0
            ? `translateX(${x}px) rotate(${x * 0.04}deg)`
            : `scale(${1 - depth * 0.04}) translateY(${depth * 10}px)`,
        zIndex: 10 - depth,
        opacity: exiting !== 0 ? 0 : 1,
        transitionProperty: dragging ? "none" : "transform, opacity",
      }}
    >
      {/* Drag overlay — swiping defers, it never decides */}
      <div
        className="pointer-events-none absolute inset-x-0 top-5 z-20 flex justify-center"
        style={{ opacity: Math.max(0, Math.min(1, (Math.abs(dx) - 30) / 70)) }}
      >
        <span className="rounded-full border border-gray-500 bg-gray-950/70 px-3 py-1 text-sm font-semibold tracking-wider text-gray-200 backdrop-blur">
          LATER ↺
        </span>
      </div>

      {/* Imagery */}
      <div className="relative min-h-0 flex-1 bg-gray-800">
        {hero ? (
          <img
            src={hero}
            alt=""
            draggable={false}
            onError={() => markFailed(hero)}
            className="h-full w-full object-cover"
          />
        ) : (
          <div className="flex h-full flex-col items-center justify-center gap-2 text-gray-500">
            <span className="flex size-14 items-center justify-center rounded-full bg-gray-700 text-xl font-semibold text-gray-300">
              {brand.name.slice(0, 1).toUpperCase()}
            </span>
            <span className="text-xs">No preview — open the site to judge</span>
          </div>
        )}
        <div className="pointer-events-none absolute inset-x-0 bottom-0 h-28 bg-gradient-to-t from-gray-950/90 to-transparent" />
        <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-2 p-4">
          <div className="min-w-0">
            <h2 className="truncate text-xl font-semibold text-white">{brand.name}</h2>
            <p className="mt-0.5 truncate text-xs text-gray-300">
              {hostOf(brand.url)}
              {brand.region ? ` · ${brand.region}` : ""}
              {brand.source ? ` · via ${brand.source}` : ""}
            </p>
          </div>
          <span
            className={cx(
              "shrink-0 rounded-full border px-2.5 py-1 text-[11px] font-medium whitespace-nowrap",
              tier.cls,
            )}
          >
            {price ? `${price} · ` : ""}
            {tier.label}
          </span>
        </div>
      </div>

      {/* Product thumbnails */}
      {thumbs.length > 0 && (
        <div className="grid shrink-0 grid-cols-3 gap-px bg-gray-800">
          {thumbs.map((t) => (
            <div key={t.src} className="relative aspect-square overflow-hidden bg-gray-900">
              <img
                src={t.src}
                alt={t.title ?? ""}
                title={t.title}
                draggable={false}
                onError={() => markFailed(t.src)}
                className="h-full w-full object-cover"
              />
            </div>
          ))}
        </div>
      )}

      {/* Actions */}
      <div className="flex shrink-0 items-center justify-center gap-5 px-4 py-3.5">
        <button
          onClick={() => decide("rejected")}
          aria-label="Reject"
          className="flex size-14 items-center justify-center rounded-full border border-gray-700 bg-gray-800 text-xl text-red-400 active:scale-95"
        >
          ✕
        </button>
        <a
          href={brand.url}
          target="_blank"
          rel="noreferrer"
          aria-label="Open site"
          className="flex h-11 items-center justify-center rounded-full border border-gray-700 bg-gray-800 px-4 text-sm font-medium text-gray-200 active:scale-95"
        >
          Open site ↗
        </a>
        <button
          onClick={() => decide("approved")}
          aria-label="Approve"
          className="flex size-14 items-center justify-center rounded-full bg-emerald-500 text-xl text-white active:scale-95"
        >
          ✓
        </button>
      </div>
    </div>
  );
}
