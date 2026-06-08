import type { ReactNode } from "react";
import type { Brand, BrandStatus, PriceTier } from "../api.ts";

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

export function BrandCard({
  brand,
  busy,
  onSetStatus,
}: {
  brand: Brand;
  busy: boolean;
  onSetStatus: (url: string, status: BrandStatus) => void;
}) {
  const tier = TIER_STYLE[brand.tier];
  const price = brand.priceSample ? `$${Math.round(brand.priceSample.usd)}` : "—";
  const status = brand.effectiveStatus;

  return (
    <div
      className={`flex flex-col gap-3 rounded-xl border bg-neutral-900/50 p-4 transition ${
        status === "rejected"
          ? "border-neutral-800 opacity-60"
          : status === "approved"
            ? "border-emerald-700/40"
            : "border-neutral-800"
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate font-medium text-neutral-100">{brand.name}</div>
          <a
            href={brand.url}
            target="_blank"
            rel="noreferrer"
            className="truncate text-xs text-neutral-500 hover:text-neutral-300 hover:underline"
          >
            {hostOf(brand.url)}
          </a>
        </div>
        <span className={`shrink-0 rounded-full border px-2 py-0.5 text-[11px] ${tier.cls}`}>
          {price} · {tier.label}
        </span>
      </div>

      <div className="flex flex-wrap gap-1.5 text-[11px] text-neutral-400">
        {brand.region && (
          <span className="rounded bg-neutral-800 px-1.5 py-0.5">{brand.region}</span>
        )}
        {brand.source && (
          <span className="rounded bg-neutral-800 px-1.5 py-0.5">via {brand.source}</span>
        )}
      </div>

      <div className="mt-auto flex items-center gap-2 pt-1">
        {status === "candidate" && (
          <>
            <ActionButton
              kind="approve"
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "approved")}
            >
              Approve
            </ActionButton>
            <ActionButton
              kind="reject"
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "rejected")}
            >
              Reject
            </ActionButton>
          </>
        )}
        {status === "approved" && (
          <>
            <span className="text-xs font-medium text-emerald-400">✓ Approved</span>
            <button
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "candidate")}
              className="ml-auto text-xs text-neutral-500 hover:text-neutral-300 disabled:opacity-50"
            >
              Undo
            </button>
          </>
        )}
        {status === "rejected" && (
          <>
            <span className="text-xs font-medium text-red-400">✕ Rejected</span>
            <button
              disabled={busy}
              onClick={() => onSetStatus(brand.url, "candidate")}
              className="ml-auto text-xs text-neutral-500 hover:text-neutral-300 disabled:opacity-50"
            >
              Undo
            </button>
          </>
        )}
      </div>
    </div>
  );
}

function ActionButton({
  kind,
  disabled,
  onClick,
  children,
}: {
  kind: "approve" | "reject";
  disabled: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  const cls =
    kind === "approve"
      ? "bg-emerald-600 hover:bg-emerald-500 text-white"
      : "bg-neutral-800 hover:bg-red-600/80 text-neutral-200";
  return (
    <button
      disabled={disabled}
      onClick={onClick}
      className={`flex-1 rounded-md px-3 py-1.5 text-sm font-medium transition disabled:opacity-50 ${cls}`}
    >
      {children}
    </button>
  );
}
