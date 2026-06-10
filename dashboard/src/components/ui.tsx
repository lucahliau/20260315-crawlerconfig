import type { ButtonHTMLAttributes, ReactNode } from "react";

/**
 * Shared UI primitives — single source of truth for the dashboard's visual
 * language: light surfaces, hairline borders, near-black primary actions,
 * status communicated with small dots + plain text (never loud pills).
 */

export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

// ---------------------------------------------------------------------------
// Buttons
// ---------------------------------------------------------------------------

type ButtonVariant = "primary" | "secondary" | "ghost" | "warn";
type ButtonSize = "sm" | "md";

const BUTTON_VARIANT: Record<ButtonVariant, string> = {
  primary: "bg-gray-900 text-white hover:bg-gray-700",
  secondary: "border border-gray-300 bg-white text-gray-700 hover:bg-gray-50 hover:text-gray-900",
  ghost: "text-gray-500 hover:bg-gray-100 hover:text-gray-900",
  warn: "border border-amber-300 bg-amber-50 text-amber-900 hover:bg-amber-100",
};

const BUTTON_SIZE: Record<ButtonSize, string> = {
  sm: "h-7 px-2.5 text-xs",
  md: "h-8 px-3.5 text-[13px]",
};

export function Button({
  variant = "secondary",
  size = "md",
  className,
  children,
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
}) {
  return (
    <button
      {...rest}
      className={cx(
        "inline-flex select-none items-center justify-center gap-1.5 rounded-md font-medium transition-colors",
        "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-gray-900",
        "disabled:cursor-not-allowed disabled:opacity-45",
        BUTTON_VARIANT[variant],
        BUTTON_SIZE[size],
        className,
      )}
    >
      {children}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

export type Tone = "ok" | "running" | "failed" | "warn" | "idle" | "info";

const DOT_TONE: Record<Tone, string> = {
  ok: "bg-emerald-500",
  running: "bg-blue-500 animate-pulse",
  failed: "bg-red-500",
  warn: "bg-amber-500",
  idle: "bg-gray-300",
  info: "bg-blue-500",
};

export function StatusDot({
  tone,
  label,
  title,
  muted,
}: {
  tone: Tone;
  label: ReactNode;
  title?: string;
  muted?: boolean;
}) {
  return (
    <span
      title={title}
      className={cx(
        "inline-flex items-center gap-1.5 whitespace-nowrap text-xs",
        muted ? "text-gray-400" : "text-gray-700",
      )}
    >
      <span className={cx("size-1.5 shrink-0 rounded-full", DOT_TONE[tone])} />
      {label}
    </span>
  );
}

export function Spinner({ className }: { className?: string }) {
  return (
    <svg
      className={cx("size-3.5 animate-spin text-gray-400", className)}
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden
    >
      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" opacity="0.25" />
      <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Icons (minimal inline set — keeps the bundle dependency-free)
// ---------------------------------------------------------------------------

function Icon({ d, className }: { d: string; className?: string }) {
  return (
    <svg
      className={cx("size-3.5 shrink-0", className)}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      <path d={d} />
    </svg>
  );
}

export const PlayIcon = ({ className }: { className?: string }) => (
  <svg className={cx("size-3.5 shrink-0", className)} viewBox="0 0 24 24" fill="currentColor" aria-hidden>
    <path d="M8 5.14v13.72a1 1 0 0 0 1.52.86l11-6.86a1 1 0 0 0 0-1.72l-11-6.86A1 1 0 0 0 8 5.14Z" />
  </svg>
);
export const SearchIcon = ({ className }: { className?: string }) => (
  <Icon className={className} d="M21 21l-4.35-4.35M17 10.5a6.5 6.5 0 1 1-13 0 6.5 6.5 0 0 1 13 0Z" />
);
export const RetryIcon = ({ className }: { className?: string }) => (
  <Icon className={className} d="M3 12a9 9 0 1 0 3-6.7M3 4v5h5" />
);
export const CheckIcon = ({ className }: { className?: string }) => (
  <Icon className={className} d="M20 6 9 17l-5-5" />
);
export const XIcon = ({ className }: { className?: string }) => (
  <Icon className={className} d="M18 6 6 18M6 6l12 12" />
);
export const ChevronIcon = ({ open, className }: { open: boolean; className?: string }) => (
  <Icon className={cx(className, "transition-transform", open && "rotate-180")} d="m6 9 6 6 6-6" />
);
export const ExternalIcon = ({ className }: { className?: string }) => (
  <Icon className={className} d="M14 4h6v6m0-6L10 14M9 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-3" />
);

// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

export function ErrorBanner({
  children,
  onDismiss,
}: {
  children: ReactNode;
  onDismiss?: () => void;
}) {
  return (
    <div className="flex items-start justify-between gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      <span className="min-w-0 break-words">{children}</span>
      {onDismiss && (
        <button onClick={onDismiss} className="text-red-400 hover:text-red-600" aria-label="Dismiss">
          <XIcon />
        </button>
      )}
    </div>
  );
}

export function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-lg border border-dashed border-gray-300 bg-white px-6 py-14 text-center text-sm text-gray-500">
      {children}
    </div>
  );
}

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cx("overflow-hidden rounded-lg border border-gray-200 bg-white", className)}>
      {children}
    </div>
  );
}

/** Underlined tab strip used for both top-level nav and in-view filters. */
export function Segmented({
  items,
  active,
  onChange,
}: {
  items: { key: string; label: string; count?: number }[];
  active: string;
  onChange: (key: string) => void;
}) {
  return (
    <div className="inline-flex items-center gap-0.5 rounded-lg border border-gray-200 bg-gray-50 p-0.5">
      {items.map((it) => (
        <button
          key={it.key}
          onClick={() => onChange(it.key)}
          className={cx(
            "inline-flex h-7 items-center gap-1.5 rounded-md px-2.5 text-xs font-medium transition-colors",
            active === it.key
              ? "bg-white text-gray-900 shadow-sm ring-1 ring-gray-200"
              : "text-gray-500 hover:text-gray-800",
          )}
        >
          {it.label}
          {it.count !== undefined && (
            <span className={cx("tnum", active === it.key ? "text-gray-400" : "text-gray-400")}>
              {it.count}
            </span>
          )}
        </button>
      ))}
    </div>
  );
}

/** Price-tier display shared by Brands and Leads. */
export function TierLabel({ tier, usd }: { tier: string; usd?: number | null }) {
  const map: Record<string, { tone: Tone; label: string }> = {
    accessible: { tone: "ok", label: "Accessible" },
    too_expensive: { tone: "warn", label: "Too pricey" },
    too_cheap: { tone: "warn", label: "Too cheap" },
    unknown: { tone: "idle", label: "Unknown" },
  };
  const t = map[tier] ?? map.unknown;
  return (
    <StatusDot
      tone={t.tone}
      muted={tier === "unknown"}
      label={
        usd && usd > 0 ? (
          <span className="tnum">
            ${Math.round(usd)} <span className="text-gray-400">· {t.label}</span>
          </span>
        ) : (
          t.label
        )
      }
    />
  );
}
