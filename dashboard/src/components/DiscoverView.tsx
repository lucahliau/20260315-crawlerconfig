import { useState } from "react";
import { BrandCuration } from "./BrandCuration.tsx";
import { LeadsView } from "./LeadsView.tsx";
import { PipelineActions } from "./PipelineActions.tsx";
import { Segmented } from "./ui.tsx";

/**
 * Stage 1 — Discover. Everything that produces and curates brand candidates
 * lives here: the research tools (AI discovery, stockist mining, price checks),
 * the review queue of found brands, and the mined leads waiting for a homepage
 * URL. Approving a brand moves it into the Configure stage.
 */
export function DiscoverView() {
  const [sub, setSub] = useState<"review" | "leads">("review");
  const [reloadKey, setReloadKey] = useState(0);

  return (
    <section className="space-y-6">
      <header>
        <h1 className="text-lg font-semibold tracking-tight">Discover brands</h1>
        <p className="mt-1 max-w-2xl text-sm text-gray-500">
          Find new brands with AI discovery or free stockist mining, then approve the cool,
          accessible ones. Approved brands move to <span className="text-gray-700">Configure</span>{" "}
          (Shopify sites get their config automatically).
        </p>
      </header>

      <PipelineActions onChanged={() => setReloadKey((k) => k + 1)} />

      <Segmented
        items={[
          { key: "review", label: "Review queue" },
          { key: "leads", label: "Stockist leads" },
        ]}
        active={sub}
        onChange={(k) => setSub(k as "review" | "leads")}
      />

      {sub === "review" ? (
        <BrandCuration key={`review-${reloadKey}`} />
      ) : (
        <LeadsView key={`leads-${reloadKey}`} onAdded={() => setReloadKey((k) => k + 1)} />
      )}
    </section>
  );
}
