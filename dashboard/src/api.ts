// Typed client for the crawler dashboard API (served by src/server.ts + src/routes/brands.ts).
// Shapes mirror the server responses; keep in sync when the API changes.

export type BrandStatus = "candidate" | "approved" | "rejected";
export type PriceTier = "too_cheap" | "accessible" | "too_expensive" | "unknown";

export interface BrandPriceSample {
  usd: number;
  sourceUrl: string;
  at: string;
}

export interface Brand {
  name: string;
  url: string;
  status?: BrandStatus;
  source?: string;
  region?: string;
  fitScore?: number;
  priceSample?: BrandPriceSample;
  discoveredAt?: string;
  // derived by the server:
  effectiveStatus: BrandStatus;
  tier: PriceTier;
  eligible: boolean;
}

export interface BrandCounts {
  total: number;
  byStatus: Record<BrandStatus, number>;
  byTier: Record<PriceTier, number>;
}

export interface BrandsResponse {
  brands: Brand[];
  counts: BrandCounts;
}

export interface VendorLead {
  name: string;
  usd: number;
  tier: PriceTier;
  productCount: number;
  stockists: string[];
}

export interface BrandLeadsResponse {
  generatedAt: string | null;
  stockists: { stockist: string; ok: boolean; vendorCount: number; productCount: number }[];
  leads: VendorLead[];
}

export interface DiscoveryRun {
  category: string | null;
  newCount: number;
  totalCount: number;
  ranAt: string;
}

export interface DiscoveryRunsResponse {
  runs: DiscoveryRun[];
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { error?: string };
      if (body?.error) detail = body.error;
    } catch {
      /* non-JSON error body */
    }
    throw new Error(`${res.status}: ${detail}`);
  }
  return (await res.json()) as T;
}

/** A single line/event from a streamed pipeline action (discovery / mine / probe). */
export interface StreamEvent {
  type: string;
  /** Progress text. The server uses `message` (tasks/discover) or `msg` (job logs). */
  message?: string;
  msg?: string;
  result?: unknown;
  brands?: unknown;
  error?: string;
  [k: string]: unknown;
}

export interface StreamHandlers {
  onLog?: (line: string) => void;
  onEvent?: (event: StreamEvent) => void;
  /** Called once when the stream reports completion (with optional error). */
  onDone?: (event: StreamEvent) => void;
  onError?: (err: Error) => void;
}

/**
 * Subscribe to a server SSE stream and normalize its events. Returns a function
 * that closes the connection. Handles both the job feed (`/api/progress/:jobId`,
 * fields `msg`) and the task feed (`/api/brands/tasks/:id/stream`, fields `message`).
 */
export function subscribeStream(streamUrl: string, handlers: StreamHandlers): () => void {
  const es = new EventSource(streamUrl);
  es.onmessage = (e: MessageEvent<string>) => {
    let event: StreamEvent;
    try {
      event = JSON.parse(e.data) as StreamEvent;
    } catch {
      return;
    }
    handlers.onEvent?.(event);
    const line = event.message ?? event.msg;
    if (line) handlers.onLog?.(line);
    if (event.type === "done") {
      handlers.onDone?.(event);
      es.close();
    }
  };
  es.onerror = () => {
    // EventSource auto-reconnects; surface only if it's a hard failure (closed).
    if (es.readyState === EventSource.CLOSED) {
      handlers.onError?.(new Error("Stream connection closed."));
    }
  };
  return () => es.close();
}

export const api = {
  discover: (category?: string) =>
    request<{ jobId: string }>("/api/discover-brands", {
      method: "POST",
      body: JSON.stringify(category ? { category } : {}),
    }),
  mineStockists: () =>
    request<{ taskId: string }>("/api/brands/mine", { method: "POST", body: "{}" }),
  probeBrands: (opts?: { onlyCandidates?: boolean; force?: boolean }) =>
    request<{ taskId: string }>("/api/brands/probe", {
      method: "POST",
      body: JSON.stringify(opts ?? {}),
    }),
  jobStreamUrl: (jobId: string) => `/api/progress/${encodeURIComponent(jobId)}`,
  taskStreamUrl: (taskId: string) => `/api/brands/tasks/${encodeURIComponent(taskId)}/stream`,
  getBrands: (params?: { status?: BrandStatus; tier?: PriceTier }) => {
    const qs = new URLSearchParams();
    if (params?.status) qs.set("status", params.status);
    if (params?.tier) qs.set("tier", params.tier);
    const q = qs.toString();
    return request<BrandsResponse>(`/api/brands${q ? `?${q}` : ""}`);
  },
  setBrandStatus: (url: string, status: BrandStatus) =>
    request<{ ok: true; url: string; status: BrandStatus }>("/api/brands/status", {
      method: "POST",
      body: JSON.stringify({ url, status }),
    }),
  addBrand: (url: string, name?: string) =>
    request<{ ok: true; url: string; name?: string }>("/api/brands/add", {
      method: "POST",
      body: JSON.stringify({ url, name }),
    }),
  getBrandLeads: () => request<BrandLeadsResponse>("/api/brand-leads"),
  getDiscoveryRuns: (limit = 50) =>
    request<DiscoveryRunsResponse>(`/api/discovery-runs?limit=${limit}`),
};
