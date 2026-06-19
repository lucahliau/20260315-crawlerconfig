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

// --- Retailer pipeline (explore → crawl → upload) ---

export type ExploreStatus =
  | "idle"
  | "running"
  | "completed"
  | "failed"
  | "needs_retry"
  | "queued_retry";
export type Recommendation = "recommended" | "usable" | "not recommended" | "unknown";

export interface CrawlSummary {
  totalUrls: number;
  crawledAt: string;
  method: string;
}

export interface CrawlLive {
  status: string;
  totalUrls: number | null;
  lastCheckpointAt: string | null;
  error: string | null;
}

export interface UploadLive {
  status: string;
  uploaded: number | null;
  skipped: number | null;
  failed: number | null;
  total: number | null;
  error: string | null;
}

export interface RetailerRow {
  retailer: string;
  filename: string;
  config: { baseUrl?: string; retailerDisplayName?: string } & Record<string, unknown>;
  configValid: boolean;
  configMethod: string | null;
  recommendation: Recommendation;
  storedRecommendation: Recommendation;
  exploreStatus: ExploreStatus;
  exploreError: string | null;
  retryAt: string | null;
  exploreFailureCode: string | null;
  exploreFailureReason: string | null;
  exploreAttempt: number | null;
  exploreMaxAttempts: number | null;
  crawl: CrawlSummary | null;
  upload: { uploaded?: number; skipped?: number; failed?: number; total?: number; uploadedAt?: string } & Record<string, unknown> | null;
  uploadMatchesCurrentCrawl: boolean;
  crawlLive: CrawlLive | null;
  uploadLive: UploadLive | null;
  latestJobId: string | null;
}

export interface RetailersOverviewResponse {
  retailers: RetailerRow[];
  identifiedWithoutConfig: { name: string; url: string }[];
}

export interface ScrapeError {
  code: string;
  detail: string;
  retailer: string | null;
  stage: string;
  url: string | null;
  attempt: number;
  jobId: string | null;
  occurredAt?: string;
}

export interface ErrorsResponse {
  errors: ScrapeError[];
  counts: { byCode: Record<string, number>; byStage: Record<string, number>; total: number };
}

/** Operational issue mirrored from the home worker (src/workerStatus.ts → worker_issues). */
export interface WorkerIssue {
  workerId: string | null;
  source: string;
  severity: string;
  message: string;
  metadata: Record<string, unknown>;
  occurredAt: string;
}

export interface WorkerIssuesResponse {
  issues: WorkerIssue[];
}

// --- Post-processing (MacBook workers: background removal, embeddings) ---

export interface ProcessingTotals {
  total: number;
  nobg: number;
  embedded: number;
}

/** Mac health shipped in the home worker's heartbeat (see src/workerStatus.ts). */
export interface WorkerTelemetry {
  loadAvg1m?: number;
  cpuCount?: number;
  freeMemMb?: number;
  totalMemMb?: number;
  diskFreeGb?: number | null;
  cpuSpeedLimitPct?: number | null;
  onACPower?: boolean | null;
  batteryPct?: number | null;
  commit?: string | null;
  updateAvailable?: boolean;
  activeJobs?: string[];
  recentIssues?: number;
}

export interface WorkerHeartbeat {
  workerId: string;
  hostname: string | null;
  pid: number | null;
  concurrency: number;
  metadata: { queues?: string[]; telemetry?: WorkerTelemetry } & Record<string, unknown>;
  firstSeenAt: string;
  lastSeenAt: string;
  ageSeconds: number;
}

export interface ProcessingResponse {
  totals: ProcessingTotals;
  rates: {
    nobg: { last1h: number; last24h: number };
    embeddings: { last1h: number; last24h: number };
    person?: { last1h: number; last24h: number };
  };
  /** People-photo scan coverage: items scanned, products hidden, still to scan. */
  person?: { scanned: number; hidden: number; needsScan: number };
  perRetailer: { retailer: string; total: number; nobg: number; embedded: number }[];
  backlog?: { needsNobg: number; needsEmbed: number; needsPerson?: number };
  queues?: QueueStat[];
  workers?: WorkerHeartbeat[];
  homeServerOnline?: boolean;
}

export interface PipelineSettings {
  auto_pipeline: boolean;
  gate_crawl: boolean;
  gate_upload: boolean;
  gate_processing: boolean;
}

// --- Mobile swipe queue ---

export interface PreviewImage {
  src: string;
  title?: string;
}

export interface BrandPreview {
  url: string;
  ok: boolean;
  hero: string | null;
  images: PreviewImage[];
  source: "shopify" | "sampled" | "og" | "none";
  fetchedAt: string;
}

export type SwipeBrand = Brand & { preview: BrandPreview | null };

export interface SwipeQueueResponse {
  total: number;
  brands: SwipeBrand[];
}

// --- Systems health ---

export interface SystemCheck {
  ok: boolean;
  latencyMs?: number;
  error?: string;
  [k: string]: unknown;
}

export interface QueueStat {
  name: string;
  waiting: number;
  active: number;
  failed: number;
  completed: number;
}

export interface SystemsResponse {
  checkedAt: string;
  crawler: { ok: boolean; uptimeSeconds: number; rssMb: number };
  database: SystemCheck & { sizeMb?: number; limitMb?: number; items?: number };
  backend: SystemCheck & { status?: number; url?: string };
  r2: SystemCheck & { bucket?: string };
  queue: SystemCheck & { queues?: QueueStat[] };
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

export type AnalyticsRange = "7d" | "30d" | "all";

export interface AnalyticsTopItem {
  itemId: string;
  name: string;
  brand: string;
  n: number;
}

export interface AnalyticsSummary {
  range: AnalyticsRange;
  since: string;
  checkedAt: string;
  engagement: {
    dau: number;
    wau: number;
    mau: number;
    sessions: number;
    events: number;
    anonSessions: number;
    identifiedSessions: number;
    avgSessionMs: number;
    medianSessionMs: number;
  };
  retention: {
    cohortSize: number;
    d1Eligible: number;
    d1Returned: number;
    d7Eligible: number;
    d7Returned: number;
  };
  funnel: { registered: number; onboarded: number; swiped: number; collected: number };
  features: {
    screens: { screen: string; count: number }[];
    searches: number;
    zeroResultSearches: number;
    swipeActions: { action: string; count: number }[];
    itemViews: number;
    uniqueItemsViewed: number;
    follows: number;
    messages: number;
  };
  content: {
    topLoved: AnalyticsTopItem[];
    topViewed: AnalyticsTopItem[];
    topBrands: { brand: string; n: number }[];
  };
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

  // --- Retailer pipeline ---
  getRetailersOverview: () => request<RetailersOverviewResponse>("/api/retailers-overview"),
  /**
   * One-shot: add a brand by URL to the template, then run the FULL identify
   * ladder (paid AI rungs allowed) — the conveyor auto-chains crawl → upload →
   * process. Returns the explore jobId so the caller can stream its log.
   */
  addBrandUrl: (url: string, name?: string) =>
    request<{ jobId: string; url: string; retailer: string; autopilotOn: boolean; message: string }>(
      "/api/pipeline/add-url",
      { method: "POST", body: JSON.stringify({ url, name }) },
    ),
  explore: (urls: string[], skipExisting = true) =>
    request<{ jobId: string; skippedUrls: string[]; skippedCount: number }>("/api/explore", {
      method: "POST",
      body: JSON.stringify({ urls, skipExisting }),
    }),
  exploreRetry: (retailers: string[]) =>
    request<{ jobId: string; submittedCount: number }>("/api/explore/retry", {
      method: "POST",
      body: JSON.stringify({ retailers }),
    }),
  crawl: (retailer: string) =>
    request<{ jobId: string }>(`/api/crawl/${encodeURIComponent(retailer)}`, {
      method: "POST",
      body: "{}",
    }),
  upload: (retailer: string) =>
    request<{ jobId: string }>(`/api/upload/${encodeURIComponent(retailer)}`, {
      method: "POST",
      body: "{}",
    }),
  runE2E: () => request<{ jobId: string }>("/api/run-e2e", { method: "POST", body: "{}" }),
  setRecommendation: (retailer: string, recommendation: Recommendation) =>
    request<{ retailer: string; recommendation: Recommendation }>(
      `/api/configs/${encodeURIComponent(retailer)}/recommendation`,
      { method: "PATCH", body: JSON.stringify({ recommendation }) },
    ),
  getErrors: (params?: { sinceMinutes?: number; retailer?: string; limit?: number }) => {
    const qs = new URLSearchParams();
    if (params?.sinceMinutes) qs.set("since", String(params.sinceMinutes));
    if (params?.retailer) qs.set("retailer", params.retailer);
    if (params?.limit) qs.set("limit", String(params.limit));
    const q = qs.toString();
    return request<ErrorsResponse>(`/api/errors${q ? `?${q}` : ""}`);
  },
  getWorkerIssues: (params?: { sinceMinutes?: number; source?: string; limit?: number }) => {
    const qs = new URLSearchParams();
    if (params?.sinceMinutes) qs.set("since", String(params.sinceMinutes));
    if (params?.source) qs.set("source", params.source);
    if (params?.limit) qs.set("limit", String(params.limit));
    const q = qs.toString();
    return request<WorkerIssuesResponse>(`/api/worker-issues${q ? `?${q}` : ""}`);
  },
  getProcessing: () => request<ProcessingResponse>("/api/processing"),
  getSystems: () => request<SystemsResponse>("/api/systems"),
  getAnalytics: (range: AnalyticsRange = "30d") =>
    request<AnalyticsSummary>(`/api/analytics/summary?range=${range}`),
  getSwipeQueue: (limit = 15) =>
    request<SwipeQueueResponse>(`/api/swipe-queue?limit=${limit}`),

  // --- Processing control (jobs claimed by the home worker) ---
  uploadEnqueue: (retailer: string, limit?: number) =>
    request<{ retailer: string; totalUrls: number; enqueued: number; duplicates: number }>(
      `/api/upload/${encodeURIComponent(retailer)}/enqueue`,
      { method: "POST", body: JSON.stringify(limit ? { limit } : {}) },
    ),
  runProcessing: (kind: "nobg" | "embed" | "person" | "all", limit?: number) =>
    request<{ jobIds: Record<string, string | null> }>("/api/processing/run", {
      method: "POST",
      body: JSON.stringify({ kind, ...(limit ? { limit } : {}) }),
    }),
  cancelProcessing: (kind: "nobg" | "embed" | "person") =>
    request<{ cancelled: number }>("/api/processing/cancel", {
      method: "POST",
      body: JSON.stringify({ kind }),
    }),
  getPipelineSettings: () => request<PipelineSettings>("/api/pipeline/settings"),
  getAutopilotOptout: (retailer: string) =>
    request<{ retailer: string; optout: boolean }>(
      `/api/pipeline/optout/${encodeURIComponent(retailer)}`,
    ),
  setAutopilotOptout: (retailer: string, optout: boolean) =>
    request<{ retailer: string; optout: boolean }>("/api/pipeline/optout", {
      method: "POST",
      body: JSON.stringify({ retailer, optout }),
    }),
  setPipelineSettings: (patch: Partial<PipelineSettings>) =>
    request<{ updated: Partial<PipelineSettings> }>("/api/pipeline/settings", {
      method: "POST",
      body: JSON.stringify(patch),
    }),
};
