/**
 * Per-domain politeness gate for the upload worker.
 *
 * The queue claims `batchSize: WORKER_CONCURRENCY` jobs and runs them with
 * `Promise.all`, and each upload job is a single URL — so the per-config
 * `delayBetweenRequestsMs` (which only spaces URLs *within* one uploadRetailer
 * call) never applies across jobs. Without this gate, several pages of the
 * SAME domain get fetched concurrently with no delay, which trips sites'
 * rate limiters (observed: cafeducycliste 429s under sustained M1 load).
 *
 * This gate serializes work per hostname (concurrency 1 per domain) and
 * enforces a minimum gap between consecutive same-domain runs. Different
 * domains still run in parallel up to WORKER_CONCURRENCY, so throughput is
 * preserved for a healthy spread of retailers.
 */

const DEFAULT_PER_DOMAIN_DELAY_MS = 1500;

function configuredDelayMs(): number {
  const raw = parseInt(process.env.UPLOAD_PER_DOMAIN_DELAY_MS ?? "", 10);
  return Number.isFinite(raw) && raw >= 0 ? raw : DEFAULT_PER_DOMAIN_DELAY_MS;
}

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Tail of the in-flight chain per domain, plus the last completion time. */
type DomainSlot = { tail: Promise<unknown>; lastRunAt: number };

const slots = new Map<string, DomainSlot>();

/**
 * Run `task` such that no two tasks for the same `domain` overlap, and
 * consecutive same-domain tasks are spaced by at least the configured delay.
 * Returns the task's result (or rejects with its error) — the chain itself
 * never rejects, so one failure does not poison later same-domain work.
 */
export function runForDomain<T>(domain: string, task: () => Promise<T>): Promise<T> {
  const minGap = configuredDelayMs();
  const prev = slots.get(domain);
  const priorTail = prev?.tail ?? Promise.resolve();

  const run = priorTail.then(async () => {
    const slot = slots.get(domain);
    const since = slot ? Date.now() - slot.lastRunAt : Infinity;
    if (Number.isFinite(since) && since < minGap) {
      await delay(minGap - since);
    }
    try {
      return await task();
    } finally {
      const cur = slots.get(domain);
      if (cur) cur.lastRunAt = Date.now();
    }
  });

  // The stored tail must never reject (it only gates ordering). Swallow here;
  // callers still see the real result/error via the returned `run`. We keep
  // one entry per distinct domain for the worker's lifetime (bounded by the
  // catalog's retailer count) so `lastRunAt` keeps enforcing the gap.
  const tail = run.then(
    () => undefined,
    () => undefined,
  );
  slots.set(domain, { tail, lastRunAt: prev?.lastRunAt ?? 0 });

  return run;
}

/** Resolve a URL to the hostname used as the gate key (falls back to input). */
export function domainKey(url: string, fallback: string): string {
  try {
    return new URL(url).hostname;
  } catch {
    return fallback;
  }
}
