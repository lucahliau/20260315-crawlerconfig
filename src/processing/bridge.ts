/**
 * Processing bridge — runs the proven post-processing tools that live in the
 * separate `bgremoverimages` folder (background removal via rembg, CLIP
 * embeddings via embed_worker.py) as child processes of the queue worker.
 *
 * Why a bridge instead of a port: those scripts are battle-tested, resumable
 * (R2/DB are the source of truth) and tuned for Apple-Silicon MPS. The worker
 * just needs to launch them in bounded batches, stream their output into the
 * job log, and sync `ClothingItem.hasNobg` afterwards.
 *
 * Requires on the worker machine (the home M1):
 *   BGREMOVER_DIR  — path to the bgremoverimages folder (with venv set up)
 *   R2_* env vars + R2_PUBLIC_URL + DATABASE_URL (the scripts don't load
 *   their own .env when spawned here; they inherit the worker's env).
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import { getPool } from "../pipelineStore.js";
import { resolveEmbedPython } from "./embedPython.js";

export const BGREMOVER_DIR =
  process.env.BGREMOVER_DIR ?? "/Users/lucaliautaud/Desktop/20260315 bgremoverimages";

/** Thrown for exit codes that are known-safe to retry (embed MPS watchdog). */
export class RetryableExitError extends Error {
  constructor(public readonly exitCode: number, message: string) {
    super(message);
    this.name = "RetryableExitError";
  }
}

interface BridgedResult {
  exitCode: number;
  timedOut: boolean;
  /** Last lines of combined stdout/stderr — surfaced on failure so a crash is
   *  diagnosable remotely (the full stream only reaches the M1's local log). */
  tail: string[];
}

function runBridged(
  cmd: string,
  args: string[],
  opts: {
    cwd: string;
    env: NodeJS.ProcessEnv;
    timeoutMs: number;
    onLine: (line: string) => void;
  },
): Promise<BridgedResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: opts.cwd,
      env: opts.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    // Keep the last N lines so a non-zero exit can carry real detail (the full
    // stream is only logged on the M1; the tail rides the thrown error to PG).
    const tail: string[] = [];
    const emit = (line: string) => {
      tail.push(line);
      // Keep a generous tail: a crash traceback can sit behind dozens of noisy
      // per-item "download failed" lines, and a too-small tail evicted the real
      // error before it reached Postgres (embed_worker exit -1 was undiagnosable
      // remotely for exactly this reason).
      if (tail.length > 120) tail.shift();
      opts.onLine(line);
    };

    let timedOut = false;
    const killTimer = setTimeout(() => {
      timedOut = true;
      emit(`[bridge] timeout after ${Math.round(opts.timeoutMs / 60000)}min — SIGTERM`);
      child.kill("SIGTERM");
      setTimeout(() => child.kill("SIGKILL"), 30_000).unref();
    }, opts.timeoutMs);

    const lineBuffer = (stream: NodeJS.ReadableStream) => {
      let buf = "";
      stream.on("data", (chunk: Buffer) => {
        buf += chunk.toString("utf8");
        let idx: number;
        while ((idx = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, idx).trimEnd();
          buf = buf.slice(idx + 1);
          if (line) emit(line);
        }
      });
      stream.on("end", () => {
        if (buf.trim()) emit(buf.trim());
      });
    };
    if (child.stdout) lineBuffer(child.stdout);
    if (child.stderr) lineBuffer(child.stderr);

    child.on("error", (err) => {
      clearTimeout(killTimer);
      reject(err);
    });
    child.on("close", (code) => {
      clearTimeout(killTimer);
      resolve({ exitCode: code ?? -1, timedOut, tail });
    });
  });
}

/**
 * Drop embed_worker's per-item download chatter from a captured tail so a real
 * Python traceback isn't evicted by dozens of "download failed … 404" lines
 * before it can ride a thrown error into Postgres. (That eviction is exactly why
 * an embed crash long read as a bare ".../multiprocessing/res…" truncation
 * instead of the real error.) Falls back to the raw tail if filtering would
 * leave almost nothing.
 */
function meaningfulTail(tail: string[], n: number): string[] {
  const noise = /download (failed|ok|future error)|download heartbeat/i;
  const signal = tail.filter((l) => !noise.test(l));
  return (signal.length >= 3 ? signal : tail).slice(-n);
}

/**
 * Flip hasNobg=true for items whose FIRST image (imageUrl) was just processed.
 * Targeted by exact R2 key — the backend's getNobgUrl() derives the nobg
 * variant from imageUrl, so this matches its semantics. The weekly
 * `populate-has-nobg` reconciliation (backend repo) heals anything missed.
 */
export async function setHasNobgForKeys(keys: string[]): Promise<number> {
  if (keys.length === 0) return 0;
  const pool = getPool();
  if (!pool) return 0;
  const result = await pool.query(
    `UPDATE "ClothingItem" ci
     SET "hasNobg" = true, "updatedAt" = NOW()
     FROM unnest($1::text[]) AS k(key)
     WHERE ci."hasNobg" IS DISTINCT FROM true
       AND (ci."imageUrl" = k.key OR ci."imageUrl" LIKE '%/' || k.key)`,
    [keys],
  );
  return result.rowCount ?? 0;
}

export interface ProcessingBacklog {
  needsNobg: number;
  needsEmbed: number;
}

/** Items still needing background removal / embeddings (active catalog only). */
export async function getProcessingBacklog(): Promise<ProcessingBacklog> {
  const pool = getPool();
  if (!pool) return { needsNobg: 0, needsEmbed: 0 };
  const { rows } = await pool.query(
    // `hasNobg IS NOT TRUE` (not `NOT hasNobg`): in SQL three-valued logic
    // `NOT NULL` = NULL, which a FILTER drops — so the old predicate counted ONLY
    // the explicit-`false` rows and silently ignored every never-processed
    // `hasNobg IS NULL` item. That made the self-feeding backlog loop blind to the
    // bulk of the nobg backlog (it saw a few hundred, not ~10k) → nobg stalled.
    `SELECT
       COUNT(*) FILTER (WHERE ci."hasNobg" IS NOT TRUE) AS needs_nobg,
       COUNT(*) FILTER (WHERE ci."hasNobg" AND e."itemId" IS NULL) AS needs_embed
     FROM "ClothingItem" ci
     LEFT JOIN (SELECT DISTINCT "itemId" FROM "ItemEmbedding") e ON e."itemId" = ci.id
     WHERE ci.active = true`,
  );
  return {
    needsNobg: Number(rows[0]?.needs_nobg ?? 0),
    needsEmbed: Number(rows[0]?.needs_embed ?? 0),
  };
}

export interface NobgBatchResult {
  processed: number;
  failed: number;
  /** Unprocessed images the tool reported beyond this batch (backlog estimate). */
  remaining: number;
  hasNobgUpdated: number;
  /** Per-image failures from this batch, with the reason parsed from the tool's
   *  stdout — recorded to scrape_errors so they're diagnosable off the M1. */
  failures: { key: string; reason: string }[];
}

/**
 * One bounded background-removal batch: spawns remove-bg-parallel.ts, parses
 * the new history.jsonl lines it appended, and flips hasNobg for successes.
 */
export async function runNobgBatch(opts: {
  limit: number;
  log: (line: string) => void;
  /** Adaptive parallel rembg chains chosen by the worker's capacity governor.
   *  Falls back to PROCESS_NOBG_PARALLEL / 6 when not supplied. */
  parallel?: number;
}): Promise<NobgBatchResult> {
  const historyFile = path.join(BGREMOVER_DIR, "history.jsonl");
  const startOffset = fs.existsSync(historyFile) ? fs.statSync(historyFile).size : 0;

  const parallel = String(
    opts.parallel && opts.parallel > 0
      ? opts.parallel
      : Math.max(1, parseInt(process.env.PROCESS_NOBG_PARALLEL ?? "6", 10)),
  );
  let remaining = -1;
  // The tool logs the cause as "...failed for <key>: <message>" before it
  // appends the {key,status:"failed"} history line — capture it so each failed
  // key carries a real reason instead of just a count.
  const reasonByKey = new Map<string, string>();
  const { exitCode, timedOut, tail } = await runBridged(
    "npx",
    ["tsx", "remove-bg-parallel.ts", String(opts.limit)],
    {
      cwd: BGREMOVER_DIR,
      env: { ...process.env, PARALLEL_CHAINS: parallel },
      // ~90s/image worst case + warmup margin, capped under the 2h job expiry.
      timeoutMs: Math.min(opts.limit * 90_000 + 10 * 60_000, 110 * 60_000),
      onLine: (line) => {
        const m = line.match(/Found (\d+) unprocessed images/);
        if (m) remaining = Math.max(0, Number(m[1]) - opts.limit);
        const f = line.match(/failed for (.+?): (.+)$/);
        if (f) reasonByKey.set(f[1]!, f[2]!.slice(0, 500));
        opts.log(line);
      },
    },
  );

  if (timedOut || exitCode !== 0) {
    const detail = tail.slice(-25).join(" | ").slice(0, 4000);
    throw new Error(
      `remove-bg-parallel exited ${exitCode}${timedOut ? " (timeout)" : ""}${detail ? ` — ${detail}` : ""}`,
    );
  }

  // history.jsonl lines appended during this run: {key, status, ts}
  const successKeys: string[] = [];
  const failedKeys: string[] = [];
  let failed = 0;
  if (fs.existsSync(historyFile)) {
    const fd = fs.openSync(historyFile, "r");
    try {
      const size = fs.fstatSync(fd).size;
      if (size > startOffset) {
        const buf = Buffer.alloc(size - startOffset);
        fs.readSync(fd, buf, 0, buf.length, startOffset);
        for (const line of buf.toString("utf8").split("\n")) {
          if (!line.trim()) continue;
          try {
            const entry = JSON.parse(line) as { key?: string; status?: string };
            if (entry.status === "success" && entry.key) successKeys.push(entry.key);
            else if (entry.status === "failed") {
              failed++;
              if (entry.key) failedKeys.push(entry.key);
            }
          } catch {
            // Partial/corrupt line — skip.
          }
        }
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  let hasNobgUpdated = 0;
  try {
    hasNobgUpdated = await setHasNobgForKeys(successKeys);
    opts.log(`[bridge] hasNobg set on ${hasNobgUpdated} items (${successKeys.length} keys processed)`);
  } catch (err) {
    // Non-fatal: weekly populate-has-nobg reconciles from R2 truth.
    opts.log(
      `[bridge] hasNobg sync failed (will reconcile later): ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  const failures = failedKeys.map((key) => ({
    key,
    reason: reasonByKey.get(key) ?? "background removal failed (no detail captured)",
  }));

  return { processed: successKeys.length, failed, remaining, hasNobgUpdated, failures };
}

export interface EmbedBatchResult {
  /** True when the batch likely exhausted its limit (more backlog may remain). */
  hitLimit: boolean;
}

/**
 * One bounded embedding batch via embed_worker.py (DB-resumable; selects
 * active hasNobg items without an ItemEmbedding row itself).
 */
export async function runEmbedBatch(opts: {
  limit: number;
  log: (line: string) => void;
}): Promise<EmbedBatchResult> {
  // Resolve a Python that can actually run embed_worker.py. The M1's bgremover
  // venv was rebuilt on Homebrew python@3.14, where torch aborts in
  // multiprocessing teardown (embed_worker exited -1 → ~0 embeddings while nobg
  // kept draining). resolveEmbedPython rejects >=3.14, prefers a managed
  // 3.11–3.13 venv, and self-heals by building one if needed — so the fix rides
  // the auto-update with no SSH to the M1. The M4's healthy <BG>/venv (3.13) is
  // accepted as-is, so nothing changes there.
  const python = await resolveEmbedPython({ log: opts.log });
  const { exitCode, timedOut, tail } = await runBridged(
    python,
    [
      "embed_worker.py",
      // Download workers are network-bound (R2 fetch). 16 against the public
      // r2.dev URL draws per-IP 429s from one home IP, so the default is 8 while
      // embed_worker fetches over the public URL; once the venv has boto3 it
      // fetches via the authenticated S3 endpoint (no per-IP throttle) and this
      // can be raised again via PROCESS_EMBED_DOWNLOAD_WORKERS. Batch size stays
      // 32: that's the MPS/GPU memory knob bounded by 8GB unified RAM.
      "--download-workers",
      process.env.PROCESS_EMBED_DOWNLOAD_WORKERS ?? "8",
      "--batch-size",
      process.env.PROCESS_EMBED_BATCH_SIZE ?? "32",
      "--limit",
      String(opts.limit),
    ],
    {
      cwd: BGREMOVER_DIR,
      env: { ...process.env },
      timeoutMs: 60 * 60_000,
      onLine: opts.log,
    },
  );

  if (exitCode === 124) {
    // MPS encode watchdog — the run_embed.sh wrapper restarts on this too.
    // DB-resumable, so a pg-boss retry simply continues where it stopped.
    throw new RetryableExitError(124, "embed_worker MPS watchdog (exit 124) — retry resumes");
  }
  if (timedOut || exitCode !== 0) {
    // Filter the per-item 404 chatter so the actual Python traceback survives
    // the tail cut (see meaningfulTail). Tag the interpreter so a venv/version
    // problem is obvious straight from scrape_errors.
    const detail = meaningfulTail(tail, 25).join(" | ").slice(0, 4000);
    throw new Error(
      `embed_worker exited ${exitCode}${timedOut ? " (timeout)" : ""} [py=${python}]${detail ? ` — ${detail}` : ""}`,
    );
  }

  // The worker decides whether to chain another batch from the live backlog.
  const backlog = await getProcessingBacklog().catch(() => null);
  return { hitLimit: (backlog?.needsEmbed ?? 0) > 0 };
}
