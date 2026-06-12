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

    let timedOut = false;
    const killTimer = setTimeout(() => {
      timedOut = true;
      opts.onLine(`[bridge] timeout after ${Math.round(opts.timeoutMs / 60000)}min — SIGTERM`);
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
          if (line) opts.onLine(line);
        }
      });
      stream.on("end", () => {
        if (buf.trim()) opts.onLine(buf.trim());
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
      resolve({ exitCode: code ?? -1, timedOut });
    });
  });
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
    `SELECT
       COUNT(*) FILTER (WHERE NOT ci."hasNobg") AS needs_nobg,
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
}

/**
 * One bounded background-removal batch: spawns remove-bg-parallel.ts, parses
 * the new history.jsonl lines it appended, and flips hasNobg for successes.
 */
export async function runNobgBatch(opts: {
  limit: number;
  log: (line: string) => void;
}): Promise<NobgBatchResult> {
  const historyFile = path.join(BGREMOVER_DIR, "history.jsonl");
  const startOffset = fs.existsSync(historyFile) ? fs.statSync(historyFile).size : 0;

  const parallel = process.env.PROCESS_NOBG_PARALLEL ?? "5";
  let remaining = -1;
  const { exitCode, timedOut } = await runBridged(
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
        opts.log(line);
      },
    },
  );

  if (timedOut || exitCode !== 0) {
    throw new Error(`remove-bg-parallel exited ${exitCode}${timedOut ? " (timeout)" : ""}`);
  }

  // history.jsonl lines appended during this run: {key, status, ts}
  const successKeys: string[] = [];
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
            else if (entry.status === "failed") failed++;
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

  return { processed: successKeys.length, failed, remaining, hasNobgUpdated };
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
  const python = path.join(BGREMOVER_DIR, "venv", "bin", "python");
  const { exitCode, timedOut } = await runBridged(
    python,
    [
      "embed_worker.py",
      "--download-workers",
      process.env.PROCESS_EMBED_DOWNLOAD_WORKERS ?? "12",
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
    throw new Error(`embed_worker exited ${exitCode}${timedOut ? " (timeout)" : ""}`);
  }

  // The worker decides whether to chain another batch from the live backlog.
  const backlog = await getProcessingBacklog().catch(() => null);
  return { hitLimit: (backlog?.needsEmbed ?? 0) > 0 };
}
