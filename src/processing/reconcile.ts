/**
 * hasNobg ⇄ R2 reconciliation — the authoritative heal that the manual
 * `populate-has-nobg` backend script was meant to provide on a schedule but
 * never did (it was never wired to a cron, so drift accumulated indefinitely).
 *
 * Why drift happens at all (the live per-image flip isn't enough):
 *   The live path (`setHasNobgForKeys`) only flips hasNobg for items the
 *   bg-removal tool actually PROCESSES in a batch. Two whole classes are
 *   invisible to it:
 *     • -nobg.png already exists in R2 but the DB row says hasNobg=false
 *       (re-crawls/re-imports, swallowed live writes, the original migration
 *       default). The tool sees the existing -nobg sibling, skips the item as
 *       "done", so it is NEVER processed → never flipped → permanent phantom
 *       backlog the dashboard counts as "needs nobg" forever.
 *     • hasNobg=true but the -nobg.png is gone/never-uploaded (404) → the feed
 *       shows a broken image and embed 404s on the download.
 *
 * This sweep HEADs each item's derived -nobg.png and sets hasNobg to match R2
 * truth. HEAD-only, so it is cheap enough to run frequently. The worker runs it
 * on a loop (see worker.ts): a cheap FORWARD pass (only hasNobg=false rows)
 * often, and a FULL bidirectional pass (also demotes missing-file rows) less
 * often. Reverse demotions act ONLY on a definitive 404 — a transient R2
 * error (5xx/network/403) is skipped so an outage can't blank the feed.
 */
import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";

import { getPool } from "../pipelineStore.js";

let r2Client: S3Client | null = null;
function getR2(): S3Client | null {
  if (r2Client) return r2Client;
  const acct = process.env.R2_ACCOUNT_ID;
  const accessKeyId = process.env.R2_ACCESS_KEY_ID;
  const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
  if (!acct || !accessKeyId || !secretAccessKey) return null;
  r2Client = new S3Client({
    region: "auto",
    endpoint: `https://${acct}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId, secretAccessKey },
  });
  return r2Client;
}

/**
 * Derive the -nobg.png R2 key from an item's imageUrl. Mirrors the backend's
 * getNobgUrl / the bgremover key rule: strip the public-URL prefix, then insert
 * `-nobg.png` in place of the extension on the last path segment.
 */
export function nobgKeyFromImageUrl(imageUrl: string, publicBase: string): string | null {
  let p = imageUrl;
  if (publicBase && p.startsWith(publicBase)) p = p.slice(publicBase.length);
  p = p.replace(/^\/+/, "");
  const slash = p.lastIndexOf("/");
  const name = p.slice(slash + 1);
  const dot = name.lastIndexOf(".");
  const stem = dot > 0 ? name.slice(0, dot) : name;
  if (!stem) return null;
  return p.slice(0, slash + 1) + stem + "-nobg.png";
}

type HeadState = "exists" | "missing" | "error";

export interface ReconcileResult {
  mode: "forward" | "full";
  checked: number;
  flippedTrue: number;
  flippedFalse: number;
  transientErrors: number;
}

/**
 * @param mode "forward" = only check hasNobg=false rows and promote existing
 *   ones (cheap, scales with the backlog). "full" = check every active item and
 *   also demote rows whose -nobg.png 404s (bounded by catalog size).
 */
export async function reconcileHasNobg(opts?: {
  mode?: "forward" | "full";
  log?: (line: string) => void;
  concurrency?: number;
}): Promise<ReconcileResult> {
  const mode = opts?.mode ?? "forward";
  const log = opts?.log ?? (() => {});
  const conc = Math.max(1, opts?.concurrency ?? parseInt(process.env.RECONCILE_CONCURRENCY ?? "25", 10));
  const empty: ReconcileResult = { mode, checked: 0, flippedTrue: 0, flippedFalse: 0, transientErrors: 0 };

  const pool = getPool();
  const r2 = getR2();
  const bucket = process.env.R2_BUCKET_NAME;
  const pub = (process.env.R2_PUBLIC_URL ?? "").replace(/\/$/, "");
  if (!pool || !r2 || !bucket) {
    log("[reconcile] missing DATABASE_URL/R2 config — skipping");
    return empty;
  }

  const where =
    mode === "forward"
      ? `active = true AND "hasNobg" IS NOT TRUE AND "imageUrl" IS NOT NULL`
      : `active = true AND "imageUrl" IS NOT NULL`;
  const { rows } = await pool.query<{ id: string; imageUrl: string; hasNobg: boolean }>(
    `SELECT id, "imageUrl", "hasNobg" FROM "ClothingItem" WHERE ${where}`,
  );

  const head = async (key: string): Promise<HeadState> => {
    try {
      await r2.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      return "exists";
    } catch (err) {
      const e = err as { $metadata?: { httpStatusCode?: number }; name?: string };
      if (e.$metadata?.httpStatusCode === 404 || e.name === "NotFound" || e.name === "NoSuchKey") return "missing";
      return "error"; // transient (network/5xx/403) — never act on these
    }
  };

  const toTrue: string[] = [];
  const toFalse: string[] = [];
  let transientErrors = 0;
  for (let i = 0; i < rows.length; i += conc) {
    const states = await Promise.all(
      rows.slice(i, i + conc).map(async (r) => {
        const key = nobgKeyFromImageUrl(r.imageUrl, pub);
        // No derivable key = no -nobg possible → treat as missing.
        return { r, st: key ? await head(key) : ("missing" as HeadState) };
      }),
    );
    for (const { r, st } of states) {
      if (st === "error") transientErrors++;
      else if (st === "exists" && r.hasNobg !== true) toTrue.push(r.id);
      else if (st === "missing" && r.hasNobg === true && mode === "full") toFalse.push(r.id);
    }
  }

  let flippedTrue = 0;
  let flippedFalse = 0;
  if (toTrue.length) {
    const u = await pool.query(
      `UPDATE "ClothingItem" ci SET "hasNobg" = true, "updatedAt" = NOW()
       FROM unnest($1::text[]) AS k(id) WHERE ci.id = k.id AND ci."hasNobg" IS NOT TRUE`,
      [toTrue],
    );
    flippedTrue = u.rowCount ?? 0;
  }
  if (toFalse.length) {
    const u = await pool.query(
      `UPDATE "ClothingItem" ci SET "hasNobg" = false, "updatedAt" = NOW()
       FROM unnest($1::text[]) AS k(id) WHERE ci.id = k.id AND ci."hasNobg" IS TRUE`,
      [toFalse],
    );
    flippedFalse = u.rowCount ?? 0;
  }

  log(
    `[reconcile:${mode}] checked=${rows.length} +hasNobg=${flippedTrue} -hasNobg=${flippedFalse}` +
      (transientErrors ? ` (skipped ${transientErrors} transient R2 errors)` : ""),
  );
  return { mode, checked: rows.length, flippedTrue, flippedFalse, transientErrors };
}
