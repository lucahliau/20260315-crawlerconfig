// Backfill + evaluate the heuristic classifier over the whole ClothingItem
// catalog (the shared Supabase DB).
//
//   npx tsx scripts/classify-catalog.ts                 # DRY-RUN (no writes) + report
//   npx tsx scripts/classify-catalog.ts --sample 80     # dry-run + print N hidden samples
//   npx tsx scripts/classify-catalog.ts --apply          # write isClothing/gender/productType/...
//
// Soft-hide only: sets isClothing=false on non-wearables; never deletes.
// Two-pass: pass 1 resolves gender from per-item signals, then we derive a
// per-retailer default gender from the confident resolutions and re-run items
// that came back unisex-with-no-signal (recovers single-gender brands).

import { readFileSync } from "node:fs";
import { Pool } from "pg";
import { classifyItem, normalizeGender, type ClassifyInput, type Gender } from "../src/classify.js";

const APPLY = process.argv.includes("--apply");
const sampleArg = process.argv.indexOf("--sample");
const SAMPLE = sampleArg >= 0 ? parseInt(process.argv[sampleArg + 1] ?? "40", 10) : 40;
const limitArg = process.argv.indexOf("--limit");
const LIMIT = limitArg >= 0 ? parseInt(process.argv[limitArg + 1] ?? "0", 10) : 0;

function dbUrl(): string {
  const env = readFileSync(new URL("../.env", import.meta.url), "utf8");
  const m = env.match(/^DATABASE_URL=(.*)$/m);
  if (!m) throw new Error("DATABASE_URL not found in .env");
  return m[1].trim().replace(/^["']|["']$/g, "");
}

type Row = {
  id: string;
  name: string | null;
  category: string | null;
  subcategory: string | null;
  tags: string[] | null;
  sourceUrl: string | null;
  description: string | null;
  metadata: Record<string, unknown> | null;
  retailer: string | null;
  gender: string | null;
};

function breadcrumbsFrom(meta: Record<string, unknown> | null): string[] | null {
  if (!meta) return null;
  const b = (meta as { breadcrumbs?: unknown }).breadcrumbs;
  if (Array.isArray(b)) return b.map(String);
  if (typeof b === "string") return [b];
  return null;
}

function toInput(r: Row, retailerDefault?: Gender | null): ClassifyInput {
  return {
    name: r.name,
    category: r.category,
    subcategory: r.subcategory,
    tags: r.tags,
    sourceUrl: r.sourceUrl,
    description: r.description,
    breadcrumbs: breadcrumbsFrom(r.metadata),
    retailer: r.retailer,
    retailerDefaultGender: retailerDefault ?? null,
  };
}

async function main() {
  const pool = new Pool({ connectionString: dbUrl(), connectionTimeoutMillis: 9000 });
  const sql = LIMIT > 0 ? `SELECT id,name,category,subcategory,tags,"sourceUrl",description,metadata,retailer,gender FROM "ClothingItem" LIMIT ${LIMIT}` : `SELECT id,name,category,subcategory,tags,"sourceUrl",description,metadata,retailer,gender FROM "ClothingItem"`;
  const rows: Row[] = (await pool.query(sql)).rows;
  console.log(`Loaded ${rows.length} rows. Mode: ${APPLY ? "APPLY" : "DRY-RUN"}\n`);

  // Pass 1 — per-item signals only.
  const pass1 = rows.map((r) => classifyItem(toInput(r)));

  // Derive per-retailer default gender from EXPLICIT, site-stated gender only
  // (never garment-implied — a few stray "dress/skirt" matches must not flip a
  // menswear brand female). Require a solid absolute base + lopsided share.
  const tally = new Map<string, { male: number; female: number }>();
  rows.forEach((r, i) => {
    const g = pass1[i].gender;
    if (!r.retailer || pass1[i].genderSource !== "explicit" || (g !== "male" && g !== "female")) return;
    const t = tally.get(r.retailer) ?? { male: 0, female: 0 };
    t[g]++;
    tally.set(r.retailer, t);
  });
  const retailerDefault = new Map<string, Gender>();
  for (const [retailer, t] of tally) {
    const sum = t.male + t.female;
    const winner = Math.max(t.male, t.female);
    if (sum < 25 || winner < 25) continue; // need real explicit evidence
    if (t.male / sum >= 0.9) retailerDefault.set(retailer, "male");
    else if (t.female / sum >= 0.9) retailerDefault.set(retailer, "female");
  }

  // Pass 2 — re-resolve only the "no signal → unisex" items using the default.
  const final = rows.map((r, i) => {
    const c = pass1[i];
    if (c.gender === "unisex" && r.retailer && retailerDefault.has(r.retailer) && c.signal.includes("no gender signal")) {
      return classifyItem(toInput(r, retailerDefault.get(r.retailer)));
    }
    return c;
  });

  // ---- Report --------------------------------------------------------------
  const hidden = final.filter((c) => !c.isClothing).length;
  const genderDist: Record<string, number> = {};
  const ptDist: Record<string, number> = {};
  const beforeGender: Record<string, number> = {};
  rows.forEach((r, i) => {
    genderDist[final[i].gender] = (genderDist[final[i].gender] ?? 0) + 1;
    ptDist[final[i].productType] = (ptDist[final[i].productType] ?? 0) + 1;
    const bg = normalizeGender(r.gender) ?? (r.gender ? "other:" + r.gender : "null");
    beforeGender[bg] = (beforeGender[bg] ?? 0) + 1;
  });

  console.log(`isClothing=false (to hide): ${hidden} / ${rows.length} = ${(100 * hidden / rows.length).toFixed(1)}%`);
  console.log(`\nGENDER before (normalized): ${JSON.stringify(beforeGender)}`);
  console.log(`GENDER after:               ${JSON.stringify(genderDist)}`);
  console.log(`PRODUCTTYPE after:          ${JSON.stringify(ptDist)}`);
  console.log(`\nRetailer gender defaults applied (${retailerDefault.size}):`);
  console.log("  " + [...retailerDefault.entries()].map(([k, v]) => `${k}=${v}`).join(", "));

  // Precision spot-check: sample of HIDDEN items (must be true non-wearables).
  const hiddenRows = rows.map((r, i) => ({ r, c: final[i] })).filter((x) => !x.c.isClothing);
  console.log(`\n=== ${Math.min(SAMPLE, hiddenRows.length)} sample HIDDEN items (verify none are real clothing) ===`);
  for (const { r, c } of shuffle(hiddenRows).slice(0, SAMPLE)) {
    console.log(`  [${(r.category || "").slice(0, 24).padEnd(24)}] ${(r.name || "").slice(0, 52).padEnd(52)} ${c.confidence}`);
  }

  // Low-confidence kept items (review candidates).
  const lowConf = rows.map((r, i) => ({ r, c: final[i] })).filter((x) => x.c.isClothing && x.c.confidence < 0.4);
  console.log(`\nKept-but-low-confidence (<0.4): ${lowConf.length}`);

  if (!APPLY) {
    console.log("\nDRY-RUN — no rows written. Re-run with --apply to persist.");
    await pool.end();
    return;
  }

  // ---- Apply (batched) -----------------------------------------------------
  console.log("\nApplying updates…");
  const CHUNK = 500;
  let done = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);
    const ids = slice.map((r) => r.id);
    const isCl = slice.map((_, j) => final[i + j].isClothing);
    const gen = slice.map((_, j) => final[i + j].gender);
    const pt = slice.map((_, j) => final[i + j].productType);
    const conf = slice.map((_, j) => final[i + j].confidence);
    await pool.query(
      `UPDATE "ClothingItem" AS ci SET
         "isClothing" = u.is_clothing,
         "gender" = u.gender,
         "productType" = u.product_type,
         "classificationConfidence" = u.conf,
         "classifiedAt" = NOW()
       FROM unnest($1::uuid[], $2::boolean[], $3::text[], $4::text[], $5::float8[])
            AS u(id, is_clothing, gender, product_type, conf)
       WHERE ci.id = u.id`,
      [ids, isCl, gen, pt, conf],
    );
    done += slice.length;
    if (done % 2000 === 0 || done === rows.length) console.log(`  ${done}/${rows.length}`);
  }
  console.log(`✅ Applied ${rows.length} classifications (hidden ${hidden}).`);
  await pool.end();
}

function shuffle<T>(arr: T[]): T[] {
  // deterministic-ish shuffle without Math.random (banned in some contexts);
  // here in a plain script it's fine, but keep it simple & stable.
  return [...arr].sort((a, b) => String(a).localeCompare(String(b)));
}

main().catch((e) => {
  console.error("ERR", e);
  process.exit(1);
});
