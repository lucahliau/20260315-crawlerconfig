/**
 * Eagerly (re)build the managed embed venv on a 3.11–3.13 base, so the first
 * embed job after a deploy finds it ready instead of building mid-job.
 *
 *   npx tsx scripts/setup-embed-venv.ts            # build only if none usable
 *   npx tsx scripts/setup-embed-venv.ts --rebuild  # force a clean rebuild
 *
 * On the home M1 this is the manual fix for the Python-3.14 venv: it builds
 * ~/.clothedd/venv-embed (override PROCESS_EMBED_VENV) from a probed 3.11–3.13
 * interpreter and smoke-tests it. Anywhere with a healthy <3.14 bgremover venv
 * (e.g. the M4) it just reports that one and builds nothing.
 *
 * Requires a 3.11–3.13 base on the box. If none is found it prints exactly what
 * to install (`brew install python@3.13`) or set (EMBED_BASE_PYTHON).
 */
import "dotenv/config";

import { resolveEmbedPython } from "../src/processing/embedPython.js";

async function main(): Promise<void> {
  const force = process.argv.includes("--rebuild") || process.argv.includes("--force");
  try {
    const py = await resolveEmbedPython({
      log: (line) => console.log(line),
      allowBuild: true,
      forceRebuild: force,
    });
    console.log(`\n✓ embed Python ready: ${py}`);
    process.exit(0);
  } catch (err) {
    console.error(`\n✗ ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  }
}

void main();
