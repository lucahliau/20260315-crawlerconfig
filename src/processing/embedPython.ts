/**
 * Resolve a Python interpreter that can actually run embed_worker.py.
 *
 * Why this exists: the home M1's bgremover venv got rebuilt on Homebrew
 * python@3.14, where torch / sentence-transformers abort during interpreter
 * shutdown (Python 3.14's multiprocessing resource_tracker teardown) →
 * `embed_worker exited -1`, ~0 embeddings while nobg kept draining. There is no
 * SSH to the M1, so the fix has to ride the auto-update: the worker now refuses
 * a >=3.14 interpreter, prefers a managed 3.11–3.13 venv, and — if none exists —
 * builds one itself, once, idempotently. The legacy <BG>/venv path is still
 * accepted when it is a healthy <3.14 venv, so the M4 / any good box is
 * unaffected and never triggers a build.
 *
 * The version check is the real fix: torch IMPORTS fine on 3.14 (the warmup
 * encode even succeeds) — the crash is in multiprocessing teardown — so an
 * import smoke test alone would not catch it. We gate on the interpreter
 * version first, then import-probe for completeness.
 */
import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const BGREMOVER_DIR =
  process.env.BGREMOVER_DIR ?? "/Users/lucaliautaud/Desktop/20260315 bgremoverimages";

/**
 * Managed embed venv location. Deliberately OUTSIDE both git repos so a build
 * never pollutes a working tree; embed_worker.py writes its progress relative to
 * its OWN dir (Path(__file__)), not the interpreter's, and the bridge spawns it
 * with cwd=BGREMOVER_DIR — so the venv may live anywhere. Override with
 * PROCESS_EMBED_VENV.
 */
const MANAGED_VENV = process.env.PROCESS_EMBED_VENV
  ? path.resolve(process.env.PROCESS_EMBED_VENV)
  : path.join(os.homedir(), ".clothedd", "venv-embed");

const BUILD_MARKER = path.join(path.dirname(MANAGED_VENV), ".venv-embed-build.json");

/** Don't pip-storm a failing build: after a failed build, wait this long before
 *  trying again from inside a job (a manual scripts/setup-embed-venv.ts ignores
 *  the cooldown). */
const BUILD_FAILURE_COOLDOWN_MS = Math.max(
  10 * 60_000,
  parseInt(process.env.PROCESS_EMBED_BUILD_COOLDOWN_MS ?? String(90 * 60_000), 10),
);

/** A cold `import torch` can take tens of seconds on a fanless M1. */
const PROBE_TIMEOUT_MS = 120_000;

/** torch's macOS arm64 wheel is ~500MB — allow a generous build ceiling. */
const BUILD_TIMEOUT_MS = Math.max(
  10 * 60_000,
  parseInt(process.env.PROCESS_EMBED_BUILD_TIMEOUT_MS ?? String(35 * 60_000), 10),
);

/** Memoized resolved interpreter (per worker process), with a TTL so an env
 *  change or a freshly-built venv is picked up without a restart. */
const MEMO_TTL_MS = 60 * 60_000;

/**
 * Packages mirror bgremover's proven `embed:setup` (which works on the M4's 3.13
 * venv). numpy is explicit because embed_worker imports it directly. Overridable
 * so a torch flavor can be tuned without a code change.
 */
const PIP_PACKAGES = (
  process.env.PROCESS_EMBED_PIP_PACKAGES ??
  "sentence-transformers psycopg2-binary pgvector pillow requests torch numpy"
)
  .split(/\s+/)
  .filter(Boolean);

/** The exact set embed_worker.py imports at runtime — the completeness probe. */
const IMPORT_PROBE =
  "import torch, sentence_transformers, pgvector.psycopg2, psycopg2, PIL, requests, numpy";

export class EmbedPythonUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EmbedPythonUnavailable";
  }
}

type Logger = (line: string) => void;

let memo: { python: string; at: number } | null = null;

interface RunResult {
  code: number;
  stdout: string;
  stderr: string;
}

function run(cmd: string, args: string[], timeoutMs: number): Promise<RunResult> {
  return new Promise((resolve) => {
    execFile(
      cmd,
      args,
      { timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024 },
      (err, stdout, stderr) => {
        // execFile's err.code is the numeric exit code on a non-zero exit; on a
        // timeout it is killed/SIGTERM (code undefined); on ENOENT it is a string
        // ("ENOENT"). Anything that isn't a clean numeric 0 counts as failure.
        const code =
          err && typeof (err as { code?: unknown }).code === "number"
            ? ((err as { code: number }).code)
            : err
              ? 1
              : 0;
        resolve({ code, stdout: stdout ?? "", stderr: stderr ?? "" });
      },
    );
  });
}

/** Major.minor of an interpreter, or null if it can't run at all. */
async function pyVersion(py: string): Promise<[number, number] | null> {
  const r = await run(py, ["-c", "import sys;print('%d.%d' % sys.version_info[:2])"], 20_000);
  if (r.code !== 0) return null;
  const m = r.stdout.trim().match(/(\d+)\.(\d+)/);
  return m ? [Number(m[1]), Number(m[2])] : null;
}

/** Accept CPython 3.9–3.13. Reject 3.14+ (torch aborts in multiprocessing
 *  teardown) and anything older than 3.9. */
function versionOk(v: [number, number] | null): boolean {
  if (!v) return false;
  const [maj, min] = v;
  return maj === 3 && min >= 9 && min <= 13;
}

function lastLine(s: string): string {
  const lines = s.trim().split("\n");
  return lines[lines.length - 1] ?? "";
}

/** Can this interpreter actually run embed_worker.py? Version gate first (the
 *  real fix), then an import probe to catch a half-installed venv. */
async function isUsable(py: string, log: Logger): Promise<boolean> {
  if (!py) return false;
  try {
    if (!fs.existsSync(py)) return false;
  } catch {
    return false;
  }
  const v = await pyVersion(py);
  if (!versionOk(v)) {
    if (v) {
      log(
        `[embed-python] reject ${py} — Python ${v[0]}.${v[1]} (need 3.9–3.13; ` +
          `3.14 aborts torch in multiprocessing teardown → embed exit -1)`,
      );
    }
    return false;
  }
  const r = await run(py, ["-c", IMPORT_PROBE], PROBE_TIMEOUT_MS);
  if (r.code !== 0) {
    log(`[embed-python] reject ${py} — import probe failed: ${lastLine(r.stderr || r.stdout)}`);
    return false;
  }
  log(`[embed-python] usable interpreter: ${py} (Python ${v![0]}.${v![1]})`);
  return true;
}

/** Find a base interpreter (3.11–3.13) to build the managed venv from. The home
 *  M1 runs the auto-updater under launchd with a minimal PATH, so we probe
 *  ABSOLUTE Homebrew/anaconda paths (and `brew --prefix`) rather than relying on
 *  a bare `python3.13` resolving. */
async function findBasePython(log: Logger): Promise<string | null> {
  const candidates: string[] = [];
  if (process.env.EMBED_BASE_PYTHON) candidates.push(process.env.EMBED_BASE_PYTHON);

  // Homebrew versioned formulae (Apple-Silicon + Intel prefixes), opt + bin.
  for (const v of ["3.13", "3.12", "3.11"]) {
    candidates.push(`/opt/homebrew/opt/python@${v}/bin/python${v}`);
    candidates.push(`/opt/homebrew/bin/python${v}`);
    candidates.push(`/usr/local/opt/python@${v}/bin/python${v}`);
    candidates.push(`/usr/local/bin/python${v}`);
  }

  // brew --prefix (covers nonstandard install prefixes) via brew's known path.
  for (const brew of ["/opt/homebrew/bin/brew", "/usr/local/bin/brew"]) {
    if (!fs.existsSync(brew)) continue;
    for (const v of ["3.13", "3.12", "3.11"]) {
      const r = await run(brew, ["--prefix", `python@${v}`], 15_000);
      if (r.code === 0 && r.stdout.trim()) {
        candidates.push(path.join(r.stdout.trim(), "bin", `python${v}`));
      }
    }
  }

  // Anaconda (the M4 has 3.13 here) + bare PATH names as a last resort. The
  // version gate below rejects the M1's default python3 (3.14).
  candidates.push("/opt/anaconda3/bin/python3");
  candidates.push("python3.13", "python3.12", "python3.11", "python3");

  const seen = new Set<string>();
  for (const c of candidates) {
    if (!c || seen.has(c)) continue;
    seen.add(c);
    const v = await pyVersion(c);
    if (versionOk(v)) {
      log(`[embed-python] base interpreter for venv build: ${c} (Python ${v![0]}.${v![1]})`);
      return c;
    }
  }
  return null;
}

interface BuildMarker {
  outcome?: "ok" | "failed";
  detail?: string;
  at?: string;
}

function writeMarker(outcome: "ok" | "failed", detail: string): void {
  try {
    fs.mkdirSync(path.dirname(BUILD_MARKER), { recursive: true });
    fs.writeFileSync(
      BUILD_MARKER,
      JSON.stringify({ outcome, detail: detail.slice(0, 1000), at: new Date().toISOString() }),
    );
  } catch {
    // best-effort
  }
}

function readMarker(): BuildMarker | null {
  try {
    return JSON.parse(fs.readFileSync(BUILD_MARKER, "utf8")) as BuildMarker;
  } catch {
    return null;
  }
}

/** ms since the last FAILED build if still within cooldown, else null. */
function recentBuildFailure(): number | null {
  const m = readMarker();
  if (!m || m.outcome !== "failed" || !m.at) return null;
  const since = Date.now() - new Date(m.at).getTime();
  return since >= 0 && since < BUILD_FAILURE_COOLDOWN_MS ? since : null;
}

/**
 * Build the managed embed venv from a probed 3.11–3.13 base into a .tmp dir,
 * smoke-test it, then atomically promote it. Renaming is safe: we only ever
 * invoke `<venv>/bin/python`, whose site-packages are resolved from its runtime
 * path, not the path baked at build time.
 */
async function buildManagedVenv(log: Logger): Promise<string> {
  const base = await findBasePython(log);
  if (!base) {
    const msg =
      "Cannot build embed venv: no Python 3.11–3.13 base found. On the M1 run " +
      "`brew install python@3.13`, or set EMBED_BASE_PYTHON=/abs/path/to/python3.13.";
    writeMarker("failed", "no base 3.11–3.13 interpreter found");
    throw new EmbedPythonUnavailable(msg);
  }

  const parent = path.dirname(MANAGED_VENV);
  fs.mkdirSync(parent, { recursive: true });
  const tmp = `${MANAGED_VENV}.tmp`;
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {
    // ignore
  }

  log(
    `[embed-python] building managed venv at ${MANAGED_VENV} from ${base} ` +
      `(can take 15–25min: torch wheel ≈500MB)…`,
  );

  let step = await run(base, ["-m", "venv", tmp], 120_000);
  if (step.code !== 0) {
    const d = (step.stderr || step.stdout).trim().slice(-400);
    writeMarker("failed", `venv create failed: ${d}`);
    throw new EmbedPythonUnavailable(`venv create failed (${base}): ${d}`);
  }

  const tmpPy = path.join(tmp, "bin", "python");
  log("[embed-python] upgrading pip/wheel…");
  await run(tmpPy, ["-m", "pip", "install", "-U", "pip", "wheel"], 5 * 60_000);

  log(`[embed-python] pip install ${PIP_PACKAGES.join(" ")} …`);
  step = await run(tmpPy, ["-m", "pip", "install", ...PIP_PACKAGES], BUILD_TIMEOUT_MS);
  if (step.code !== 0) {
    const d = (step.stderr || step.stdout).trim().slice(-600);
    writeMarker("failed", `pip install failed: ${d}`);
    throw new EmbedPythonUnavailable(`pip install failed: ${d}`);
  }

  // Smoke-test before promoting: imports + a tiny op, and report MPS so a CPU
  // fallback is visible (embed_worker falls back to CPU on its own).
  const probe = await run(
    tmpPy,
    [
      "-c",
      `${IMPORT_PROBE}; import torch; print('mps', torch.backends.mps.is_available())`,
    ],
    PROBE_TIMEOUT_MS,
  );
  if (probe.code !== 0) {
    const d = (probe.stderr || probe.stdout).trim().slice(-600);
    writeMarker("failed", `smoke test failed: ${d}`);
    throw new EmbedPythonUnavailable(`built venv failed smoke test: ${d}`);
  }

  // Promote: remove any old venv, then rename tmp into place.
  try {
    fs.rmSync(MANAGED_VENV, { recursive: true, force: true });
  } catch {
    // ignore
  }
  fs.renameSync(tmp, MANAGED_VENV);
  writeMarker("ok", `built from ${base}; ${probe.stdout.trim()}`);
  const finalPy = path.join(MANAGED_VENV, "bin", "python");
  log(`[embed-python] managed venv ready: ${finalPy} (${probe.stdout.trim()})`);
  return finalPy;
}

/**
 * Return an absolute path to a Python that can run embed_worker.py.
 *
 * Order: PROCESS_EMBED_PYTHON → managed venv (~/.clothedd/venv-embed) → legacy
 * <BG>/venv. If none is usable and `allowBuild`, build the managed venv from a
 * 3.11–3.13 base (cooldown-guarded). `forceRebuild` always rebuilds.
 *
 * Throws EmbedPythonUnavailable when no interpreter can be found or built; the
 * worker surfaces that as EMBED_WORKER_CRASH so it's diagnosable off the M1.
 */
export async function resolveEmbedPython(opts: {
  log: Logger;
  allowBuild?: boolean;
  forceRebuild?: boolean;
}): Promise<string> {
  const { log } = opts;
  const allowBuild = opts.allowBuild ?? true;

  if (opts.forceRebuild) {
    memo = null;
    const built = await buildManagedVenv(log);
    memo = { python: built, at: Date.now() };
    return built;
  }

  if (memo && Date.now() - memo.at < MEMO_TTL_MS) {
    try {
      if (fs.existsSync(memo.python)) return memo.python;
    } catch {
      // fall through to re-resolve
    }
  }

  const candidates = [
    process.env.PROCESS_EMBED_PYTHON,
    path.join(MANAGED_VENV, "bin", "python"),
    path.join(BGREMOVER_DIR, "venv", "bin", "python"),
  ].filter((p): p is string => !!p);

  for (const py of candidates) {
    if (await isUsable(py, log)) {
      memo = { python: py, at: Date.now() };
      return py;
    }
  }

  if (!allowBuild) {
    throw new EmbedPythonUnavailable(
      `No usable embed Python (checked: ${candidates.join(", ")}). ` +
        "Run `npx tsx scripts/setup-embed-venv.ts` to build one.",
    );
  }

  const cooling = recentBuildFailure();
  if (cooling !== null) {
    throw new EmbedPythonUnavailable(
      `No usable embed Python; a venv build failed ${Math.round(cooling / 60_000)}min ago ` +
        `(cooldown ${Math.round(BUILD_FAILURE_COOLDOWN_MS / 60_000)}min). ` +
        `Last error: ${readMarker()?.detail ?? "?"}. ` +
        "Fix the base interpreter (brew install python@3.13) then run scripts/setup-embed-venv.ts.",
    );
  }

  const built = await buildManagedVenv(log);
  memo = { python: built, at: Date.now() };
  return built;
}
