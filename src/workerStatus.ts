/**
 * Local status dashboard for the home worker — a single page on
 * http://127.0.0.1:4577 (env WORKER_STATUS_PORT; 0 disables) showing worker
 * activity, queue depth, processing backlog, Mac health (load, memory,
 * thermal throttling, power source), and cloud reachability. Issues are kept
 * in a ring buffer AND appended to ~/Library/Logs/crawler-worker-issues.log.
 *
 * Binds to localhost only — never exposed to the network, safe to leave on
 * everywhere (incl. a Railway worker, where it is simply unreachable).
 */
import http from "node:http";
import os from "node:os";
import fs from "node:fs";
import path from "node:path";
import { execFile, spawn } from "node:child_process";

import { getQueueStats } from "./queue.js";
import { getProcessingBacklog } from "./processing/bridge.js";

const RING_MAX = 250;
const ISSUES_MAX = 100;
const COMPLETED_MAX = 50;

interface JobInfo {
  id: string;
  kind: string;
  startedAt: string;
  detail?: string;
}

const state = {
  startedAt: Date.now(),
  workerId: "",
  queues: [] as string[],
  concurrency: 0,
  activeJobs: new Map<string, JobInfo>(),
  completed: [] as { at: string; kind: string; id: string; summary: string }[],
  issues: [] as { at: string; source: string; message: string }[],
  activity: [] as { at: string; line: string }[],
};

const issuesFile = path.join(os.homedir(), "Library", "Logs", "crawler-worker-issues.log");

/**
 * Optional durable sink for issues — wired by the worker to mirror every issue
 * into Postgres (worker_issues) so they're visible on the cloud dashboard, not
 * just here on localhost. Kept as an injected callback so this module stays
 * dependency-light (no DB import) and usable in contexts without a database.
 */
type IssueSink = (issue: { workerId: string; source: string; message: string }) => void;
let issueSink: IssueSink | null = null;
export function setIssueSink(sink: IssueSink | null): void {
  issueSink = sink;
}

export function statusJobStarted(id: string, kind: string, detail?: string): void {
  state.activeJobs.set(id, { id, kind, startedAt: new Date().toISOString(), detail });
}

export function statusJobFinished(id: string, kind: string, summary: string): void {
  state.activeJobs.delete(id);
  state.completed.unshift({ at: new Date().toISOString(), kind, id, summary });
  if (state.completed.length > COMPLETED_MAX) state.completed.pop();
}

export function recordActivity(line: string): void {
  state.activity.unshift({ at: new Date().toISOString(), line: line.slice(0, 500) });
  if (state.activity.length > RING_MAX) state.activity.pop();
}

/** Ring buffer + durable file — anything that needs eyes lands here. */
export function recordIssue(source: string, message: string): void {
  const at = new Date().toISOString();
  state.issues.unshift({ at, source, message: message.slice(0, 1000) });
  if (state.issues.length > ISSUES_MAX) state.issues.pop();
  try {
    fs.mkdirSync(path.dirname(issuesFile), { recursive: true });
    fs.appendFileSync(issuesFile, `${at} [${source}] ${message.replace(/\n/g, " | ")}\n`);
  } catch {
    /* logging must never break the worker */
  }
  // Mirror to the durable sink (cloud DB) when wired. Fire-and-forget — the sink
  // itself must never throw (it swallows its own errors), but guard anyway so a
  // logging path can never break the worker.
  if (issueSink) {
    try {
      issueSink({ workerId: state.workerId, source, message });
    } catch {
      /* never break the worker on a logging mirror */
    }
  }
}

/**
 * Keep the Mac awake while the worker lives — caffeinate needs no sudo and
 * exits with us (-w pid). Prevents idle/system sleep on AC power; lid-closed
 * operation additionally needs the pmset step from setup.sh.
 */
export function startCaffeinate(): void {
  if (process.platform !== "darwin") return;
  try {
    const child = spawn("caffeinate", ["-ims", "-w", String(process.pid)], {
      stdio: "ignore",
    });
    child.on("error", () => {});
    child.unref();
    recordActivity("caffeinate started — Mac will not idle-sleep while the worker runs");
  } catch {
    /* best effort */
  }
}

function execOut(cmd: string, args: string[]): Promise<string> {
  return new Promise((resolve) => {
    try {
      execFile(cmd, args, { timeout: 4000 }, (_err, stdout) => resolve(stdout ?? ""));
    } catch {
      resolve("");
    }
  });
}

export interface MachineTelemetry {
  loadAvg1m: number;
  cpuCount: number;
  /**
   * Real CPU utilisation 0–100 (busy %), averaged across all cores over the
   * interval since the previous telemetry sample (~10–15s). Computed from
   * os.cpus() time deltas — NOT load average. This is the number that matches
   * Activity Monitor's "% used" (≈ 100 − %idle). null on the very first sample
   * (no prior baseline to diff against). Load average alone made the machine
   * look busier than it was; this exposes the true headroom.
   */
  cpuUsagePct: number | null;
  /**
   * AVAILABLE memory in MB — what people mean by "free". On macOS this is NOT
   * os.freemem(): the OS keeps almost all RAM occupied with reclaimable file
   * cache (inactive/speculative/purgeable pages), so os.freemem() reads ~1% on
   * an idle machine and falsely looks alarming. We compute free + inactive +
   * speculative + purgeable from vm_stat instead, matching Activity Monitor's
   * low memory pressure. Off darwin, this is os.freemem().
   */
  freeMemMb: number;
  totalMemMb: number;
  diskFreeGb: number | null;
  cpuSpeedLimitPct: number | null;
  onACPower: boolean | null;
  batteryPct: number | null;
}

/**
 * Rolling CPU-utilisation sampler. os.cpus() returns cumulative time counters
 * per core; busy% over an interval = 1 − Δidle/Δtotal. We keep the previous
 * cumulative snapshot module-side and diff on each telemetry refresh (~10–15s),
 * which is exactly the window the dashboard cares about.
 */
let prevCpuSample: { idle: number; total: number } | null = null;
function sampleCpuTimes(): { idle: number; total: number } {
  let idle = 0;
  let total = 0;
  for (const c of os.cpus()) {
    const tmes = c.times;
    idle += tmes.idle;
    total += tmes.user + tmes.nice + tmes.sys + tmes.idle + tmes.irq;
  }
  return { idle, total };
}
function cpuUsagePctSince(): number | null {
  const cur = sampleCpuTimes();
  const prev = prevCpuSample;
  prevCpuSample = cur;
  if (!prev) return null;
  const idleDelta = cur.idle - prev.idle;
  const totalDelta = cur.total - prev.total;
  if (totalDelta <= 0) return null;
  return Math.max(0, Math.min(100, Math.round((1 - idleDelta / totalDelta) * 100)));
}

/**
 * Reclaimable-aware "available" memory in MB from `vm_stat`. macOS counts only
 * fully-unused pages as free; the large inactive/speculative/purgeable pools
 * are reclaimed on demand and should count as available. Falls back to
 * os.freemem() off darwin or if parsing fails.
 */
function parseAvailableMemMb(vmStat: string): number {
  const freemem = Math.round(os.freemem() / 1024 / 1024);
  if (!vmStat) return freemem;
  const pageSize = Number(vmStat.match(/page size of (\d+) bytes/)?.[1] ?? 0);
  const pages = (label: string) =>
    Number(vmStat.match(new RegExp(`Pages ${label}:\\s+(\\d+)`))?.[1] ?? 0);
  const availablePages =
    pages("free") + pages("inactive") + pages("speculative") + pages("purgeable");
  if (!pageSize || !availablePages) return freemem;
  return Math.round((availablePages * pageSize) / 1024 / 1024);
}

let machineCache: { at: number; value: MachineTelemetry } | null = null;

/** Mac health snapshot, cached 10s (also shipped in the worker heartbeat). */
export async function getMachineTelemetry(): Promise<MachineTelemetry> {
  if (machineCache && Date.now() - machineCache.at < 10_000) return machineCache.value;
  const [therm, batt, df, vmstat] = await Promise.all([
    process.platform === "darwin" ? execOut("pmset", ["-g", "therm"]) : Promise.resolve(""),
    process.platform === "darwin" ? execOut("pmset", ["-g", "batt"]) : Promise.resolve(""),
    execOut("df", ["-k", os.homedir()]),
    process.platform === "darwin" ? execOut("vm_stat", []) : Promise.resolve(""),
  ]);
  const speedMatch = therm.match(/CPU_Speed_Limit\s*=\s*(\d+)/);
  const battPctMatch = batt.match(/(\d+)%/);
  // df -k: header line, then "<fs> <1k-blocks> <used> <avail> ..."
  const dfFields = df.split("\n")[1]?.trim().split(/\s+/) ?? [];
  const availKb = Number(dfFields[3]);
  const value: MachineTelemetry = {
    loadAvg1m: Math.round(os.loadavg()[0] * 100) / 100,
    cpuCount: os.cpus().length,
    cpuUsagePct: cpuUsagePctSince(),
    freeMemMb:
      process.platform === "darwin" ? parseAvailableMemMb(vmstat) : Math.round(os.freemem() / 1024 / 1024),
    totalMemMb: Math.round(os.totalmem() / 1024 / 1024),
    diskFreeGb: Number.isFinite(availKb) ? Math.round(availKb / 1024 / 1024) : null,
    cpuSpeedLimitPct: speedMatch ? Number(speedMatch[1]) : null,
    onACPower: batt ? batt.includes("AC Power") : null,
    batteryPct: battPctMatch ? Number(battPctMatch[1]) : null,
  };
  machineCache = { at: Date.now(), value };
  return value;
}

/**
 * Everything the cloud dashboard needs to show the home server's health —
 * rides the worker heartbeat into worker_heartbeats.metadata every 15s.
 */
export async function getHeartbeatTelemetry(): Promise<Record<string, unknown>> {
  const [machine, version] = await Promise.all([
    getMachineTelemetry(),
    versionStats().catch(() => ({ commit: null, updateAvailable: false })),
  ]);
  return {
    ...machine,
    ...version,
    activeJobs: [...state.activeJobs.values()].map((j) => `${j.kind}${j.detail ? ` (${j.detail})` : ""}`),
    recentIssues: state.issues.length,
  };
}

let versionCache: { at: number; value: Record<string, unknown> } | null = null;
/** Local commit vs origin HEAD — surfaces "update available" on the page. */
async function versionStats(): Promise<Record<string, unknown>> {
  if (versionCache && Date.now() - versionCache.at < 60 * 60_000) return versionCache.value;
  const local = (await execOut("git", ["rev-parse", "HEAD"])).trim();
  const remoteOut = await execOut("git", ["ls-remote", "origin", "HEAD"]);
  const remote = remoteOut.split(/\s+/)[0]?.trim() || null;
  const value = {
    commit: local ? local.slice(0, 7) : null,
    updateAvailable: Boolean(local && remote && local !== remote),
  };
  versionCache = { at: Date.now(), value };
  return value;
}

/**
 * Unattended boxes fill logs forever — cap them. The launchd redirect fd is
 * O_APPEND, so truncation is safe (appends continue at the new EOF).
 */
function houseKeepLogs(): void {
  const workerLog = path.join(os.homedir(), "Library", "Logs", "crawler-worker.log");
  try {
    if (fs.existsSync(workerLog) && fs.statSync(workerLog).size > 100 * 1024 * 1024) {
      fs.truncateSync(workerLog, 0);
      recordActivity("worker log exceeded 100MB — truncated");
    }
  } catch {
    /* best effort */
  }
  try {
    if (fs.existsSync(issuesFile) && fs.statSync(issuesFile).size > 5 * 1024 * 1024) {
      const lines = fs.readFileSync(issuesFile, "utf8").split("\n");
      fs.writeFileSync(issuesFile, lines.slice(-2000).join("\n"));
    }
  } catch {
    /* best effort */
  }
}

let cloudCache: { at: number; value: Record<string, unknown> } | null = null;
async function cloudStats(): Promise<Record<string, unknown>> {
  if (cloudCache && Date.now() - cloudCache.at < 30_000) return cloudCache.value;
  const probe = async (label: string, url: string) => {
    const t0 = Date.now();
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 6000);
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timer);
      // Any HTTP response (even 401 from the password gate) proves reachability.
      return { label, ok: res.status < 500, status: res.status, latencyMs: Date.now() - t0 };
    } catch (err) {
      return {
        label,
        ok: false,
        latencyMs: Date.now() - t0,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  };
  const [crawler, backend] = await Promise.all([
    probe(
      "crawler dashboard",
      process.env.CRAWLER_PUBLIC_URL ?? "https://20260315-crawlerconfig-production.up.railway.app/login",
    ),
    probe(
      "app backend",
      process.env.BACKEND_HEALTH_URL ??
        "https://20260311-clothes-backend-production.up.railway.app/health",
    ),
  ]);
  const value = { crawler, backend };
  cloudCache = { at: Date.now(), value };
  return value;
}

async function buildStatus(): Promise<Record<string, unknown>> {
  const [queues, backlog, machine, cloud, version] = await Promise.all([
    getQueueStats().catch(() => []),
    getProcessingBacklog().catch(() => null),
    getMachineTelemetry(),
    cloudStats(),
    versionStats().catch(() => ({ commit: null, updateAvailable: false })),
  ]);
  return {
    worker: {
      id: state.workerId,
      queues: state.queues,
      concurrency: state.concurrency,
      pid: process.pid,
      uptimeSeconds: Math.round((Date.now() - state.startedAt) / 1000),
    },
    activeJobs: [...state.activeJobs.values()],
    recentCompleted: state.completed,
    issues: state.issues,
    activity: state.activity.slice(0, 80),
    queues,
    backlog,
    machine,
    cloud,
    version,
    issuesFile,
    generatedAt: new Date().toISOString(),
  };
}

const PAGE = `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Clothedd home worker</title>
<style>
body{background:#030712;color:#e5e7eb;font:13px/1.5 ui-monospace,Menlo,monospace;margin:0;padding:18px;max-width:880px}
h1{font-size:15px;margin:0 0 2px} .sub{color:#6b7280;font-size:11px;margin-bottom:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;margin-bottom:14px}
.card{border:1px solid #1f2937;border-radius:10px;padding:10px;background:#0b1120}
.k{color:#6b7280;font-size:11px}.v{font-size:15px;margin-top:2px}
.ok{color:#34d399}.bad{color:#f87171}.warn{color:#fbbf24}
h2{font-size:12px;color:#9ca3af;margin:16px 0 6px;text-transform:uppercase;letter-spacing:.04em}
table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:3px 8px 3px 0;font-size:12px}
th{color:#6b7280;font-weight:normal}
.log{border:1px solid #1f2937;border-radius:10px;background:#0b1120;padding:10px;max-height:260px;overflow:auto;font-size:11px;color:#9ca3af;white-space:pre-wrap;word-break:break-word}
.issue{color:#f87171}
.muted{color:#4b5563}
</style></head><body>
<h1>Clothedd home worker</h1>
<div class="sub" id="sub">connecting…</div>
<div class="grid" id="cards"></div>
<h2>Queues</h2><div id="queues"></div>
<h2>Working on</h2><div id="active" class="muted">—</div>
<h2>Issues <span class="muted" id="issuecount"></span></h2><div class="log" id="issues">none</div>
<h2>Recent activity</h2><div class="log" id="activity">—</div>
<script>
function esc(s){return String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]))}
function card(k,v,cls){return '<div class="card"><div class="k">'+k+'</div><div class="v '+(cls||"")+'">'+v+'</div></div>'}
function fmtUp(s){if(s<3600)return Math.floor(s/60)+"m";if(s<86400)return Math.floor(s/3600)+"h "+Math.floor(s%3600/60)+"m";return Math.floor(s/86400)+"d "+Math.floor(s%86400/3600)+"h"}
async function tick(){
 let d;try{d=await (await fetch("/status.json")).json()}catch(e){document.getElementById("sub").textContent="worker unreachable — is it running? ("+e.message+")";return}
 document.getElementById("sub").textContent=d.worker.id+" · pid "+d.worker.pid+" · up "+fmtUp(d.worker.uptimeSeconds)+" · queues: "+d.worker.queues.join(", ")+" · updated "+new Date(d.generatedAt).toLocaleTimeString();
 const m=d.machine,c=d.cloud,b=d.backlog;
 let cards="";
 cards+=card("worker","online","ok");
 cards+=card("cloud server",c.crawler.ok?("reachable · "+c.crawler.latencyMs+"ms"):"unreachable",c.crawler.ok?"ok":"bad");
 cards+=card("app backend",c.backend.ok?("healthy · "+c.backend.latencyMs+"ms"):"unreachable",c.backend.ok?"ok":"bad");
 if(b)cards+=card("backlog",b.needsNobg+" nobg · "+b.needsEmbed+" embed",(b.needsNobg+b.needsEmbed)>0?"warn":"ok");
 cards+=card("memory available",m.freeMemMb+" / "+m.totalMemMb+" MB"+(m.totalMemMb?" ("+Math.round(m.freeMemMb/m.totalMemMb*100)+"%)":""),m.totalMemMb&&m.freeMemMb<m.totalMemMb*0.1?"warn":"");
 if(m.cpuUsagePct!==null&&m.cpuUsagePct!==undefined)cards+=card("cpu used",m.cpuUsagePct+"% busy · "+(100-m.cpuUsagePct)+"% idle",m.cpuUsagePct>90?"warn":"ok");
 cards+=card("load (1m)",m.loadAvg1m+" / "+m.cpuCount+" cores",m.loadAvg1m>m.cpuCount?"warn":"");
 if(m.cpuSpeedLimitPct!==null)cards+=card("thermal",m.cpuSpeedLimitPct+"% speed"+(m.cpuSpeedLimitPct<100?" (throttling — normal under load)":""),m.cpuSpeedLimitPct<70?"warn":"");
 if(m.onACPower!==null)cards+=card("power",m.onACPower?"plugged in":"on battery — plug in!",m.onACPower?"ok":"bad");
 if(m.diskFreeGb!==null)cards+=card("disk free",m.diskFreeGb+" GB",m.diskFreeGb<15?"warn":"");
 if(d.version&&d.version.commit)cards+=card("code",d.version.commit+(d.version.updateAvailable?" — update available (run update.sh)":""),d.version.updateAvailable?"warn":"");
 document.getElementById("cards").innerHTML=cards;
 document.getElementById("queues").innerHTML="<table><tr><th>queue</th><th>waiting</th><th>active</th><th>failed</th></tr>"+d.queues.map(q=>"<tr><td>"+esc(q.name)+"</td><td>"+q.waiting+"</td><td>"+q.active+"</td><td class='"+(q.failed>0?"bad":"")+"'>"+q.failed+"</td></tr>").join("")+"</table>";
 document.getElementById("active").innerHTML=d.activeJobs.length?d.activeJobs.map(j=>esc(j.kind)+" job "+esc(j.id.slice(0,8))+"… since "+new Date(j.startedAt).toLocaleTimeString()+(j.detail?" — "+esc(j.detail):"")).join("<br>"):"idle — waiting for jobs";
 document.getElementById("issuecount").textContent=d.issues.length?("("+d.issues.length+" recent)"):"";
 document.getElementById("issues").innerHTML=d.issues.length?d.issues.map(i=>'<div class="issue">'+esc(i.at.slice(5,19).replace("T"," "))+" ["+esc(i.source)+"] "+esc(i.message)+"</div>").join(""):"none 🎉".replace(" 🎉","");
 document.getElementById("activity").innerHTML=d.activity.map(a=>'<div><span class="muted">'+esc(a.at.slice(11,19))+"</span> "+esc(a.line)+"</div>").join("")||"—";
}
tick();setInterval(tick,5000);
</script></body></html>`;

export function startWorkerStatusServer(info: {
  workerId: string;
  queues: string[];
  concurrency: number;
}): void {
  const port = parseInt(process.env.WORKER_STATUS_PORT ?? "4577", 10);
  if (!port) return;
  state.workerId = info.workerId;
  state.queues = info.queues;
  state.concurrency = info.concurrency;

  const server = http.createServer((req, res) => {
    if (req.url === "/status.json") {
      buildStatus()
        .then((status) => {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(status));
        })
        .catch((err) => {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: String(err) }));
        });
      return;
    }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(PAGE);
  });

  server.on("error", (err) => {
    recordIssue("status-server", `local dashboard failed to start: ${err.message}`);
  });
  // Localhost ONLY — this must never listen on a network interface.
  server.listen(port, "127.0.0.1", () => {
    console.log(`[worker] local status dashboard: http://localhost:${port}`);
  });

  houseKeepLogs();
  setInterval(houseKeepLogs, 6 * 60 * 60 * 1000).unref();
}
