# Home server (MacBook Air M1) — processing worker setup

The M1 runs `npm run worker` 24/7 against the shared Supabase Postgres. It
claims `upload-url` (product scraping → R2/DB), `process-nobg` (background
removal via rembg), and `process-embed` (CLIP embeddings) jobs that the
Railway dashboard — or the phone at `/swipe` — enqueues. No port-forwarding:
the worker polls the queue, so the M1 just needs outbound internet.

## 1. Prerequisites

- Node 22 (or 20+) with `npx tsx` working.
- This repo cloned (e.g. `~/crawlerconfig`) with `npm install` run.
- The **bgremoverimages** folder copied to the M1 (e.g.
  `~/Desktop/20260315 bgremoverimages`) with its tools set up:
  - `npm install` inside it (TypeScript bg-removal orchestrator)
  - `rembg` available (`pip3 install rembg` — first run downloads the U²-Net model)
  - embeddings venv: `npm run embed:setup` inside that folder (creates `./venv`
    with sentence-transformers; MPS-accelerated on Apple Silicon)

## 2. Worker .env (in the crawler repo on the M1)

```bash
DATABASE_URL=postgres://…            # same Supabase as Railway
R2_ACCOUNT_ID=…
R2_ACCESS_KEY_ID=…
R2_SECRET_ACCESS_KEY=…
R2_BUCKET_NAME=product-images
R2_PUBLIC_URL=https://pub-….r2.dev   # embed worker downloads -nobg.png from here
CONFIGS_DIR=./configs
WORKER_ID=m1-home
WORKER_CONCURRENCY=4                 # upload-url parallelism (processing is always 1 batch at a time)
WORKER_QUEUES=upload-url,process-nobg,process-embed
BGREMOVER_DIR=/Users/<you>/Desktop/20260315 bgremoverimages
# optional tuning:
# PROCESS_NOBG_PARALLEL=5            # rembg chains per batch
# PROCESS_NOBG_DEFAULT_LIMIT=100
# PROCESS_EMBED_DEFAULT_LIMIT=2000
```

The bridged tools inherit this env — they do NOT read their own `.env` when
spawned by the worker, so `R2_*`, `R2_PUBLIC_URL`, and `DATABASE_URL` must all
be present here.

## 3. launchd (keep-alive service)

`~/Library/LaunchAgents/com.clothedd.crawler-worker.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.clothedd.crawler-worker</string>
  <key>WorkingDirectory</key><string>/Users/YOU/crawlerconfig</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/caffeinate</string>
    <string>-dims</string>
    <string>/usr/local/bin/npm</string>
    <string>run</string>
    <string>worker</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/YOU/Library/Logs/crawler-worker.log</string>
  <key>StandardErrorPath</key><string>/Users/YOU/Library/Logs/crawler-worker.log</string>
</dict>
</plist>
```

- Adjust the npm path (`which npm` — often `/opt/homebrew/bin/npm` on Apple Silicon).
- `caffeinate -dims` prevents sleep while the worker runs; on AC power also set
  `sudo pmset -c sleep 0` so the lid can stay closed.
- Load: `launchctl load ~/Library/LaunchAgents/com.clothedd.crawler-worker.plist`
- Logs: `tail -f ~/Library/Logs/crawler-worker.log`

## 4. Verify

Within ~30s of starting, the dashboard **Process** tab (desktop `/app` or
phone `/swipe`) shows **“Home server: online”** — that's the worker heartbeat
(every 15s, stale after 120s; the heartbeat metadata lists its queues). Queue
a tiny batch from the phone (Process tab → "Remove backgrounds") and watch the
counts move.

## 5. Operational notes

- **Bounded batches, chained**: each processing job handles `limit` items and
  re-enqueues the next batch while backlog remains (chain stops when the
  Autopilot kill switch is off; queued-but-unclaimed jobs can be cancelled
  from the Process tab).
- **Embed exit 124** (MPS watchdog) is expected occasionally — pg-boss retries
  and the embed worker resumes from the DB (same semantics as `run_embed.sh`).
- **hasNobg sync**: the worker flips `ClothingItem.hasNobg` for exactly the
  keys each batch processed. Run the backend's reconciliation weekly to heal
  any drift from R2 truth:
  `npm --prefix "$HOME/Desktop/20260311 Clothes backend" run populate-has-nobg`
- **M1 offline?** Nothing breaks — jobs wait in Postgres; the Process tab
  shows "offline — queued jobs will wait".
- **Railway must not claim processing**: the default `WORKER_QUEUES` is
  `upload-url`, so any Railway worker service is unaffected. Only the M1 sets
  the full list. (If a Railway worker service exists, pin
  `WORKER_QUEUES=upload-url` on it explicitly.)
- **iCloud Desktop sync gotcha**: if the repo lives under Desktop/Documents,
  check for corrupted deps after sync: `find node_modules -maxdepth 4 -name "* 2" | wc -l`
  (>0 → `rm -rf node_modules && npm install`).

## 6. Schedules (registered by the Railway server, claimed by this worker)

- 01:00 nightly — background-removal sweep (limit 200/batch, chains until done)
- 04:00 nightly — embedding sweep (limit 5000)
- Mon 06:00 — weekly re-crawl of `recommended` retailers (server-side), whose
  uploads then fan out to this worker.

All sweeps respect the Autopilot kill switch (Pipeline tab on the phone).
