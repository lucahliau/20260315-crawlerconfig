#!/bin/bash
# ============================================================================
# One-shot recovery for the home M1 after the embed worker died on Python 3.14.
#
# The embed bridge now refuses a >=3.14 interpreter and uses/builds a managed
# 3.11-3.13 venv (~/.clothedd/venv-embed). This script lands that on the M1:
# it builds the venv, restarts the worker so it loads the new bridge, revives
# the auto-update LaunchAgent, and prints a remote-diagnosable status.
#
# Run it (paste these plain lines — no shell-special characters):
#   cd ~/Desktop/20260315\ crawlerconfig
#   git fetch origin main
#   git reset --hard origin/main
#   bash scripts/bootstrap-m1-embed-fix.sh
#
# Idempotent + safe to re-run.
# ============================================================================
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_PLIST="$HOME/Library/LaunchAgents/com.clothedd.crawler-worker.plist"
VENV_PY="${PROCESS_EMBED_VENV:-$HOME/.clothedd/venv-embed}/bin/python"

cd "$REPO" || { echo "repo not found: $REPO"; exit 1; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }

step "0. Keep this Mac awake for the session (the real fix is AC power + lid open)"
caffeinate -dimsu >/dev/null 2>&1 &
echo "caffeinate started (pid $!)"

step "1. Why was the auto-updater stuck? (read this before trusting auto-deploy)"
launchctl list | grep com.clothedd || echo ">> com.clothedd agents are NOT listed (never installed / not loaded)"
tail -20 "$HOME/Library/Logs/crawler-autoupdate.log" 2>/dev/null || echo "(no autoupdate log yet)"

step "2. Build the 3.11-3.13 embed venv (eager; torch is ~500MB, can take 15-25 min)"
if npx tsx scripts/setup-embed-venv.ts; then
  echo "embed venv build/resolve OK"
else
  echo ">> No usable 3.11-3.13 base interpreter was found."
  echo ">> Install one:   brew install python@3.13"
  echo ">> Then re-run:   bash scripts/bootstrap-m1-embed-fix.sh"
  exit 1
fi

step "3. Verify the embed venv is healthy"
if [ -x "$VENV_PY" ]; then
  "$VENV_PY" - <<'PY'
import sys, torch, sentence_transformers, pgvector.psycopg2, psycopg2, PIL, requests, numpy
assert sys.version_info[:2] <= (3, 13), f"unexpected interpreter version: {sys.version}"
print("OK", sys.version.split()[0], "| mps", torch.backends.mps.is_available())
PY
else
  echo "(a pre-existing healthy venv is in use; nothing to verify here)"
fi

step "4. Restart the worker so it loads the new bridge code"
if [ -f "$WORKER_PLIST" ]; then
  launchctl unload "$WORKER_PLIST" 2>/dev/null || true
  launchctl load "$WORKER_PLIST" && echo "worker reloaded" || echo "launchctl load warned"
else
  echo ">> worker plist not found at $WORKER_PLIST"
  echo ">> restart the worker however you normally start it (it must reload to pick up the fix)"
fi

step "5. Revive the auto-updater so future in-scope fixes land by themselves"
bash scripts/install-m1-autoupdate.sh || echo "install-m1-autoupdate warned"
DRY_RUN=1 bash scripts/auto-update.sh || true

step "6. Remote-diagnosable status (this command also works from your M4)"
npx tsx scripts/diagnose-remote.ts || true

step "Done"
echo "Expect: commit != 1487b1e, updateAvailable=false, ItemEmbedding embeddedAt last_24h climbing,"
echo "        and the needsEmbed backlog (~11795) draining. Keep the M1 on AC so it doesn't sleep."
