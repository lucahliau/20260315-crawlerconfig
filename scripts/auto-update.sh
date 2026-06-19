#!/bin/bash
# ============================================================================
# Full auto-updater for the Clothedd home (M1) worker.
#
# Run periodically by the com.clothedd.crawler-autoupdate LaunchAgent (every
# ~10 min). The M1 hosts nothing but this worker, so it ALWAYS hard-syncs the
# checkout to origin/main when behind — code lands on the M1 with no manual step,
# pushed straight from the M4. The allowlist (scripts/m1-autoupdate-allow.txt)
# now only decides whether to RESTART the worker: worker / processing / queue
# code (or a dependency change) restarts it; a pure cloud-server / dashboard /
# docs push just syncs the files and leaves a resumable in-flight job running.
# New Python deps self-install on first use (bridge ensurePipPackages), and the
# sibling bgremoverimages repo's *.py/*.ts/*.sh are synced too — so adding a new
# processing tool or dep needs only a git push, never an SSH/one-off on the M1.
#
# Idempotent + safe to run on a timer: a no-op when already up to date, and a
# restart mid-job is fine (pg-boss jobs are resumable).
#
# Manual one-off run / dry check:
#   bash scripts/auto-update.sh            # apply if in scope
#   DRY_RUN=1 bash scripts/auto-update.sh  # report only, never change code
# ============================================================================
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ALLOWFILE="$REPO/scripts/m1-autoupdate-allow.txt"
PLIST="$HOME/Library/LaunchAgents/com.clothedd.crawler-worker.plist"
LOG="$HOME/Library/Logs/crawler-autoupdate.log"
BRANCH="${AUTOUPDATE_BRANCH:-main}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$(dirname "$LOG")"
log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" | tee -a "$LOG"; }

# Best-effort durable breadcrumb → worker_issues, visible on the cloud Errors
# tab. Never fails the run.
crumb() { # crumb <message> <severity>
  ( cd "$REPO" && npx --yes tsx scripts/record-worker-issue.ts "autoupdate" "$1" "${2:-info}" ) \
    >/dev/null 2>&1 || true
}

cd "$REPO" || { log "repo not found at $REPO"; exit 0; }

# Is `path` matched by any allowlist pattern? Dir patterns end in "/".
in_scope() {
  local file="$1" pattern
  while IFS= read -r pattern; do
    pattern="${pattern%%#*}"                       # strip inline comments
    pattern="$(echo "$pattern" | xargs 2>/dev/null)" # trim whitespace
    [ -z "$pattern" ] && continue
    if [ "${pattern: -1}" = "/" ]; then
      case "$file" in "$pattern"*) return 0 ;; esac
    else
      [ "$file" = "$pattern" ] && return 0
    fi
  done < "$ALLOWFILE"
  return 1
}

# Keep the background-removal / embedding tools current too. They live in a
# SEPARATE repo (bgremoverimages) the worker shells out to per batch
# (`npx tsx remove-bg-parallel.ts`), so a code fix there takes effect on the
# NEXT batch with no worker restart. We checkout ONLY code (*.ts/*.py/*.sh) from
# origin and NEVER reset --hard: that repo tracks dirty runtime artifacts
# (embed-*.jsonl/.json/.log) and the worker writes untracked progress
# (history.jsonl, progress.json) that a hard reset would wipe → reprocessing.
update_bgremover() {
  local BG BR NEW MARK
  BG="${BGREMOVER_DIR:-$HOME/Desktop/20260315 bgremoverimages}"
  [ -d "$BG/.git" ] || return 0
  BR="$(git -C "$BG" rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)"
  git -C "$BG" fetch --quiet origin "$BR" 2>>"$LOG" || { log "bgremover fetch failed"; return 0; }
  NEW="$(git -C "$BG" rev-parse "origin/$BR" 2>/dev/null)"
  [ -z "$NEW" ] && return 0
  MARK="$HOME/Library/Logs/.bgremover-applied"
  [ -f "$MARK" ] && [ "$(cat "$MARK" 2>/dev/null)" = "$NEW" ] && return 0   # already synced
  if git -C "$BG" checkout "origin/$BR" -- '*.ts' '*.py' '*.sh' 2>>"$LOG"; then
    echo "$NEW" > "$MARK"
    log "bgremover code synced to ${NEW:0:7} (applies on next batch; no restart)"
    crumb "bgremover code synced to ${NEW:0:7} (applies on next nobg/embed batch)" "info"
  else
    log "bgremover checkout warned — left as-is"
  fi
}
update_bgremover

git fetch --quiet origin "$BRANCH" 2>>"$LOG" || { log "git fetch failed — skipping"; crumb "auto-update: git fetch failed (network/creds) — update NOT applied this cycle" "warn"; exit 0; }

LOCAL="$(git rev-parse HEAD 2>/dev/null)"
REMOTE="$(git rev-parse "origin/$BRANCH" 2>/dev/null)"
if [ -z "$LOCAL" ] || [ -z "$REMOTE" ]; then log "could not resolve revisions — skipping"; crumb "auto-update: could not resolve git revisions — update NOT applied" "warn"; exit 0; fi
if [ "$LOCAL" = "$REMOTE" ]; then exit 0; fi   # up to date — quiet no-op

CHANGED="$(git diff --name-only HEAD "origin/$BRANCH")"
if [ -z "$CHANGED" ]; then exit 0; fi

IN_SCOPE=""
OUT_OF_SCOPE=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  if in_scope "$f"; then IN_SCOPE+="$f "; else OUT_OF_SCOPE+="$f "; fi
done <<< "$CHANGED"

SHORT_LOCAL="${LOCAL:0:7}"; SHORT_REMOTE="${REMOTE:0:7}"

# ALWAYS sync the checkout to origin/main when behind. The M1 hosts nothing but
# this worker, so there's no server to protect: keeping it fully current (server
# /dashboard/schema files ride along harmlessly, the worker never runs them)
# avoids the stale-diff confusion the old scoped gate caused. The allowlist now
# only decides whether to RESTART the worker — a pure dashboard/docs push syncs
# the code without interrupting a resumable in-flight job. New Python deps
# self-install on first use (bridge ensurePipPackages), so no venv step here.
RESTART=0
[ -n "$IN_SCOPE" ] && RESTART=1
echo "$CHANGED" | grep -q "package-lock.json\|package.json" && RESTART=1

if [ "$DRY_RUN" = "1" ]; then
  log "DRY_RUN: would sync $SHORT_LOCAL→$SHORT_REMOTE (restart=$RESTART) — worker-side: ${IN_SCOPE:-none}${OUT_OF_SCOPE:+ | also: $OUT_OF_SCOPE}"
  exit 0
fi

log "syncing $SHORT_LOCAL→$SHORT_REMOTE (restart=$RESTART) — worker-side: ${IN_SCOPE:-none}${OUT_OF_SCOPE:+ | also pulled: $OUT_OF_SCOPE}"

# Reset to remote — only touches tracked files; gitignored configs/ and .env
# are untouched.
if ! git reset --hard "origin/$BRANCH" >>"$LOG" 2>&1; then
  log "git reset failed — leaving worker as-is"; crumb "auto-update git reset FAILED at $SHORT_REMOTE" "error"; exit 0
fi
if echo "$CHANGED" | grep -q "package-lock.json\|package.json"; then
  log "lockfile changed — npm install"; npm install --no-fund --no-audit >>"$LOG" 2>&1 || log "npm install warned"
fi

if [ "$RESTART" = "0" ]; then
  log "synced to $SHORT_REMOTE — no worker-side change, worker left running."
  crumb "auto-synced $SHORT_LOCAL→$SHORT_REMOTE (no restart needed)." "info"
  exit 0
fi

# Restart the worker (mirrors the kit's update.sh).
if [ -f "$PLIST" ]; then
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load "$PLIST" 2>>"$LOG" || log "launchctl load warned"
else
  log "worker plist not found at $PLIST — restart skipped (is the worker installed?)"
fi

# Confirm it came back (best-effort).
WLOG="$HOME/Library/Logs/crawler-worker.log"
MARK="$(date +%s)"; READY=0
for _ in $(seq 1 30); do
  if [ -f "$WLOG" ] && [ "$(stat -f %m "$WLOG" 2>/dev/null || echo 0)" -ge "$MARK" ] \
     && tail -8 "$WLOG" 2>/dev/null | grep -q "ready, waiting for jobs"; then READY=1; break; fi
  sleep 2
done

if [ "$READY" = "1" ]; then
  log "auto-updated to $SHORT_REMOTE and worker is back online."
  crumb "auto-updated $SHORT_LOCAL→$SHORT_REMOTE; worker restarted OK." "info"
else
  log "auto-updated to $SHORT_REMOTE but worker not confirmed ready — check $WLOG"
  crumb "auto-updated $SHORT_LOCAL→$SHORT_REMOTE but worker NOT confirmed ready." "warn"
fi
exit 0
