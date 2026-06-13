#!/bin/bash
# ============================================================================
# Scoped auto-updater for the Clothedd home (M1) worker.
#
# Run periodically by the com.clothedd.crawler-autoupdate LaunchAgent (every
# ~10 min). It pulls + restarts the worker ONLY when every change pending on
# origin/main is confined to the M1 auto-update allowlist
# (scripts/m1-autoupdate-allow.txt) — i.e. the worker / processing / local
# status code. Anything touching the cloud server, the React dashboard, the DB
# schema, or dependencies is left for a manual `update.sh`, so a remote push of
# unrelated work can never silently redeploy this Mac.
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

git fetch --quiet origin "$BRANCH" 2>>"$LOG" || { log "git fetch failed — skipping"; exit 0; }

LOCAL="$(git rev-parse HEAD 2>/dev/null)"
REMOTE="$(git rev-parse "origin/$BRANCH" 2>/dev/null)"
if [ -z "$LOCAL" ] || [ -z "$REMOTE" ]; then log "could not resolve revisions — skipping"; exit 0; fi
if [ "$LOCAL" = "$REMOTE" ]; then exit 0; fi   # up to date — quiet no-op

CHANGED="$(git diff --name-only HEAD "origin/$BRANCH")"
if [ -z "$CHANGED" ]; then exit 0; fi

OUT_OF_SCOPE=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  in_scope "$f" || OUT_OF_SCOPE+="$f "
done <<< "$CHANGED"

SHORT_LOCAL="${LOCAL:0:7}"; SHORT_REMOTE="${REMOTE:0:7}"

if [ -n "$OUT_OF_SCOPE" ]; then
  MSG="update $SHORT_LOCAL→$SHORT_REMOTE NOT auto-applied — out-of-scope files: ${OUT_OF_SCOPE}. Run update.sh manually."
  log "$MSG"
  crumb "$MSG" "warn"
  exit 0
fi

if [ "$DRY_RUN" = "1" ]; then
  log "DRY_RUN: would auto-apply $SHORT_LOCAL→$SHORT_REMOTE (in scope): $(echo "$CHANGED" | tr '\n' ' ')"
  exit 0
fi

log "auto-applying $SHORT_LOCAL→$SHORT_REMOTE (in scope): $(echo "$CHANGED" | tr '\n' ' ')"

# Reset to remote — only touches tracked files; gitignored configs/ and .env
# are untouched. Worker/processing/status changes need no npm install (deps are
# out of scope by allowlist), but install defensively if a lockfile slipped in.
if ! git reset --hard "origin/$BRANCH" >>"$LOG" 2>&1; then
  log "git reset failed — leaving worker as-is"; crumb "auto-update git reset FAILED at $SHORT_REMOTE" "error"; exit 0
fi
if echo "$CHANGED" | grep -q "package-lock.json\|package.json"; then
  log "lockfile changed — npm install"; npm install --no-fund --no-audit >>"$LOG" 2>&1 || log "npm install warned"
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
