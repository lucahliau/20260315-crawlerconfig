#!/bin/bash
# ============================================================================
# One-time installer for the M1 scoped auto-updater LaunchAgent.
#
# Run this ONCE on the home Mac (after a manual update.sh that brings in this
# commit). From then on, pushes that only touch the M1 worker/processing/status
# code (per scripts/m1-autoupdate-allow.txt) auto-deploy to this Mac every
# ~10 minutes — no manual update needed. Anything broader still waits for you
# to run update.sh.
#
#   bash scripts/install-m1-autoupdate.sh
#
# Uninstall:
#   launchctl unload "$HOME/Library/LaunchAgents/com.clothedd.crawler-autoupdate.plist"
#   rm "$HOME/Library/LaunchAgents/com.clothedd.crawler-autoupdate.plist"
# ============================================================================
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.clothedd.crawler-autoupdate"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
INTERVAL="${AUTOUPDATE_INTERVAL_SECONDS:-600}"
LOG="$HOME/Library/Logs/crawler-autoupdate.log"

step() { printf "\n\033[1m==> %s\033[0m\n" "$1"; }

step "Sanity checks"
command -v git >/dev/null || { echo "git not found"; exit 1; }
command -v npx >/dev/null || { echo "npx (Node) not found"; exit 1; }
[ -f "$REPO/scripts/auto-update.sh" ] || { echo "auto-update.sh missing — pull latest first"; exit 1; }
chmod +x "$REPO/scripts/auto-update.sh"
mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs"

# The job MUST launch via node, not bash: macOS TCC gates launchd access to
# ~/Desktop by the job's responsible binary, and on this Mac only node holds
# the Desktop-folder grant (the worker runs via `exec npm` for the same
# reason). A /bin/bash job is denied with "Operation not permitted" and
# launchctl shows exit 126 (seen after the macOS 26.5.1 update, 2026-07).
# bash runs as a CHILD of node here and inherits the grant.
NODE_BIN="$(command -v node || true)"
[ -n "$NODE_BIN" ] || NODE_BIN="/opt/homebrew/bin/node"
[ -x "$NODE_BIN" ] || { echo "node not found (PATH or /opt/homebrew/bin)"; exit 1; }

step "Writing LaunchAgent ($LABEL, every ${INTERVAL}s)"
cat > "$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$NODE_BIN</string>
    <string>-e</string>
    <string>const cp=require('child_process');try{cp.execFileSync('/bin/bash',['$REPO/scripts/auto-update.sh'],{stdio:'inherit'});}catch(e){process.exit(typeof e.status==='number'?e.status:1);}</string>
  </array>
  <key>WorkingDirectory</key><string>$REPO</string>
  <key>StartInterval</key><integer>$INTERVAL</integer>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict>
</plist>
PLIST_EOF

step "Loading it"
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load "$PLIST"

step "Done"
printf "\033[32m✓ Auto-updater installed.\033[0m It checks origin/main every %ss.\n" "$INTERVAL"
echo "  Scope:   $REPO/scripts/m1-autoupdate-allow.txt"
echo "  Log:     $LOG"
echo "  Status:  also surfaces on the cloud dashboard → Errors → Worker issues"
echo "  Test:    DRY_RUN=1 bash \"$REPO/scripts/auto-update.sh\""
