#!/usr/bin/env bash
# Stop the processes this run started and delete its instance dir. Evidence is kept.
# Usage: cleanup.sh RUN_ID
set -uo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="${1:?usage: cleanup.sh RUN_ID}"
RUN_DIR="$REPO/.data/verify/$RUN_ID"
[ -f "$RUN_DIR/state.env" ] || { echo "nothing to clean for $RUN_ID"; exit 0; }
# shellcheck disable=SC1090
source "$RUN_DIR/state.env"
for pid in "$SERVER_PID" "$PROXY_PID"; do
  kill "$pid" 2>/dev/null || true
done
for _ in $(seq 1 20); do
  kill -0 "$SERVER_PID" 2>/dev/null || kill -0 "$PROXY_PID" 2>/dev/null || break
  sleep 0.5
done
kill -9 "$SERVER_PID" "$PROXY_PID" 2>/dev/null || true
rm -rf "$RUN_DIR"
echo "cleaned $RUN_ID; evidence kept at $EVIDENCE_DIR"
