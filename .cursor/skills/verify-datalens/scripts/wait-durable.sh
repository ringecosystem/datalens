#!/usr/bin/env bash
# Block until the server log shows durable coverage written for DATASET over the pinned
# window (or VERIFY_START..VERIFY_END). Usage: wait-durable.sh RUN_ID DATASET [TIMEOUT_S]
set -euo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="$1"; DATASET="$2"; TIMEOUT="${3:-180}"
# shellcheck disable=SC1090
source "$REPO/.data/verify/$RUN_ID/state.env"
START="${VERIFY_START:-$PIN_START}"
END="${VERIFY_END:-$((START + 9))}"
for _ in $(seq 1 "$TIMEOUT"); do
  if grep -E "storage wrote coverage .*dataset=$DATASET .*range=$START-$END " "$EVIDENCE_DIR/server.log" >/dev/null; then
    echo "durable coverage written: $DATASET $START-$END"
    exit 0
  fi
  sleep 1
done
echo "timeout: no durable coverage for $DATASET $START-$END after ${TIMEOUT}s" >&2
exit 1
