#!/usr/bin/env bash
# Wait until a recorded query summary for LABEL is complete over the pin window.
# Usage: wait-durable.sh RUN_ID LABEL [TIMEOUT_S]
set -euo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="$1"; LABEL="$2"; TIMEOUT="${3:-180}"
# shellcheck disable=SC1090
source "$REPO/.data/verify/$RUN_ID/state.env"
for _ in $(seq 1 "$TIMEOUT"); do
  if python3 - "$EVIDENCE_DIR/queries" "$LABEL" <<'PY'
import json, pathlib, sys
root, label = pathlib.Path(sys.argv[1]), sys.argv[2]
matches = sorted(root.glob(f"*-{label}.summary.json"))
if not matches:
    sys.exit(1)
summary = json.loads(matches[-1].read_text())
ok = summary.get("http_status") == 200 and summary.get("incomplete_reason") is None and summary.get("served")
sys.exit(0 if ok else 1)
PY
  then
    echo "query complete: $LABEL"
    exit 0
  fi
  sleep 1
done
echo "timeout: $LABEL was not a complete query after ${TIMEOUT}s" >&2
exit 1
