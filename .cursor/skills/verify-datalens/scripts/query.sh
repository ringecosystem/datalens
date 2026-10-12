#!/usr/bin/env bash
# POST /v1/query as the verify application and record the response plus new RPC calls.
# Usage: [VERIFY_START=N VERIFY_END=M] query.sh RUN_ID REQUEST_JSON_FILE [LABEL]
set -euo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="$1"; REQUEST="$2"; LABEL="${3:-$(basename "$REQUEST" .json)}"
# shellcheck disable=SC1090
source "$REPO/.data/verify/$RUN_ID/state.env"

N=$(find "$EVIDENCE_DIR/queries" -name '*.request.json' | wc -l)
PREFIX="$EVIDENCE_DIR/queries/$(printf '%02d' $((N + 1)))-$LABEL"
START="${VERIFY_START:-$PIN_START}"
END="${VERIFY_END:-$PIN_END}"
sed -e "s|\"@START@\"|$START|g" -e "s|\"@END@\"|$END|g" "$REQUEST" > "$PREFIX.request.json"
BEFORE=$(wc -l < "$EVIDENCE_DIR/rpc-calls.jsonl")

STATUS=$(curl -sS -m 60 -o "$PREFIX.response.json" -w '%{http_code}' -X POST "$BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -H "x-datalens-application: $APP" \
  -H "Authorization: Bearer $TOKEN" \
  --data @"$PREFIX.request.json")
sleep 0.3
AFTER=$(wc -l < "$EVIDENCE_DIR/rpc-calls.jsonl")
sed -n "$((BEFORE + 1)),${AFTER}p" "$EVIDENCE_DIR/rpc-calls.jsonl" > "$PREFIX.rpc-calls.jsonl"

python3 - "$PREFIX" "$STATUS" <<'PY'
import collections, json, sys
prefix, status = sys.argv[1], sys.argv[2]
calls = [json.loads(line) for line in open(prefix + ".rpc-calls.jsonl") if line.strip()]
by_method = collections.Counter(call.get("method", "") for call in calls)
try:
    body = json.load(open(prefix + ".response.json"))
except ValueError:
    body = {}
rows = body.get("rows") if isinstance(body, dict) and isinstance(body.get("rows"), list) else []
page = body.get("page") if isinstance(body, dict) else {}
summary = {
    "http_status": int(status),
    "row_count": len(rows),
    "served": body.get("served") if isinstance(body, dict) else None,
    "next_start": body.get("next_start") if isinstance(body, dict) else None,
    "incomplete_reason": body.get("incomplete_reason") if isinstance(body, dict) else None,
    "next_cursor": (page or {}).get("next_cursor") if isinstance(page, dict) else None,
    "rpc_calls": len(calls),
    "rpc_calls_by_method": dict(by_method),
    "error": body.get("error") if isinstance(body, dict) else None,
}
json.dump(summary, open(prefix + ".summary.json", "w"), indent=2)
print(json.dumps(summary, indent=2))
PY
echo "evidence: $PREFIX.*" >&2
