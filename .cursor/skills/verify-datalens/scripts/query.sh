#!/usr/bin/env bash
# POST a native query as the verify application and record request, response, and the
# RPC calls the query caused.
# Usage: [VERIFY_START=N VERIFY_END=M] query.sh RUN_ID REQUEST_JSON_FILE [LABEL]
# "@START@"/"@END@" in the request default to PIN_START..PIN_START+9 from launch.
set -euo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="$1"; REQUEST="$2"; LABEL="${3:-$(basename "$REQUEST" .json)}"
# shellcheck disable=SC1090
source "$REPO/.data/verify/$RUN_ID/state.env"

N=$(find "$EVIDENCE_DIR/queries" -name '*.request.json' | wc -l)
PREFIX="$EVIDENCE_DIR/queries/$(printf '%02d' $((N + 1)))-$LABEL"
START="${VERIFY_START:-$PIN_START}"
END="${VERIFY_END:-$((START + 9))}"
sed -e "s|\"@START@\"|$START|g" -e "s|\"@END@\"|$END|g" "$REQUEST" > "$PREFIX.request.json"
REQUEST="$PREFIX.request.json"
BEFORE=$(wc -l < "$EVIDENCE_DIR/rpc-calls.jsonl")

STATUS=$(curl -sS -o "$PREFIX.response.json" -w '%{http_code}' -X POST "$BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -H "x-datalens-application: $APP" \
  -H "Authorization: Bearer $TOKEN" \
  --data @"$REQUEST")
sleep 0.3
AFTER=$(wc -l < "$EVIDENCE_DIR/rpc-calls.jsonl")
sed -n "$((BEFORE + 1)),${AFTER}p" "$EVIDENCE_DIR/rpc-calls.jsonl" > "$PREFIX.rpc-calls.jsonl"

python3 - "$PREFIX" "$STATUS" <<'PY'
import collections, json, sys
prefix, status = sys.argv[1], sys.argv[2]
calls = [json.loads(line) for line in open(prefix + ".rpc-calls.jsonl") if line.strip()]
by_method = collections.Counter(call["method"] for call in calls)
try:
    body = json.load(open(prefix + ".response.json"))
except ValueError:
    body = {}
cache = body.get("cache", {}) if isinstance(body, dict) else {}
rows = (((body.get("rows") or {}).get("rows") or {}).get("rows") or []) if isinstance(body, dict) else []
summary = {
    "http_status": int(status),
    "row_count": len(rows),
    "hit_ranges": cache.get("hit_ranges"),
    "missing_ranges": cache.get("missing_ranges"),
    "provider_fill_ranges": cache.get("provider_fill_ranges"),
    "rpc_calls": len(calls),
    "rpc_http_requests": len({c.get("http_request", i) for i, c in enumerate(calls)}),
    "rpc_calls_by_method": dict(by_method),
    "error": body.get("error") if isinstance(body, dict) else None,
}
json.dump(summary, open(prefix + ".summary.json", "w"), indent=2)
print(json.dumps(summary, indent=2))
PY
echo "evidence: $PREFIX.*" >&2
