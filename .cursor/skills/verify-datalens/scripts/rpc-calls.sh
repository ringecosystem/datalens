#!/usr/bin/env bash
# Summarize RPC calls recorded by the proxy for a run (or one query's slice).
# Usage: rpc-calls.sh RUN_ID [JSONL_FILE]
set -euo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="$1"
# shellcheck disable=SC1090
source "$REPO/.data/verify/$RUN_ID/state.env" 2>/dev/null || EVIDENCE_DIR="$REPO/.data/verify-evidence/$RUN_ID"
FILE="${2:-$EVIDENCE_DIR/rpc-calls.jsonl}"
python3 - "$FILE" <<'PY'
import collections, json, sys
calls = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
print(f"total JSON-RPC calls: {len(calls)} in {len({c.get('http_request', i) for i, c in enumerate(calls)})} HTTP requests")
for method, count in collections.Counter(c["method"] for c in calls).most_common():
    print(f"  {method}: {count}")
spans = collections.Counter()
for c in calls:
    if c["method"] == "eth_getLogs" and c.get("params"):
        p = c["params"][0]
        try:
            spans[int(p.get("toBlock", "0x0"), 16) - int(p.get("fromBlock", "0x0"), 16) + 1] += 1
        except (TypeError, ValueError):
            spans["tag"] += 1
if spans:
    print("eth_getLogs block span -> count:", dict(sorted(spans.items(), key=lambda kv: str(kv[0]))))
errors = [c for c in calls if c.get("error")]
print(f"calls with JSON-RPC error: {len(errors)}")
PY
