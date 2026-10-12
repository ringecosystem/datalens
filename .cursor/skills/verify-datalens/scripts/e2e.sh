#!/usr/bin/env bash
# Drive the new HTTP API against live Ethereum and print a feature table.
# Usage: e2e.sh [--no-build]
set -euo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN="$("$SKILL_DIR/scripts/launch.sh" "$@")"
# shellcheck disable=SC1090
source "$REPO/.data/verify/$RUN/state.env"
TABLE="$EVIDENCE_DIR/features.tsv"
printf 'feature\tresult\tdetail\n' > "$TABLE"
pass() { printf '%s\tPASS\t%s\n' "$1" "$2" | tee -a "$TABLE"; }
fail() { printf '%s\tFAIL\t%s\n' "$1" "$2" | tee -a "$TABLE"; FAILED=1; }
FAILED=0
cleanup() { "$SKILL_DIR/scripts/cleanup.sh" "$RUN" || true; }
trap cleanup EXIT

if "$SKILL_DIR/scripts/doctor.sh" "$RUN"; then
  pass doctor "pids, /health, /ready, 401, heads, proxy"
else
  fail doctor "see doctor output"
fi

auth=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "x-datalens-application: $APP" -H 'Authorization: Bearer wrong' "$BASE_URL/v1/heads/ethereum")
if [ "$auth" = 401 ]; then pass auth-bad-token "401"; else fail auth-bad-token "status=$auth"; fi

"$SKILL_DIR/scripts/query.sh" "$RUN" "$SKILL_DIR/requests/logs-usdc-transfer-durable.json" logs-fill >/dev/null
python3 - "$EVIDENCE_DIR" "$TABLE" <<'PY' || FAILED=1
import json, pathlib, sys
evidence, table = sys.argv[1], sys.argv[2]
root = pathlib.Path(evidence) / "queries"
def latest(label):
    matches = sorted(root.glob(f"*-{label}.summary.json"))
    return json.loads(matches[-1].read_text()) if matches else None
rows = []
fill = latest("logs-fill")
ok = fill and fill["http_status"] == 200 and fill["incomplete_reason"] is None and fill["served"]
rows.append(("logs-durable", "PASS" if ok else "FAIL", json.dumps(fill)))
open(table, "a").write("\t".join(rows[0]) + "\n")
print(rows[0][0], rows[0][1])
sys.exit(0 if ok else 1)
PY

BEFORE=$(python3 -c 'import json,sys; print(sum(1 for line in open(sys.argv[1]) if "eth_getLogs" in line))' "$EVIDENCE_DIR/rpc-calls.jsonl")
"$SKILL_DIR/scripts/query.sh" "$RUN" "$SKILL_DIR/requests/logs-usdc-transfer-durable.json" logs-hit >/dev/null
AFTER=$(python3 -c 'import json,sys; print(sum(1 for line in open(sys.argv[1]) if "eth_getLogs" in line))' "$EVIDENCE_DIR/rpc-calls.jsonl")
DELTA=$((AFTER - BEFORE))
if [ "$DELTA" = 0 ]; then pass logs-cache-hit "eth_getLogs delta 0"; else fail logs-cache-hit "eth_getLogs delta $DELTA"; fi

ONE_END=$PIN_START
VERIFY_START=$PIN_START VERIFY_END=$ONE_END "$SKILL_DIR/scripts/query.sh" "$RUN" "$SKILL_DIR/requests/logs-with-sender.json" logs-sender >/dev/null
python3 - "$EVIDENCE_DIR" "$TABLE" <<'PY' || FAILED=1
import json, pathlib, sys
evidence, table = sys.argv[1], sys.argv[2]
summary = json.loads(sorted((pathlib.Path(evidence)/"queries").glob("*-logs-sender.summary.json"))[-1].read_text())
body = json.loads(sorted((pathlib.Path(evidence)/"queries").glob("*-logs-sender.response.json"))[-1].read_text())
ok = summary["http_status"] == 200 and summary["incomplete_reason"] is None
if ok and summary["row_count"]:
    ok = all("transaction_from" in row for row in body["rows"])
open(table, "a").write(f"transaction_from\t{'PASS' if ok else 'FAIL'}\trows={summary['row_count']} status={summary['http_status']}\n")
print("transaction_from", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
true

"$SKILL_DIR/scripts/query.sh" "$RUN" "$SKILL_DIR/requests/logs-page.json" logs-page >/dev/null
python3 - "$EVIDENCE_DIR" "$TABLE" <<'PY' || FAILED=1
import json, pathlib, sys
evidence, table = sys.argv[1], sys.argv[2]
summary = json.loads(sorted((pathlib.Path(evidence)/"queries").glob("*-logs-page.summary.json"))[-1].read_text())
full = json.loads(sorted((pathlib.Path(evidence)/"queries").glob("*-logs-fill.summary.json"))[-1].read_text())
ok = summary["http_status"] == 200 and summary["row_count"] <= 1
if full["row_count"] > 1:
    ok = ok and bool(summary["next_cursor"])
open(table, "a").write(f"pagination\t{'PASS' if ok else 'FAIL'}\tpage_rows={summary['row_count']} cursor={summary['next_cursor']}\n")
print("pagination", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
true

"$SKILL_DIR/scripts/query.sh" "$RUN" "$SKILL_DIR/requests/blocks-durable.json" blocks >/dev/null
python3 - "$EVIDENCE_DIR" "$TABLE" <<'PY' || FAILED=1
import json, pathlib, sys
evidence, table = sys.argv[1], sys.argv[2]
summary = json.loads(sorted((pathlib.Path(evidence)/"queries").glob("*-blocks.summary.json"))[-1].read_text())
ok = summary["http_status"] == 200 and summary["incomplete_reason"] is None and summary["row_count"] >= 1
open(table, "a").write(f"blocks-durable\t{'PASS' if ok else 'FAIL'}\trows={summary['row_count']} status={summary['http_status']}\n")
print("blocks-durable", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
true

WATCH=$(curl -sS -m 20 -o "$EVIDENCE_DIR/watch.json" -w '%{http_code}' -X POST "$BASE_URL/v1/watch" \
  -H 'content-type: application/json' -H "x-datalens-application: $APP" -H "Authorization: Bearer $TOKEN" \
  -d '{"chain":"ethereum","dataset":"evm.logs","selector":{"addresses":["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]}}')
if [ "$WATCH" = 200 ]; then pass watch "200"; else fail watch "status=$WATCH"; fi

WAIT=$(curl -sS -m 20 -o "$EVIDENCE_DIR/wait.json" -w '%{http_code}' \
  -H "x-datalens-application: $APP" -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/v1/heads/ethereum/wait?after=0&kind=safe&timeout_ms=500")
if [ "$WAIT" = 200 ]; then pass heads-wait "200"; else fail heads-wait "status=$WAIT"; fi

GQL=$(curl -sS -m 60 -o "$EVIDENCE_DIR/graphql.json" -w '%{http_code}' -X POST "$BASE_URL/graphql" \
  -H 'content-type: application/json' -H "x-datalens-application: $APP" -H "Authorization: Bearer $TOKEN" \
  -d "{\"query\":\"query(\$input: JSON!) { query(input: \$input) { dataset incompleteReason } }\",\"variables\":{\"input\":{\"chain\":\"ethereum\",\"dataset\":\"evm.logs\",\"selector\":{\"addresses\":[\"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\"]},\"range\":{\"start\":$PIN_START,\"end\":$PIN_END},\"finality\":\"durable_only\"}}}")
if [ "$GQL" = 200 ] && grep -q evm.logs "$EVIDENCE_DIR/graphql.json"; then pass graphql "200 evm.logs"; else fail graphql "status=$GQL"; fi

"$SKILL_DIR/scripts/query.sh" "$RUN" "$SKILL_DIR/requests/logs-over-quota.json" logs-quota >/dev/null
python3 - "$EVIDENCE_DIR" "$TABLE" <<'PY' || FAILED=1
import json, pathlib, sys
evidence, table = sys.argv[1], sys.argv[2]
summary = json.loads(sorted((pathlib.Path(evidence)/"queries").glob("*-logs-quota.summary.json"))[-1].read_text())
kind = (summary.get("error") or {}).get("kind")
ok = summary["http_status"] == 400 and kind == "invalid_input"
open(table, "a").write(f"quota-range\t{'PASS' if ok else 'FAIL'}\tstatus={summary['http_status']} kind={kind}\n")
print("quota-range", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
true

MET=$(curl -sS -m 10 -H "Authorization: Bearer $METRICS_TOKEN" "$BASE_URL/metrics" | tee "$EVIDENCE_DIR/metrics.txt" | grep -c datalens_provider_calls_total || true)
if [ "$MET" -ge 1 ]; then pass metrics "datalens_provider_calls_total present"; else fail metrics "missing provider counter"; fi

echo "RUN=$RUN evidence=$EVIDENCE_DIR"
column -t -s $'\t' "$TABLE" || cat "$TABLE"
exit $FAILED
