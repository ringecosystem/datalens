#!/usr/bin/env bash
# Read-only check that a verify instance is ours, up, and answering.
# Usage: doctor.sh RUN_ID
set -uo pipefail
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
RUN_ID="${1:?usage: doctor.sh RUN_ID}"
STATE="$REPO/.data/verify/$RUN_ID/state.env"
[ -f "$STATE" ] || { echo "FAIL no state file $STATE"; exit 1; }
# shellcheck disable=SC1090
source "$STATE"
fail=0
check() { if eval "$2" >/dev/null 2>&1; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }

check "server pid $SERVER_PID alive" "kill -0 $SERVER_PID"
check "proxy pid $PROXY_PID alive" "kill -0 $PROXY_PID"
check "port $SERVER_PORT owned by server pid" "ss -ltnp | grep -E ':$SERVER_PORT\\b' | grep -q 'pid=$SERVER_PID,'"
check "GET /health" "curl -fsS $BASE_URL/health"
check "GET /v1/chains without credentials is 401" "[ \$(curl -s -o /dev/null -w '%{http_code}' $BASE_URL/v1/chains) = 401 ]"
check "GET /v1/chains lists ethereum" "curl -fsS -H 'x-datalens-application: $APP' -H 'Authorization: Bearer $TOKEN' $BASE_URL/v1/chains | grep -q ethereum"
check "auth accepted (GET /v1/discovery)" "curl -fsS -H 'x-datalens-application: $APP' -H 'Authorization: Bearer $TOKEN' $BASE_URL/v1/discovery"
check "rpc proxy up" "curl -fsS $PROXY_URL/"
check "upstream answers eth_blockNumber via proxy" "curl -fsS -X POST $PROXY_URL -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}' | grep -q result"
echo "info git_rev=$GIT_REV upstream=$UPSTREAM evidence=$EVIDENCE_DIR"
echo "info note: the doctor's own eth_blockNumber probe is recorded in rpc-calls.jsonl"
exit $fail
