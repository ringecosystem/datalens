#!/usr/bin/env bash
# Read-only check that a verify instance is up and answering the PostgreSQL API.
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
check "GET /health" "curl -fsS -m 5 $BASE_URL/health | grep -q ok"
check "GET /ready" "curl -fsS -m 5 $BASE_URL/ready | grep -q ready"
check "GET /v1/heads/ethereum without credentials is 401" "[ \$(curl -s -m 5 -o /dev/null -w '%{http_code}' $BASE_URL/v1/heads/ethereum) = 401 ]"
check "GET /v1/heads/ethereum lists a head" "curl -fsS -m 10 -H \"x-datalens-application: $APP\" -H \"Authorization: Bearer $TOKEN\" $BASE_URL/v1/heads/ethereum | grep -q latest"
check "rpc proxy up" "curl -fsS -m 5 -X POST $PROXY_URL -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}' | grep -q result"
echo "info git_rev=$GIT_REV upstream=$UPSTREAM pin=$PIN_START..$PIN_END evidence=$EVIDENCE_DIR"
exit $fail
