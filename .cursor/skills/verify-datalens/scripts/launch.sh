#!/usr/bin/env bash
# Start one isolated datalens (PostgreSQL, auth on) behind a counting RPC proxy.
# Usage: launch.sh [--upstream URL] [--no-build]
# Prints RUN_ID. State is .data/verify/<RUN_ID>/state.env.
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$SKILL_DIR/../../.." && pwd)"
UPSTREAM="${VERIFY_DATALENS_UPSTREAM:-https://ethereum-rpc.publicnode.com}"
BUILD=1
while [ $# -gt 0 ]; do
  case "$1" in
    --upstream) UPSTREAM="$2"; shift 2 ;;
    --no-build) BUILD=0; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

cd "$REPO"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/code/helixbox/.cargo-target/datalens}"
BIN="$CARGO_TARGET_DIR/release/datalens"
if [ "$BUILD" = 1 ]; then
  cargo build --release -p datalens-server --bin datalens >&2
fi
[ -x "$BIN" ] || { echo "missing $BIN; run without --no-build" >&2; exit 1; }

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
DB_NAME="verify_$(date -u +%Y%m%d%H%M%S)_$$"
RUN_DIR="$REPO/.data/verify/$RUN_ID"
EVIDENCE_DIR="$REPO/.data/verify-evidence/$RUN_ID"
mkdir -p "$RUN_DIR" "$EVIDENCE_DIR/queries"
SERVER_PORT="$(free_port)"
PROXY_PORT="$(free_port)"
APP="verify"
TOKEN="verify-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
METRICS_TOKEN="metrics-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
DB_URL="postgres://datalens:datalens@127.0.0.1:55901/${DB_NAME}"

docker exec datalens-redesign-test-pg createdb -U datalens "$DB_NAME"

HEAD=$(curl -fsS -m 30 -X POST "$UPSTREAM" -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))')
PIN_END=$((HEAD - 70))
PIN_START=$((PIN_END - 7))

sed -e "s|@SERVER_PORT@|$SERVER_PORT|g" \
    -e "s|@DB_URL@|$DB_URL|g" \
    -e "s|@RPC_URL@|http://127.0.0.1:$PROXY_PORT|g" \
    -e "s|@APP@|$APP|g" \
    -e "s|@TOKEN@|$TOKEN|g" \
    -e "s|@METRICS_TOKEN@|$METRICS_TOKEN|g" \
    "$SKILL_DIR/templates/datalens.verify.toml" > "$RUN_DIR/datalens.toml"

python3 "$SKILL_DIR/scripts/rpc_proxy.py" --port "$PROXY_PORT" --upstream "$UPSTREAM" \
  --log "$EVIDENCE_DIR/rpc-calls.jsonl" > "$EVIDENCE_DIR/proxy.log" 2>&1 &
PROXY_PID=$!

RUST_LOG="${RUST_LOG:-info}" "$BIN" serve --config "$RUN_DIR/datalens.toml" \
  > "$EVIDENCE_DIR/server.log" 2>&1 &
SERVER_PID=$!

cat > "$RUN_DIR/state.env" <<EOF
RUN_ID=$RUN_ID
RUN_DIR=$RUN_DIR
EVIDENCE_DIR=$EVIDENCE_DIR
BASE_URL=http://127.0.0.1:$SERVER_PORT
SERVER_PORT=$SERVER_PORT
SERVER_PID=$SERVER_PID
PROXY_URL=http://127.0.0.1:$PROXY_PORT
PROXY_PORT=$PROXY_PORT
PROXY_PID=$PROXY_PID
UPSTREAM=$UPSTREAM
PIN_START=$PIN_START
PIN_END=$PIN_END
DB_NAME=$DB_NAME
APP=$APP
TOKEN=$TOKEN
METRICS_TOKEN=$METRICS_TOKEN
GIT_REV=$(git -C "$REPO" rev-parse --short HEAD)$(git -C "$REPO" diff --quiet || echo -dirty)
EOF
touch "$EVIDENCE_DIR/rpc-calls.jsonl"
grep -v -E '^(TOKEN|METRICS_TOKEN|DB_URL)=' "$RUN_DIR/state.env" > "$EVIDENCE_DIR/run.env"

BASE_URL="http://127.0.0.1:$SERVER_PORT"
ready=0
for _ in $(seq 1 90); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "datalens exited during startup; see $EVIDENCE_DIR/server.log" >&2
    tail -30 "$EVIDENCE_DIR/server.log" >&2
    kill "$PROXY_PID" 2>/dev/null || true
    docker exec datalens-redesign-test-pg dropdb -U datalens --if-exists "$DB_NAME" >/dev/null 2>&1 || true
    exit 1
  fi
  if curl -fsS -m 2 "$BASE_URL/ready" >/dev/null 2>&1 \
     && curl -fsS -m 5 -H "x-datalens-application: $APP" -H "Authorization: Bearer $TOKEN" \
          "$BASE_URL/v1/heads/ethereum" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  echo "datalens not ready; see $EVIDENCE_DIR/server.log" >&2
  tail -30 "$EVIDENCE_DIR/server.log" >&2
  exit 1
fi
echo "$RUN_ID"
