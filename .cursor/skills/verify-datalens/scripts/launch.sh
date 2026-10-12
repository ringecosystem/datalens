#!/usr/bin/env bash
# Start one isolated datalens instance (local storage, auth on) behind a counting RPC proxy.
# Usage: launch.sh [--upstream URL] [--no-build]
# Prints RUN_ID; all state lives in .data/verify/<RUN_ID>/state.env.
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
BIN="$REPO/target/release/datalens"
if [ "$BUILD" = 1 ]; then
  cargo build --release -p datalens-cli >&2
fi
[ -x "$BIN" ] || { echo "missing $BIN; run without --no-build" >&2; exit 1; }

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="$REPO/.data/verify/$RUN_ID"
EVIDENCE_DIR="$REPO/.data/verify-evidence/$RUN_ID"
mkdir -p "$RUN_DIR/storage" "$EVIDENCE_DIR/queries"
SERVER_PORT="$(free_port)"
PROXY_PORT="$(free_port)"
APP="verify"
TOKEN="verify-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
METRICS_TOKEN="metrics-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"

# Pinned query window: head-500 is past finalized lag (96) yet inside the ~1000-block
# history public non-archive nodes serve. Fetched directly, so it is not counted.
HEAD=$(curl -fsS -X POST "$UPSTREAM" -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
  | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))')
PIN_START=$((HEAD - 500))

sed -e "s|@SERVER_PORT@|$SERVER_PORT|g" \
    -e "s|@STORAGE_ROOT@|$RUN_DIR/storage|g" \
    -e "s|@RPC_URL@|http://127.0.0.1:$PROXY_PORT|g" \
    -e "s|@APP@|$APP|g" \
    -e "s|@TOKEN@|$TOKEN|g" \
    -e "s|@METRICS_TOKEN@|$METRICS_TOKEN|g" \
    "$SKILL_DIR/templates/datalens.verify.toml" > "$RUN_DIR/datalens.toml"

python3 "$SKILL_DIR/scripts/rpc_proxy.py" --port "$PROXY_PORT" --upstream "$UPSTREAM" \
  --log "$EVIDENCE_DIR/rpc-calls.jsonl" > "$EVIDENCE_DIR/proxy.log" 2>&1 &
PROXY_PID=$!

RUST_LOG="${RUST_LOG:-info}" "$BIN" serve --config "$RUN_DIR/datalens.toml" \
  --bind "127.0.0.1:$SERVER_PORT" > "$EVIDENCE_DIR/server.log" 2>&1 &
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
APP=$APP
TOKEN=$TOKEN
METRICS_TOKEN=$METRICS_TOKEN
GIT_REV=$(git -C "$REPO" rev-parse --short HEAD)$(git -C "$REPO" diff --quiet || echo -dirty)
EOF
touch "$EVIDENCE_DIR/rpc-calls.jsonl"
grep -v -E '^(TOKEN|METRICS_TOKEN)=' "$RUN_DIR/state.env" > "$EVIDENCE_DIR/run.env"

for _ in $(seq 1 120); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "datalens exited during startup; see $EVIDENCE_DIR/server.log" >&2
    tail -20 "$EVIDENCE_DIR/server.log" >&2
    kill "$PROXY_PID" 2>/dev/null || true
    exit 1
  fi
  if curl -fsS "http://127.0.0.1:$SERVER_PORT/health" >/dev/null 2>&1 \
     && curl -fsS "http://127.0.0.1:$PROXY_PORT/" >/dev/null 2>&1; then
    echo "$RUN_ID"
    exit 0
  fi
  sleep 0.5
done
echo "datalens not ready after 60s; see $EVIDENCE_DIR/server.log" >&2
exit 1
