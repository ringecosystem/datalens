#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/code/helixbox/.cargo-target/datalens}"
export DATABASE_URL="${DATABASE_URL:-postgres://datalens:datalens@127.0.0.1:55901/datalens}"
cargo test --release -p datalens-server --test load -- --nocapture --test-threads=1
