# Config-First Index Smoke

Goal: Manually validate the declarative index runner with RustFS/S3-compatible storage,
`datalens serve`, the ORMP example config, checkpoint skip behavior, and durable-cache
reruns.

Read this when: You need opt-in live coverage for `datalens index doctor`, `index plan`,
and `index run --config examples/ormp/ormp.index.toml` against a real datalens server.

Preconditions: Docker Compose, Rust toolchain, `python3`, and stable EVM RPC URLs for
Ethereum and Polygon. Public RPC may be used only when acceptable for a manual smoke;
CI must not run this by default.

Depends on: `docs/runbook/local-rustfs.md` for RustFS startup and S3-compatible
environment variables.

Verification: The final report records initial run timing, checkpoint skip timing,
deleted-checkpoint durable-cache timing, JSONL rows, provider fill ranges, durable
full-hit counts, storage object count, storage size, and provider limitations.

## Setup

Run from the repository root. Use a unique prefix so repeated smoke runs do not share
storage state.

```sh
export SMOKE_ROOT=".tmp/ormp-live-smoke"
export DATALENS_S3_ENDPOINT_URL=http://localhost:9000
export DATALENS_S3_BUCKET=datalens
export DATALENS_S3_PREFIX="ormp-live-smoke-$(date +%s)"
export DATALENS_S3_REGION=auto
export DATALENS_S3_FORCE_PATH_STYLE=true
export AWS_ACCESS_KEY_ID=datalens-dev
export AWS_SECRET_ACCESS_KEY=datalens-dev-secret
export AWS_REGION=auto
export DATALENS_ORMP_TOKEN=ormp-smoke-token
export DATALENS_ETHEREUM_RPC_URL="https://ethereum-rpc.example.invalid"
export DATALENS_POLYGON_RPC_URL="https://polygon-rpc.example.invalid"
mkdir -p "$SMOKE_ROOT"
```

Start RustFS using the repository compose flow:

```sh
docker compose up -d rustfs-init
docker compose ps
```

Create a smoke server config that points at RustFS and enables the ORMP chains:

```sh
cat > "$SMOKE_ROOT/datalens.toml" <<'EOF'
[server]
bind = "127.0.0.1:3000"

[storage]
backend = "s3"

[storage.s3]
bucket = "${DATALENS_S3_BUCKET}"
prefix = "${DATALENS_S3_PREFIX}"
region = "${DATALENS_S3_REGION}"
endpoint_url = "${DATALENS_S3_ENDPOINT_URL}"
force_path_style = true

[planner]
max_query_range_blocks = 100000
default_chunk_range_blocks = 10000

[writer]
target_object_bytes = 16777216
min_object_rows = 1
record_empty_coverage = true

[writer.staging]
enabled = true
min_rows = 1000
target_object_bytes = 16777216
max_staged_ranges = 32
max_staged_rows = 10000
max_staged_age_ms = 60000
flush_on_shutdown = true
max_staged_bytes = 33554432

[metrics]
enabled = true
default_application = "ormp"

[query.native]
graphql_enabled = false
path = "/native/graphql"
playground_enabled = false
playground_path = "/native/graphiql"

[query.index]
graphql_enabled = false
path = "/index/graphql"
playground_enabled = false
playground_path = "/index/graphiql"

[warmup]
enabled = false
registry_path = ".tmp/ormp-live-smoke/warmup"
scheduler_interval_ms = 1000
max_global_tasks = 1
max_per_chain_tasks = 1
max_fetches_per_loop = 1
flush_on_shutdown = true

[applications]
required = false

[chains.ethereum]
kind = "evm"
chain_id = 1
rpc_urls = ["${DATALENS_ETHEREUM_RPC_URL}"]

[chains.ethereum.finality]
mode = "lag"
safe_lag_blocks = 32
finalized_lag_blocks = 96

[chains.ethereum.datasets.blocks]
enabled = true
max_batch_blocks = 1000

[chains.ethereum.datasets.logs]
enabled = true
max_get_logs_range_blocks = 1000
max_addresses_per_query = 16

[chains.polygon]
kind = "evm"
chain_id = 137
rpc_urls = ["${DATALENS_POLYGON_RPC_URL}"]

[chains.polygon.finality]
mode = "lag"
safe_lag_blocks = 128
finalized_lag_blocks = 256

[chains.polygon.datasets.blocks]
enabled = true
max_batch_blocks = 1000

[chains.polygon.datasets.logs]
enabled = true
max_get_logs_range_blocks = 1000
max_addresses_per_query = 16
EOF
```

Start the server and wait for health:

```sh
cargo run -p datalens-cli -- serve --config "$SMOKE_ROOT/datalens.toml" \
  > "$SMOKE_ROOT/serve.log" 2>&1 &
echo "$!" > "$SMOKE_ROOT/serve.pid"
python3 - <<'PY'
import time
import urllib.request

for _ in range(60):
    try:
        with urllib.request.urlopen("http://127.0.0.1:3000/health", timeout=1) as response:
            if response.status == 200:
                raise SystemExit(0)
    except Exception:
        time.sleep(1)
raise SystemExit("datalens health check did not become ready")
PY
```

## Execute

Run doctor and plan:

```sh
cargo run -p datalens-cli -- index doctor --config examples/ormp/ormp.index.toml \
  | tee "$SMOKE_ROOT/doctor.json"
cargo run -p datalens-cli -- index plan --config examples/ormp/ormp.index.toml \
  | tee "$SMOKE_ROOT/plan.json"
```

Run from an empty checkpoint and output path:

```sh
rm -f .data/indexes/ormp/events.jsonl .data/indexes/ormp/checkpoint.json
python3 - <<'PY'
import json
import subprocess
import time

started = time.monotonic()
output = subprocess.check_output([
    "cargo", "run", "-p", "datalens-cli", "--",
    "index", "run", "--config", "examples/ormp/ormp.index.toml",
], text=True)
elapsed_ms = int((time.monotonic() - started) * 1000)
report = json.loads(output)
report["wall_time_ms"] = elapsed_ms
open(".tmp/ormp-live-smoke/initial-run.json", "w").write(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
PY
```

Run a second time to verify checkpoint skip behavior:

```sh
python3 - <<'PY'
import json
import subprocess
import time

started = time.monotonic()
output = subprocess.check_output([
    "cargo", "run", "-p", "datalens-cli", "--",
    "index", "run", "--config", "examples/ormp/ormp.index.toml",
], text=True)
elapsed_ms = int((time.monotonic() - started) * 1000)
report = json.loads(output)
report["wall_time_ms"] = elapsed_ms
open(".tmp/ormp-live-smoke/checkpoint-rerun.json", "w").write(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
PY
```

Delete the checkpoint only, keep RustFS durable cache, then rerun:

```sh
rm -f .data/indexes/ormp/checkpoint.json
python3 - <<'PY'
import json
import subprocess
import time

started = time.monotonic()
output = subprocess.check_output([
    "cargo", "run", "-p", "datalens-cli", "--",
    "index", "run", "--config", "examples/ormp/ormp.index.toml",
], text=True)
elapsed_ms = int((time.monotonic() - started) * 1000)
report = json.loads(output)
report["wall_time_ms"] = elapsed_ms
open(".tmp/ormp-live-smoke/durable-cache-rerun.json", "w").write(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
PY
```

## Inspect

Collect JSONL, storage, and metrics evidence:

```sh
python3 - <<'PY'
from pathlib import Path

path = Path(".data/indexes/ormp/events.jsonl")
print({"jsonl_rows": sum(1 for _ in path.open()) if path.exists() else 0})
PY

python3 - <<'PY'
from pathlib import Path

root = Path(".data/oss")
files = [path for path in root.rglob("*") if path.is_file()]
print({"storage_files": len(files), "storage_bytes": sum(path.stat().st_size for path in files)})
PY

python3 - <<'PY'
import urllib.request

with urllib.request.urlopen("http://127.0.0.1:3000/metrics", timeout=5) as response:
    body = response.read().decode()
open(".tmp/ormp-live-smoke/metrics.prom", "w").write(body)
print("\n".join(line for line in body.splitlines() if "datalens" in line.lower())[:4000])
PY
```

Write the final comparison report from the three run reports:

```sh
python3 - <<'PY'
import json
from pathlib import Path

root = Path(".tmp/ormp-live-smoke")
runs = {
    "initial": json.loads((root / "initial-run.json").read_text()),
    "checkpoint_skip": json.loads((root / "checkpoint-rerun.json").read_text()),
    "durable_cache_rerun": json.loads((root / "durable-cache-rerun.json").read_text()),
}

def compact(report):
    return {
        "wall_time_ms": report["wall_time_ms"],
        "summary": report["summary"],
        "chains": report["chains"],
    }

print(json.dumps({name: compact(report) for name, report in runs.items()}, indent=2))
PY
```

The report must include:

- Initial run wall time and `summary.elapsed_ms`.
- Per-chain elapsed time from `chains[].elapsed_ms`.
- Checkpoint skip wall time and `summary.checkpoint_skipped_ranges`.
- Deleted-checkpoint durable-cache rerun wall time.
- Rows written from `summary.rows_written` and JSONL line count.
- Provider fill ranges from `chains[].provider_fill_ranges`.
- Durable cache full-hit counts from `summary.full_durable_hit_count` and
  `chains[].full_durable_hit_count`.
- Storage file count and bytes after the runs.
- Provider limitations such as RPC rate limits, range caps, missing archive data, or
  endpoint errors observed in `serve.log` or command stderr.

## Cleanup

```sh
kill "$(cat "$SMOKE_ROOT/serve.pid")"
docker compose down
```

Do not commit `.tmp/`, `.data/indexes/`, `.data/oss/`, smoke reports, checkpoints,
storage data, provider logs, or secrets.
