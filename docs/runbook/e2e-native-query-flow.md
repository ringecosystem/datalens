# E2E Native Query Flow

Goal: Validate the initial native EVM block and log query flow after code changes.

Read this when: You need to decide whether an HBX-13-style change is ready for review.

Preconditions:

- Rust toolchain available for the workspace.
- Run commands from the repository root.
- Use deterministic tests for CI and PR readiness.
- Use live RPC only as an optional manual smoke test.

Depends on:

- `docs/spec/technical-architecture/en/03-storage-and-manifest.md`
- `docs/spec/technical-architecture/en/04-query-and-fill-flow.md`
- `docs/spec/technical-architecture/en/05-chain-adapters-and-evm.md`
- `docs/spec/technical-architecture/en/06-api-sdk-and-compatibility.md`

Verification:

- Required PR readiness command: `just e2e`
- Required workspace gates: `just fmt-check` and `just check`

## CI-Suitable Validation

Run this before considering a native block/log query PR ready for review:

```sh
just fmt-check
just check
just e2e
```

`just e2e` runs:

```sh
cargo test -p datalens-api --test query_flow
```

This deterministic test suite uses an in-process mock source and local temporary storage.
It does not call public RPC endpoints.

Coverage included by `just e2e`:

| Scenario | Validation |
| --- | --- |
| Block cache miss then hit | First `blocks` query fetches and persists rows; equivalent second query is served from cache. |
| Block partial hit | Seeded covered range is read from cache; only missing block range is fetched. |
| Non-empty log miss then hit | First filtered `logs` query persists matching rows; equivalent second query is served from cache. |
| Empty log coverage | Empty `logs` result writes `manifest.json`, records row count `0`, and does not create tiny data objects. |
| Log filter correctness | Returned deterministic log rows satisfy the requested address, topic, and block range filters. |
| Range limit error | Query range above `planner.max_query_range_blocks` returns `InvalidInput`. |
| Provider limit error | Provider limit failure is surfaced as `ProviderLimit` and does not write manifest coverage. |

Cache-hit proof:

- The mock source records `fetch_blocks` and `fetch_logs` calls.
- Equivalent cache-hit queries must not add another source call.
- Partial-hit queries must fetch only the missing range.

Local artifacts:

- Deterministic tests write under OS temporary directories named `datalens-*`.
- They do not require manual cleanup after a normal successful run.

## Local Manual HTTP Smoke

Use this section only when you need to manually exercise the HTTP server with a real or
local JSON-RPC endpoint. Do not use public RPC smoke results as the only PR readiness
signal.

Required environment variables:

```sh
export DATALENS_E2E_RPC_URL="http://127.0.0.1:8545"
export DATALENS_E2E_STORAGE_ROOT="$(pwd)/.tmp/datalens-e2e-storage"
export DATALENS_E2E_BIND="127.0.0.1:3417"
```

`DATALENS_E2E_RPC_URL` should point to a stable local fixture-backed JSON-RPC server when
available. If it points to a live public endpoint, treat the run as manual smoke only.

Create a local config:

```sh
mkdir -p .tmp
cat > .tmp/datalens.e2e.toml <<'EOF'
[server]
bind = "${DATALENS_E2E_BIND}"

[storage]
backend = "local"
root = "${DATALENS_E2E_STORAGE_ROOT}"

[planner]
max_query_range_blocks = 10
default_chunk_range_blocks = 2

[writer]
target_object_bytes = 1048576
min_object_rows = 1
record_empty_coverage = true

[chains.ethereum]
kind = "evm"
chain_id = 1
rpc_urls = ["${DATALENS_E2E_RPC_URL}"]

[chains.ethereum.finality]
mode = "auto"

[chains.ethereum.datasets.blocks]
enabled = true
max_batch_blocks = 2

[chains.ethereum.datasets.logs]
enabled = true
max_get_logs_range_blocks = 2
max_addresses_per_query = 4
EOF
```

Start datalens:

```sh
rm -rf "$DATALENS_E2E_STORAGE_ROOT"
cargo run -p datalens-cli -- --config .tmp/datalens.e2e.toml
```

In another shell, set:

```sh
export DATALENS_E2E_BASE_URL="http://${DATALENS_E2E_BIND}"
```

### Startup Checks

Health check:

```sh
curl -sS "$DATALENS_E2E_BASE_URL/health"
```

Expected status: `200`.

Expected response fields:

```json
{"status":"ok"}
```

Configured chains:

```sh
curl -sS "$DATALENS_E2E_BASE_URL/v1/chains"
```

Expected status: `200`.

Expected response fields:

```json
{"chains":["ethereum"]}
```

### Block Query

Choose a small bounded range that the configured provider can serve:

```sh
curl -sS -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "blocks",
    "range": { "from_block": 1, "to_block": 2 },
    "filter": null,
    "include_block": false
  }'
```

Expected status: `200`.

Expected key fields:

```json
{
  "chain": "ethereum",
  "range": { "from_block": 1, "to_block": 2 },
  "cache": {
    "hit_ranges": [],
    "missing_ranges": [{ "from_block": 1, "to_block": 2 }]
  },
  "rows": {
    "dataset": "blocks",
    "rows": [
      {
        "number": 1,
        "hash": "0x...",
        "parent_hash": "0x...",
        "timestamp": 0
      }
    ]
  }
}
```

Validate:

- Every row has `number`, `hash`, `parent_hash`, and `timestamp`.
- Returned block numbers match the requested range.
- Ordering is ascending by `number`.

Run the same request again.

Expected cache fields:

```json
{
  "cache": {
    "hit_ranges": [{ "from_block": 1, "to_block": 2 }],
    "missing_ranges": []
  }
}
```

Cache-hit proof:

- With a fixture-backed provider, verify its request count did not increase for the
  equivalent second query.
- With live RPC, use the response cache fields only as smoke evidence; do not treat it as
  deterministic proof.

### Log Query

Choose an address and topic known to exist in the fixture or provider range:

```sh
curl -sS -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "logs",
    "range": { "from_block": 1, "to_block": 2 },
    "filter": {
      "addresses": ["0x0000000000000000000000000000000000000000"],
      "topics": [
        ["0x0000000000000000000000000000000000000000000000000000000000000000"],
        null,
        null,
        null
      ]
    },
    "include_block": false
  }'
```

Expected status: `200`.

Expected key fields for non-empty results:

```json
{
  "rows": {
    "dataset": "logs",
    "rows": [
      {
        "block_number": 1,
        "block_hash": "0x...",
        "transaction_hash": "0x...",
        "transaction_index": 0,
        "log_index": 0,
        "address": "0x...",
        "topics": ["0x..."],
        "data": "0x...",
        "removed": false
      }
    ]
  }
}
```

Validate:

- Every row has `block_number`, `block_hash`, `transaction_hash`,
  `transaction_index`, `log_index`, `address`, `topics`, `data`, and `removed`.
- Every `block_number` is inside the requested range.
- Every `address` matches the address filter.
- Every filtered topic position satisfies the requested topic filter.
- Ordering is stable by `block_number` and `log_index`.

Run the same request again.

Expected cache fields:

```json
{
  "cache": {
    "hit_ranges": [{ "from_block": 1, "to_block": 2 }],
    "missing_ranges": []
  }
}
```

Cache-hit proof follows the same fixture-backed provider request-count rule as block
queries.

### Empty Log Coverage

Query a range and filter that should return no logs:

```sh
curl -sS -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "logs",
    "range": { "from_block": 3, "to_block": 4 },
    "filter": {
      "addresses": ["0x0000000000000000000000000000000000000001"],
      "topics": [null, null, null, null]
    },
    "include_block": false
  }'
```

Expected status: `200`.

Expected response shape:

```json
{
  "rows": {
    "dataset": "logs",
    "rows": []
  }
}
```

Inspect storage:

```sh
test -f "$DATALENS_E2E_STORAGE_ROOT/manifest.json"
find "$DATALENS_E2E_STORAGE_ROOT" -maxdepth 3 -type f | sort
```

Expected:

- `manifest.json` exists.
- The manifest has an entry for `dataset: "logs"` and `row_count: 0`.
- That empty entry has `object_key: null`.
- No tiny object is written for the empty result.

### Partial Hit

Seed cache with a smaller range:

```sh
curl -sS -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "blocks",
    "range": { "from_block": 5, "to_block": 6 },
    "filter": null,
    "include_block": false
  }'
```

Then query a larger overlapping range:

```sh
curl -sS -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "blocks",
    "range": { "from_block": 5, "to_block": 8 },
    "filter": null,
    "include_block": false
  }'
```

Expected cache fields:

```json
{
  "cache": {
    "hit_ranges": [{ "from_block": 5, "to_block": 6 }],
    "missing_ranges": [{ "from_block": 7, "to_block": 8 }]
  }
}
```

With a fixture-backed provider, verify only the missing range was fetched.

### Error Checks

Invalid chain:

```sh
curl -sS -i -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "not-configured",
    "dataset": "blocks",
    "range": { "from_block": 1, "to_block": 1 },
    "filter": null,
    "include_block": false
  }'
```

Expected status: `422`.

Expected error kind: `Unsupported`.

Range above configured limit:

```sh
curl -sS -i -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "blocks",
    "range": { "from_block": 1, "to_block": 99 },
    "filter": null,
    "include_block": false
  }'
```

Expected status: `400`.

Expected error kind: `InvalidInput`.

Unsupported dataset:

```sh
curl -sS -i -X POST "$DATALENS_E2E_BASE_URL/v1/query" \
  -H 'content-type: application/json' \
  -d '{
    "chain": "ethereum",
    "dataset": "transactions",
    "range": { "from_block": 1, "to_block": 1 },
    "filter": null,
    "include_block": false
  }'
```

Expected status: `400`.

Expected response: JSON deserialization failure before the request reaches query
planning. Native support is currently limited to `blocks` and `logs`.

Provider failure:

- Point `DATALENS_E2E_RPC_URL` at a fixture provider that returns a JSON-RPC error or
  invalid response.
- Query a range that is not already cached.
- Expected status is one of:
  - `502` for `ProviderFailure`
  - `429` for `ProviderLimit` or `RateLimited`
  - `504` for `ProviderTimeout`
- Inspect `manifest.json` and verify the failed missing range was not marked covered.

## Storage Inspection

Inspect local manifest:

```sh
sed -n '1,220p' "$DATALENS_E2E_STORAGE_ROOT/manifest.json"
```

Inspect objects:

```sh
find "$DATALENS_E2E_STORAGE_ROOT" -maxdepth 5 -type f | sort
```

Expected object layout:

```text
manifest.json
objects/blocks/all/<from>-<to>.json
objects/logs/<filter-key>/<from>-<to>.json
```

Empty coverage entries should appear only in `manifest.json` with `row_count: 0` and
`object_key: null`.

## Cleanup

Stop the datalens process, then remove local artifacts:

```sh
rm -rf .tmp/datalens.e2e.toml "$DATALENS_E2E_STORAGE_ROOT"
```

Do not commit `.tmp/`, local storage output, provider logs, or secrets.
