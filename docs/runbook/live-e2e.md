# Live E2E Business Examples

Goal: Validate one local `datalens serve` process, durable object storage, and the
DeGov and ORMP external-business examples with a first provider-fill run and a restarted
same-prefix cache-hit run.

Read this when: You need an executable live E2E sequence for the business examples
against `config/datalens.compose.toml` from a clean checkout after preparing `.env`.

Inputs: Local `.env`, live RPC endpoints, local RustFS or S3-compatible object storage,
application tokens by variable name, and optional application-owned SQLite/PostgreSQL
databases.

Depends on: `docs/runbook/local-rustfs.md` for local object storage,
`docs/runbook/examples-long-running-e2e.md` for the canonical report template,
`docs/spec/production-runtime.md` for the application/runtime ownership boundary, and
the DeGov and ORMP example READMEs for application integration details.

Verification: The first run inserts business rows for data-positive ranges. The
restarted second run uses the same storage prefix, reports duplicate application rows,
has no invalid rows, has durable cache hit ranges and no missing ranges for repeated
ranges, and does not issue upstream provider fetches for already covered ranges.

Not this document: This is not a design note for business indexers and does not define
the native query API contract. Link to the dependencies above instead of duplicating
their background sections.

## Scope

- Service under test: `cargo run -p datalens-cli -- serve --config
  config/datalens.compose.toml`.
- Business examples under test: `datalens-example-degov-client` and
  `datalens-example-ormp-client`.
- Datalens endpoint: `http://127.0.0.1:${DATALENS_SERVER_PORT:-3000}`.
- Native GraphQL endpoint: `/native/graphql`.
- Metrics endpoint: `/metrics`.
- Application index GraphQL endpoints such as `/index/graphql` are external application
  services and are not exposed by `datalens serve`.

## Environment Prerequisites

1. Copy and edit the local environment file:

   ```sh
   cp .env.example .env
   ```

2. Set the required storage variables in `.env`:

   ```sh
   DATALENS_S3_ENDPOINT_URL=http://localhost:9000
   DATALENS_S3_BUCKET=datalens
   DATALENS_S3_REGION=auto
   DATALENS_S3_FORCE_PATH_STYLE=true
   AWS_ACCESS_KEY_ID=datalens-dev
   AWS_SECRET_ACCESS_KEY=datalens-dev-secret
   AWS_REGION=auto
   ```

3. Choose a unique repeatable prefix for this run:

   ```sh
   export DATALENS_RUN_ID="live-e2e-$(date -u +%Y%m%dT%H%M%SZ)"
   export DATALENS_S3_PREFIX="runs/${DATALENS_RUN_ID}"
   ```

   Keep the same `DATALENS_S3_PREFIX` for the first run, server restart, and second run.
   Use a new prefix for a clean provider-fill run. Do not reuse a shared prefix such as
   `local-compose` for reportable E2E evidence. Re-export this prefix after sourcing
   `.env` because `.env` may contain its own default `DATALENS_S3_PREFIX`.

4. Set the application tokens in `.env`. Use variable names only in reports:

   ```sh
   DATALENS_DEGOV_TOKEN=...
   DATALENS_ORMP_TOKEN=...
   DATALENS_METRICS_TOKEN=...
   ```

5. Set the RPC variables required by `config/datalens.compose.toml`:

   ```sh
   DATALENS_ETHEREUM_RPC_URL=...
   DATALENS_ARBITRUM_RPC_URL=...
   DATALENS_BASE_RPC_URL=...
   DATALENS_DARWINIA_RPC_URL=...
   DATALENS_SOLANA_RPC_URL=...
   DATALENS_TRON_RPC_URL=...
   DATALENS_TRONGRID_API_KEY=...
   ```

   DeGov and ORMP use the EVM chains above. Solana and Tron are still required by the
   compose config placeholders. If a provider URL includes credentials or query-string
   tokens, report only the provider name, host, or internal alias.

6. Choose application databases:

   ```sh
   export DEGOV_DATABASE_URL="sqlite:.tmp/${DATALENS_RUN_ID}-degov.sqlite"
   export ORMP_DATABASE_URL="sqlite:.tmp/${DATALENS_RUN_ID}-ormp.sqlite"
   ```

   SQLite is sufficient for this runbook. PostgreSQL is optional when you need to test
   an application mode that supports it.

7. Load the environment before running commands:

   ```sh
   set -a
   . ./.env
   set +a
   export DATALENS_RUN_ID="${DATALENS_RUN_ID:-live-e2e-$(date -u +%Y%m%dT%H%M%SZ)}"
   export DATALENS_S3_PREFIX="runs/${DATALENS_RUN_ID}"
   export DATALENS_ENDPOINT="http://127.0.0.1:${DATALENS_SERVER_PORT:-3000}"
   mkdir -p .tmp/live-e2e
   ```

## Start Local Dependencies

Start RustFS and the optional local PostgreSQL service:

```sh
docker compose up -d rustfs-init postgres
docker compose ps
```

RustFS listens on `http://localhost:9000`. The console listens on
`http://localhost:9001` when enabled. PostgreSQL is optional for this runbook when both
examples use SQLite, but starting it keeps the local dependency set consistent with the
compose environment.

## Start Datalens Server

Start `datalens serve` natively so all `.env` variables are available to
`config/datalens.compose.toml`:

```sh
export RUST_LOG="${RUST_LOG:-datalens=info}"
cargo run -p datalens-cli -- serve \
  --config config/datalens.compose.toml \
  --bind "127.0.0.1:${DATALENS_SERVER_PORT:-3000}" \
  > ".tmp/live-e2e/${DATALENS_RUN_ID}-server-first.log" 2>&1 &
export DATALENS_SERVER_PID=$!
```

Check readiness and metrics:

```sh
curl -fsS "$DATALENS_ENDPOINT/health"
curl -fsS "$DATALENS_ENDPOINT/healthz"
curl -fsS \
  -H "authorization: Bearer ${DATALENS_METRICS_TOKEN:?set DATALENS_METRICS_TOKEN}" \
  "$DATALENS_ENDPOINT/metrics" \
  > ".tmp/live-e2e/${DATALENS_RUN_ID}-metrics-before.prom"
```

Expected readiness output: `{"status":"ok"}`. Expected metrics output: Prometheus text
including Datalens metric names. Capture all server logs under `.tmp/live-e2e/` and
attach or summarize them in the final report.

## Storage Snapshot Helper

Use the RustFS client container to capture object count and storage bytes for the run
prefix:

```sh
docker compose run --rm --entrypoint /bin/sh rustfs-init -c '
  mc alias set rustfs http://rustfs:9000 "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" >/dev/null &&
  mc ls --recursive --json "rustfs/$DATALENS_S3_BUCKET/$DATALENS_S3_PREFIX" |
    sed -n "s/.*\"size\":\([0-9][0-9]*\).*/\1/p" |
    awk "{count += 1; bytes += \$1} END {printf \"objects=%d bytes=%d\\n\", count, bytes}"
' | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-storage-before.txt"
```

Run the same command after the first run and after the restarted second run, changing
the output file suffix to `storage-after-first.txt` and `storage-after-second.txt`.

## First-Run Sequence

Run DeGov across the bundled live fixture chains:

```sh
time env \
  DATALENS_ENDPOINT="$DATALENS_ENDPOINT" \
  DATALENS_APPLICATION=degov-live \
  DATALENS_TOKEN="$DATALENS_DEGOV_TOKEN" \
  DATALENS_TIMEOUT_SECONDS=120 \
  DEGOV_FIXTURES_PATH=examples/degov-client/fixtures/live-votecast.toml \
  DEGOV_DATABASE_URL="$DEGOV_DATABASE_URL" \
  DEGOV_RESET_CHECKPOINT=true \
    cargo run -p datalens-example-degov-client \
  2>&1 | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-degov-first.log"
```

Run ORMP with the data-positive Base fixture:

```sh
time env \
  DATALENS_ENDPOINT="$DATALENS_ENDPOINT" \
  DATALENS_APPLICATION=ormp \
  DATALENS_TOKEN="$DATALENS_ORMP_TOKEN" \
  ORMP_DATABASE_URL="$ORMP_DATABASE_URL" \
  ORMP_CHAIN_NAME=base \
  ORMP_CHAIN_ID=8453 \
  ORMP_CONTRACT_ADDRESS=0x13b2211a7ca45db2808f6db05557ce5347e3634e \
  ORMP_EVENT_TOPIC0=0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18 \
  ORMP_START_BLOCK=30519000 \
  ORMP_END_BLOCK=30520999 \
  ORMP_CHUNK_SIZE=1000 \
  ORMP_RESET_CHECKPOINT=true \
    cargo run -p datalens-example-ormp-client \
  2>&1 | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-ormp-first.log"
```

Capture metrics and storage after the first run:

```sh
curl -fsS \
  -H "authorization: Bearer ${DATALENS_METRICS_TOKEN:?set DATALENS_METRICS_TOKEN}" \
  "$DATALENS_ENDPOINT/metrics" \
  > ".tmp/live-e2e/${DATALENS_RUN_ID}-metrics-after-first.prom"
```

Record for each example and chain:

- Elapsed wall time from `time`.
- Fetched rows from the example summary.
- Inserted rows: `inserted_votes` for DeGov and `inserted` for ORMP.
- Duplicate rows: `skipped_duplicates` for DeGov and `duplicates` for ORMP.
- Invalid rows: `skipped_invalid` for DeGov and `invalid` for ORMP.
- Checkpoint cursor from the example summary.
- Storage object count and bytes from the storage snapshot.
- Metrics deltas for `datalens_cache_coverage_total`,
  `datalens_provider_error_total`, and query/fill metrics present in `/metrics`.

## Restart and Cache-Hit Sequence

Stop the server and wait for staged durable objects to flush:

```sh
kill "$DATALENS_SERVER_PID"
wait "$DATALENS_SERVER_PID" || true
```

Start the server again with the same `DATALENS_S3_PREFIX`:

```sh
cargo run -p datalens-cli -- serve \
  --config config/datalens.compose.toml \
  --bind "127.0.0.1:${DATALENS_SERVER_PORT:-3000}" \
  > ".tmp/live-e2e/${DATALENS_RUN_ID}-server-second.log" 2>&1 &
export DATALENS_SERVER_PID=$!

curl -fsS "$DATALENS_ENDPOINT/health"
```

Re-run DeGov and ORMP over the same ranges and same application databases. Keep
`*_RESET_CHECKPOINT=true` so the application intentionally replays the same business
ranges and reports duplicate rows instead of only resuming past the end:

```sh
time env \
  DATALENS_ENDPOINT="$DATALENS_ENDPOINT" \
  DATALENS_APPLICATION=degov-live \
  DATALENS_TOKEN="$DATALENS_DEGOV_TOKEN" \
  DATALENS_TIMEOUT_SECONDS=120 \
  DEGOV_FIXTURES_PATH=examples/degov-client/fixtures/live-votecast.toml \
  DEGOV_DATABASE_URL="$DEGOV_DATABASE_URL" \
  DEGOV_RESET_CHECKPOINT=true \
    cargo run -p datalens-example-degov-client \
  2>&1 | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-degov-second.log"

time env \
  DATALENS_ENDPOINT="$DATALENS_ENDPOINT" \
  DATALENS_APPLICATION=ormp \
  DATALENS_TOKEN="$DATALENS_ORMP_TOKEN" \
  ORMP_DATABASE_URL="$ORMP_DATABASE_URL" \
  ORMP_CHAIN_NAME=base \
  ORMP_CHAIN_ID=8453 \
  ORMP_CONTRACT_ADDRESS=0x13b2211a7ca45db2808f6db05557ce5347e3634e \
  ORMP_EVENT_TOPIC0=0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18 \
  ORMP_START_BLOCK=30519000 \
  ORMP_END_BLOCK=30520999 \
  ORMP_CHUNK_SIZE=1000 \
  ORMP_RESET_CHECKPOINT=true \
    cargo run -p datalens-example-ormp-client \
  2>&1 | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-ormp-second.log"
```

Capture restarted metrics:

```sh
curl -fsS \
  -H "authorization: Bearer ${DATALENS_METRICS_TOKEN:?set DATALENS_METRICS_TOKEN}" \
  "$DATALENS_ENDPOINT/metrics" \
  > ".tmp/live-e2e/${DATALENS_RUN_ID}-metrics-after-second.prom"
```

Confirm durable cache use after restart:

```sh
rg 'query executor cache summary|hit_ranges=|missing_ranges=|provider fetch failed' \
  ".tmp/live-e2e/${DATALENS_RUN_ID}-server-second.log"
```

Pass evidence for the repeated ranges:

- `hit_ranges` covers each repeated range.
- `missing_ranges=[]` for each repeated range.
- `provider_fill_ranges` is empty in native response metadata when captured by the
  client or logs.
- No upstream provider fetch failure or provider-call log appears for already covered
  ranges in `server-second.log`.
- Second-run metrics increment cache `hit` outcomes without corresponding provider
  error or fill evidence for the same repeated ranges.

## Database Count Checks

For SQLite-backed runs, capture application row counts:

```sh
sqlite3 "${DEGOV_DATABASE_URL#sqlite:}" \
  'select count(*) as degov_votes from degov_votes; select consumer_name, cursor from consumer_checkpoints order by consumer_name;' \
  | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-degov-db.txt"

sqlite3 "${ORMP_DATABASE_URL#sqlite:}" \
  'select count(*) as ormp_messages from ormp_messages; select consumer_name, cursor from consumer_checkpoints order by consumer_name;' \
  | tee ".tmp/live-e2e/${DATALENS_RUN_ID}-ormp-db.txt"
```

If `sqlite3` is not available, record the row counts with any available SQLite client
or report the missing tool and the alternate command used.

## Expected Pass Criteria

- DeGov first run inserts vote rows across Ethereum, Arbitrum, Base, and Darwinia and
  reports `skipped_invalid=0`.
- DeGov second run over the same database and fixture ranges reports duplicate rows and
  `skipped_invalid=0`.
- ORMP first run uses the Base data-positive range above and inserts at least one
  `MessageAccepted` row.
- ORMP second run over the same database and range reports duplicate rows and
  `invalid=0`.
- The restarted same-prefix second run is materially faster than the first run for the
  same ranges.
- The second run does not perform upstream provider fetches for already covered ranges.
- Metrics, server logs, example logs, database counts, storage object count, and storage
  bytes are captured in the final report.

Recent baseline values are examples, not hard assertions unless the fixture ranges stay
pinned:

- DeGov recent live fixture: 811 total vote rows across Ethereum, Arbitrum, Base, and
  Darwinia; invalid rows 0.
- ORMP verified Base fixture: contract
  `0x13b2211a7ca45db2808f6db05557ce5347e3634e`, topic
  `0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18`, range
  `30519000-30520999`, data at block `30519957`.
- Restarted same-prefix runs should show cache hits and no provider fetch logs for
  covered ranges.

## Report Format

Use `docs/runbook/examples-long-running-e2e.md` as the canonical report template. For
this runbook, include these concrete summary tables in the report.

Per-example and per-chain results:

| Example | Chain | Range | Run | Elapsed | Fetched rows | Inserted rows | Duplicate rows | Invalid rows | Checkpoint | Cache hit ranges | Missing ranges | Provider fill ranges | Storage objects | Storage bytes | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| degov-client | ethereum | 23900000-23909999 | first_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | ethereum | 23900000-23909999 | second_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | arbitrum | 435080000-435089999 | first_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | arbitrum | 435080000-435089999 | second_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | base | 21040000-21049999 | first_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | base | 21040000-21049999 | second_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | darwinia | 5370000-5379999 | first_run |  |  |  |  |  |  |  |  |  |  |  |  |
| degov-client | darwinia | 5370000-5379999 | second_run |  |  |  |  |  |  |  |  |  |  |  |  |
| ormp-client | base | 30519000-30520999 | first_run |  |  |  |  |  |  |  |  |  |  |  |  |
| ormp-client | base | 30519000-30520999 | second_run |  |  |  |  |  |  |  |  |  |  |  |  |

Evidence inventory:

| Evidence | Path or value | Redaction check |
| --- | --- | --- |
| Server first-run log | `.tmp/live-e2e/<run_id>-server-first.log` | No RPC credentials or tokens |
| Server second-run log | `.tmp/live-e2e/<run_id>-server-second.log` | No RPC credentials or tokens |
| DeGov first/second logs | `.tmp/live-e2e/<run_id>-degov-*.log` | No bearer tokens |
| ORMP first/second logs | `.tmp/live-e2e/<run_id>-ormp-*.log` | No bearer tokens |
| Metrics snapshots | `.tmp/live-e2e/<run_id>-metrics-*.prom` | Labels only |
| Storage snapshots | `.tmp/live-e2e/<run_id>-storage-*.txt` | Object paths contain no secrets |
| DB counts | `.tmp/live-e2e/<run_id>-*-db.txt` | Counts and cursors only |

## Troubleshooting

| Failure | Likely cause | Check | Fix |
| --- | --- | --- | --- |
| Missing token | Required application token is unset or still a placeholder. | Server startup log or API response mentions authentication or empty token. | Set `DATALENS_DEGOV_TOKEN`, `DATALENS_ORMP_TOKEN`, and `DATALENS_METRICS_TOKEN` in `.env`; reload the environment; restart `datalens serve`. |
| Unauthorized chain | Application identity is not allowed to query the configured chain. | Response mentions unauthorized chain, dataset, or operation. | Use `DATALENS_APPLICATION=degov-live` with `DATALENS_DEGOV_TOKEN` for DeGov and `DATALENS_APPLICATION=ormp` with `DATALENS_ORMP_TOKEN` for ORMP. |
| Empty fixture range | The selected contract/range has no target event rows. | Example summary shows `fetched=0 inserted=0` or equivalent. | For ORMP use the pinned Base range in this runbook. For DeGov use `examples/degov-client/fixtures/live-votecast.toml`. Classify zero-row runs as cache-only, not business-row-positive. |
| RPC errors | Provider URL is unavailable, rate-limited, not archive-capable, or unauthorized. | Server log contains provider error kinds or metrics increment `datalens_provider_error_total`. | Use a provider authorized for the chain and historical range; keep secrets out of logs and reports. |
| RustFS not available | Object storage is down, bucket initialization failed, or credentials do not match. | `docker compose ps`; `curl http://localhost:9000`; storage snapshot command fails. | Run `docker compose up -d rustfs-init`; verify `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, bucket, endpoint, and prefix. |
| GraphQL endpoint unavailable | Server is not running, wrong base URL, or native GraphQL is disabled. | `curl "$DATALENS_ENDPOINT/health"` fails or client cannot reach `/native/graphql`. | Start `datalens serve` with `config/datalens.compose.toml`; set `DATALENS_ENDPOINT` to the base URL, not `/native/graphql`. |
| Second run fetches provider ranges | Prefix changed, first server did not flush staged objects, or range differs. | Compare `DATALENS_S3_PREFIX`, fixture ranges, storage snapshots, and `server-second.log`. | Re-run with the same prefix and ranges; stop the first server with `kill` and `wait` before restart; check for staged flush messages. |

## Stop and Clean Up

Stop the native server:

```sh
kill "$DATALENS_SERVER_PID"
wait "$DATALENS_SERVER_PID" || true
```

Stop local dependencies when no other local workflow is using them:

```sh
docker compose down
```

Keep `.tmp/live-e2e/` artifacts until the final report has been written and redacted.
Remove `.data/oss`, `.data/postgres`, and `.tmp/` only when the run evidence is no
longer needed.
