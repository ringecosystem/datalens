# Production Runbook

Goal: Execute the first datalens production packaging, configuration validation, local
container smoke, storage backup, restore, and release check procedures.

Read this when: You need concrete commands for building or validating a production-shaped
datalens binary/container, checking production configuration, or recovering an object
store prefix.

Preconditions: Rust 1.95 stable, Docker or a compatible container builder for container
smoke, and environment-provided RPC/S3/application secrets for production doctor checks.

Depends on: `docs/spec/production-runtime.md` for the normative boundary and
`docs/runbook/local-rustfs.md` for local S3-compatible test storage.

Verification: Commands below exit successfully and do not print secrets.

## Build Binary

```bash
cargo build --locked --release --package datalens-cli
./target/release/datalens --help
```

Expected output: the binary prints the `serve`, `doctor`, `query`, and `inspect`
subcommands.

## Build Container

```bash
docker build -t datalens:local .
docker run --rm datalens:local --help
```

Expected output: the container starts as the non-root `datalens` user and prints CLI help.

Published release images use `ghcr.io/ringecosystem/datalens` through
`${{ github.repository }}` in the release workflow. Version tag releases publish
`sha-<short-sha>`, `<git-tag>`, and `latest` tags.

## Configure Production

Provide runtime values through environment or the deployment secret mechanism:

```bash
export DATALENS_ETHEREUM_RPC_URL="https://example.invalid"
export DATALENS_S3_BUCKET="datalens-production"
export DATALENS_S3_PREFIX="datalens"
export DATALENS_S3_REGION="auto"
export DATALENS_S3_ENDPOINT_URL="https://s3.example.invalid"
export DATALENS_PUBLIC_APP_TOKEN="replace-with-secret"
export AWS_ACCESS_KEY_ID="replace-with-secret"
export AWS_SECRET_ACCESS_KEY="replace-with-secret"
```

Do not commit concrete values for these variables.

## Doctor Check

```bash
datalens doctor --config config/datalens.production.toml
```

Expected output: JSON with `"status": "ok"`, redacted RPC URLs, configured storage and
runtime settings, and detected safe/finalized height for every configured chain.

## Compaction Rollout Gate

`config/datalens.production.toml` is the GitOps-facing production parameter source for
background compaction. Keep the first production rollout conservative:

- `storage.compaction.enabled = true` starts the controller.
- `storage.compaction.cleanup_enabled = false` keeps reconciliation cleanup deletes
  off during the first rollout.
- `storage.compaction.delete_source_objects = false` preserves source objects even
  after a compacted replacement is published.
- `storage.compaction.max_candidates_per_tick = 1`,
  `storage.compaction.max_tick_duration_ms = 2000`, and
  `storage.compaction.max_merge_ranges = 4` keep each tick small.
- `query.metadata.worker_threads = 2`, `query.metadata.queue_capacity = 1024`, and
  `query.metadata.coalesced_capacity = 256` keep query metadata work bounded so
  backpressure applies before background work can grow unbounded.

Before enabling cleanup, record the baseline and the post-change values for query latency,
write latency, object store timeout/5xx rate, and RustFS CPU. Treat sustained regression
in any of those signals as a rollout stop.

Automatic retreat is built into the compaction worker: failed ticks back off from one
interval to two, four, eight, then sixteen intervals before retrying. For an immediate
manual rollback, set `storage.compaction.enabled = false` to stop the controller. To keep
compaction running but stop reconciliation/source-object cleanup, set
`storage.compaction.cleanup_enabled = false` and
`storage.compaction.delete_source_objects = false`.

## Local Development Profile

```bash
export DATALENS_ETHEREUM_RPC_URL="http://127.0.0.1:8545"
cargo run -p datalens-cli -- doctor --config config/datalens.dev.toml
cargo run -p datalens-cli -- serve --config config/datalens.dev.toml
```

Expected output: doctor succeeds against the configured local RPC fixture or node, and
the service binds `127.0.0.1:3000`.

## Runtime Endpoint Smoke

```bash
curl -fsS http://127.0.0.1:3000/health
curl -fsS http://127.0.0.1:3000/healthz
curl -fsS http://127.0.0.1:3000/metrics
```

Expected output: `/health` and `/healthz` return `{"status":"ok"}` and `/metrics`
returns Prometheus text when metrics are enabled.

## Backup

Back up the complete configured object store prefix as one consistency unit:

```bash
aws s3 sync "s3://$DATALENS_S3_BUCKET/$DATALENS_S3_PREFIX/" \
  "s3://$DATALENS_BACKUP_BUCKET/$DATALENS_BACKUP_PREFIX/"
```

Expected result: manifests, data objects, and usage ledger objects are present under the
backup prefix.

## Restore

Restore into an empty bucket or replacement prefix:

```bash
aws s3 sync "s3://$DATALENS_BACKUP_BUCKET/$DATALENS_BACKUP_PREFIX/" \
  "s3://$DATALENS_S3_BUCKET/$DATALENS_RESTORE_PREFIX/"
export DATALENS_S3_PREFIX="$DATALENS_RESTORE_PREFIX"
datalens doctor --config config/datalens.production.toml
datalens inspect manifest --config config/datalens.production.toml
```

Expected result: doctor succeeds and inspect returns `"status": "ok"` with manifest
entries readable from the restored prefix.

## Release Checks

Run the standard gate:

```bash
just release-check
```

`just release-check` includes the deterministic durable lifecycle and multi-chain
indexing gates, including EVM lifecycle coverage plus Solana, Tron, indexer full
indexing, and CLI index command tests.

Run optional S3 coverage when S3-compatible credentials are available:

```bash
DATALENS_RUN_S3_TESTS=1 just s3-e2e
```

Run the container-only smoke:

```bash
just container-smoke
```

Run the production config doctor smoke:

```bash
just config-doctor-smoke
```
