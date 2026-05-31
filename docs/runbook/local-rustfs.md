# Local RustFS

Goal: Start, stop, validate, and clean the local RustFS object storage environment and
the optional local Datalens Compose deployment example.

Read this when: You need a local object store for `datalens-storage` S3 integration
tests, manual storage checks, local PostgreSQL, or the local Datalens service Compose
example.

Preconditions: Docker Compose is available, ports `9000` and `9001` are free, and local
development secrets are copied from `.env.example` to `.env` if defaults should be
overridden.

Depends on: `docker-compose.yml` for RustFS, PostgreSQL, bucket initialization, and the
unified Datalens service.

Verification: `docker compose up -d rustfs-init` creates `.data/oss`, starts RustFS, and
creates the configured bucket.

## Start

1. Copy local defaults if needed:

   ```sh
   cp .env.example .env
   ```

2. Start RustFS and initialize the bucket:

   ```sh
   docker compose up -d rustfs-init
   ```

3. Check service status:

   ```sh
   docker compose ps
   ```

RustFS S3 API listens on `http://localhost:9000`. The console listens on
`http://localhost:9001` when `RUSTFS_CONSOLE_ENABLE=true`.

## Local Deployment Example

Start object storage and PostgreSQL:

```sh
docker compose up -d rustfs-init postgres
```

Build and start the local Datalens service with the embedded ORMP index example:

```sh
docker compose --profile datalens up -d --build
```

Check service status:

```sh
docker compose --profile datalens ps
```

The Datalens service listens on `http://localhost:3000`. Native GraphQL is exposed at
`/native/graphql` when enabled, and the ORMP application index GraphQL surface is exposed
at `/index/graphql` when enabled.

## Live Smoke

Set the RPC endpoint for each chain you want to exercise:

```sh
export DATALENS_ETHEREUM_RPC_URL=https://ethereum-rpc.example.invalid
export DATALENS_SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
export DATALENS_TRON_RPC_URL=https://api.trongrid.io
export DATALENS_TRONGRID_API_KEY=
export DATALENS_LIVE_SMOKE_TOKEN=replace-with-live-smoke-token
```

Run the Solana durable cache smoke:

```sh
cargo run -p datalens-cli -- cache backfill \
  --config config/datalens.compose.toml \
  --chain solana-mainnet-beta \
  --dataset transactions \
  --range-kind block \
  --range-start 250000000 \
  --range-end 250000001 \
  --application live-smoke \
  --json

cargo run -p datalens-cli -- cache verify \
  --config config/datalens.compose.toml \
  --chain solana-mainnet-beta \
  --dataset transactions \
  --range-kind block \
  --range-start 250000000 \
  --range-end 250000001 \
  --application live-smoke \
  --json
```

Run the Tron durable cache smoke:

```sh
cargo run -p datalens-cli -- cache backfill \
  --config config/datalens.compose.toml \
  --chain tron-mainnet \
  --dataset events \
  --range-kind block \
  --range-start 60000000 \
  --range-end 60000001 \
  --application live-smoke \
  --address 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c \
  --event-name Transfer \
  --json

cargo run -p datalens-cli -- cache verify \
  --config config/datalens.compose.toml \
  --chain tron-mainnet \
  --dataset events \
  --range-kind block \
  --range-start 60000000 \
  --range-end 60000001 \
  --application live-smoke \
  --address 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c \
  --event-name Transfer \
  --json
```

Run the smoke-specific application index configs:

```sh
cargo run -p datalens-cli -- index doctor --config config/datalens.solana-live-smoke.index.toml
cargo run -p datalens-cli -- index run --config config/datalens.solana-live-smoke.index.toml
cargo run -p datalens-cli -- index doctor --config config/datalens.tron-live-smoke.index.toml
cargo run -p datalens-cli -- index run --config config/datalens.tron-live-smoke.index.toml
```

Run the three chain smokes together after setting the endpoints:

```sh
for DATALENS_SMOKE_CHAIN in ethereum solana-mainnet-beta tron-mainnet; do
  echo "smoke ${DATALENS_SMOKE_CHAIN}"
done
```

Use the JSON summaries to verify `status: "ok"` and the second pass reports existing
durable coverage instead of writing a new range.

## Test Configuration

Use these variables for S3 integration tests:

```sh
export DATALENS_RUN_S3_TESTS=1
export DATALENS_S3_ENDPOINT_URL=http://localhost:9000
export DATALENS_S3_BUCKET=datalens
export DATALENS_S3_PREFIX=dev
export DATALENS_S3_REGION=auto
export DATALENS_S3_FORCE_PATH_STYLE=true
export AWS_ACCESS_KEY_ID=datalens-dev
export AWS_SECRET_ACCESS_KEY=datalens-dev-secret
export AWS_REGION=auto
```

Run the storage integration test:

```sh
cargo test -p datalens-storage --test object_store test_s3_object_store_put_get_exists_list_delete_with_prefix
```

## Stop

```sh
docker compose down
```

## Clean

```sh
docker compose down
rm -rf .data/oss
```

`.data/oss` is local persisted object data and must not be committed.
`.data/postgres` and `.data/indexes` are local PostgreSQL and application index data and
must not be committed.
