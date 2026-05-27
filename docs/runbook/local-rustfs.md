# Local RustFS

Goal: Start, stop, validate, and clean the local RustFS object storage environment for
S3-compatible storage integration tests.

Read this when: You need a local object store for `datalens-storage` S3 integration
tests, manual storage checks, or future e2e flows.

Preconditions: Docker Compose is available, ports `9000` and `9001` are free, and local
development secrets are copied from `.env.example` to `.env` if defaults should be
overridden.

Depends on: `docker-compose.yml` for the RustFS service, permission initialization, and
bucket initialization.

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
