# Production Runtime

Purpose: Define the normative production packaging, configuration, runtime endpoint,
storage, compatibility, and release boundary for datalens.

Status: normative

Read this when: You need to decide whether a datalens binary, container image,
configuration file, runtime exposure, storage backend, or release check is production
compatible.

Not this document: Step-by-step release execution, backup execution, or local object store
setup. Use `docs/runbook/production.md` and `docs/runbook/local-rustfs.md` for procedures.

Defines: Production release artifact, configuration and secret rules, runtime endpoints,
storage backend boundary, manifest compatibility boundary, operations boundary, and
release gates.

## Production Release Artifact

- The production release artifact is the `datalens` binary built from the `datalens-cli`
  package.
- The binary owns both server and operator CLI entry points:
  - `datalens serve --config <path>` starts the HTTP service.
  - `datalens doctor --config <path>` validates configuration and upstream finality
    readiness before service start.
  - `datalens inspect ...` exposes read-only operator inspection commands.
- Release builds must use the workspace lockfile with `cargo build --locked --release
  --package datalens-cli`.
- The Rust toolchain used by the first production packaging boundary is Rust 1.95 on the
  stable channel.
- The first production build uses default crate features only. Adding production feature
  flags requires updating this spec and the release runbook in the same change.
- The container image must copy only the release binary into the runtime stage. Test
  fixtures, `.env` files, local scratch data, archives, private keys, and `target/` must
  stay outside the production image build context or be excluded by `.dockerignore`.

## Configuration And Secrets

- `config/datalens.production.toml` is the production configuration schema example.
- `config/datalens.local.toml` is the local development profile example.
- Production must set `storage.backend = "s3"` and configure `[storage.s3]`.
- Local development may set `storage.backend = "local"` and configure
  `[storage.local]`.
- Secret or environment-specific values must be injected with `${ENV_NAME}` placeholders
  and must not be committed as literal credentials.
- Required secret or environment-specific values include:
  - RPC URLs such as `DATALENS_ETHEREUM_RPC_URL`.
  - S3 bucket, prefix, region, endpoint, and credentials.
  - Application bearer tokens such as `DATALENS_PUBLIC_APP_TOKEN`.
- AWS-compatible credentials are supplied through the runtime environment recognized by
  the AWS SDK, such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
  and related provider-specific variables.
- Missing environment placeholders must fail configuration loading before the server
  starts.
- `datalens doctor --config <path>` is the pre-start validation command. It must fail for
  invalid bind addresses, missing storage backend fields, invalid application boundaries,
  invalid quota values, invalid chain dataset limits, and unavailable safe/finalized chain
  height detection.
- Production must set `applications.required = true`. Local development may set
  `applications.required = false`.

## Runtime Endpoints

- `GET /health` is the orchestration liveness and readiness endpoint for the first
  production boundary. It returns HTTP 200 with `{"status":"ok"}` when the process can
  serve requests.
- `GET /metrics` is the Prometheus scrape endpoint. It returns Prometheus text format
  with content type `text/plain; version=0.0.4` when metrics are enabled.
- `GET /v1/chains` and `GET /v1/discovery` expose configured chain discovery and
  native dataset capability discovery.
- `POST /v1/query` is the REST query transport. It executes the native query contract.
- `POST /graphql` is the GraphQL query and warmup transport when
  `edge.graphql.enabled = true`.
- `GET /graphql/playground` exposes GraphiQL when GraphQL and
  `edge.graphql.playground_enabled` are both enabled.
- REST and GraphQL query operations must expose equivalent query capability over the
  same native contract. GraphQL may reshape inputs and outputs for GraphQL clients, but
  it must not introduce a separate query planner, storage contract, or dataset
  vocabulary.
- Warmup task submission, listing, mutation, and run-once routes are edge operations over
  the warmup service registry. They share the same application authentication and
  authorization boundary as query routes.
- inspect and maintenance writes must not be exposed as default public HTTP routes.
  Operator inspection remains CLI-only through `datalens inspect ...`.
- Runtime logging must continue to use the Rust `log` facade with `tracing-subscriber`
  initialization in the CLI boundary.

## Storage Operations Boundary

- S3-compatible object storage is the production recommendation.
- Local filesystem storage is valid only for local development, smoke tests, and isolated
  fixtures.
- Production S3 configuration must define:
  - `bucket`: object store bucket name.
  - `prefix`: datalens object namespace prefix.
  - `region`: S3-compatible region value.
  - `endpoint_url`: provider endpoint for non-AWS S3-compatible stores.
  - `force_path_style`: path-style addressing switch for S3-compatible stores that need
    it.
- Durable coverage truth is the manifest plus immutable data objects under the configured
  object store prefix. Local scratch files must not be treated as durable coverage.
- Manifest compatibility is strict for the first production boundary: a manifest must
  deserialize through the checked-in `datalens-storage` manifest types and must reject
  malformed object keys, invalid empty coverage row counts, invalid data-object row
  counts, and object encoding/key mismatches.
- Schema compatibility is represented by the object encoding key, currently
  `parquet-v1`. A future incompatible object schema requires a new encoding or schema
  version in object keys and manifest entries.

## Runtime Operations Boundary

- Production startup sequence is: provide secrets through environment, run
  `datalens doctor --config config/datalens.production.toml`, then start
  `datalens serve --config config/datalens.production.toml`.
- Backup and restore are object-store procedures. The minimum supported backup is a
  point-in-time copy of the configured S3 prefix, including manifests, data objects, and
  usage ledger objects.
- Disaster recovery restores the prefix into an empty bucket or replacement prefix,
  points `storage.s3.bucket` and `storage.s3.prefix` at the restored location, runs
  `datalens doctor`, and verifies `datalens inspect manifest`.
- Kubernetes, Helm, GitOps, autoscaling policy, billing platform integration, and complex
  secret management systems are outside this production boundary.
- Release notes or changelog files are not mandatory for every release. Create them only
  when a human release process explicitly needs durable release communication.

## Release Gates

- `just fmt-check`
- `just check`
- `just clippy`
- `just e2e-lifecycle`
- `DATALENS_RUN_S3_TESTS=1 just s3-e2e` when S3-compatible credentials are available.
- `just container-smoke`
- `just config-doctor-smoke`
- `just release-check` is the default aggregate release gate when the required production
  environment variables and upstream RPC are available.
