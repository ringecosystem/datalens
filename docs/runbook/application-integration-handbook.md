# Application Integration Handbook

Goal: Build and operate an application-owned business indexer that uses the shared
Datalens service through SDK or protocol calls.

Read this when: You are integrating an ORMP-like, DeGov-like, or other product indexer
with Datalens and need the ownership boundary, configuration model, local smoke path,
production checklist, and troubleshooting sequence.

Preconditions:

- A running `datalens serve` service is available, or you can start one from a checked-in
  Datalens server config.
- The application team owns a database, schema migrations, business handlers, and
  checkpoint storage for its indexer.
- The application has an authorized Datalens application identity and token when
  `applications.required = true`.

Depends on:

- `docs/spec/production-runtime.md` for the production Datalens runtime boundary.
- `docs/spec/technical-architecture/en/06-api-sdk-and-compatibility.md` for native API,
  SDK, application identity, authorization, and finality contracts.
- `docs/runbook/e2e-native-query-flow.md` for live validation and E2E cache lifecycle
  procedures.
- `examples/ormp-client/README.md` and `examples/degov-client/README.md` for concrete
  external-business indexer examples.

Verification:

- Local smoke proves that one application fixture can query Datalens, write business DB
  rows or explicitly report a zero-row structure-only result, expose useful metrics, and
  resume from an application checkpoint.
- A repeated run over the same Datalens range must show idempotent application writes and
  Datalens cache-hit behavior for already covered finalized or safe ranges.
- Full E2E readiness uses `docs/runbook/e2e-native-query-flow.md`; do not duplicate the
  full E2E procedure here.

## Product Boundary

Datalens is a shared indexing cache service. A production deployment runs one shared
`datalens serve` service for configured chains, datasets, storage, and application
authorization.

Application indexers are separate services owned by application teams. They call
Datalens through supported SDKs or protocol surfaces, then write product-specific rows
to an application-owned database.

Do not build application indexers by linking Datalens server, runtime, edge, storage,
executor, chain adapter, or internal indexer crates. The supported integration boundary
is the SDK or protocol surface exposed by `datalens serve`, such as `POST /v1/query` or
`POST /native/graphql`.

| Owner | Responsibilities |
| --- | --- |
| Datalens shared service | Chain fetching, query planning, durable cache, staged cache, coverage manifests, object storage layout, native query API, application authorization, quotas, usage ledger, service metrics. |
| Application indexer | Product database schema, migrations, business transforms, event decoding policy, idempotency, checkpoint storage, retry policy, product query API, deployment lifecycle. |

Datalens does not decide business tables. It returns native rows for the requested chain,
dataset, selector, range, and finality contract. The application decides how those rows
become product state.

## SDK Usage Model

The current Rust SDK is a service client. Future TypeScript and Go SDKs should follow the
same model: SDKs wrap Datalens protocol calls and helper concerns, but they do not embed
the Datalens server.

An SDK may provide typed request structs, pagination helpers, retry helpers,
authentication helpers, and response normalization. It must not require application
code to link the Datalens server runtime.

Application indexers need these inputs before making a query:

| Input | Source | Notes |
| --- | --- | --- |
| `endpoint` | Application config or environment | Base URL for `datalens serve`, for example `http://127.0.0.1:3000`; SDKs append transport paths when they own that detail. |
| `application name` | Application config | Sent as the Datalens application identity, normally `x-datalens-application`. |
| `token` | Secret manager or local `.env` | Sent as bearer auth when the shared service requires applications. Do not commit real tokens. |
| `chain identity` | Application workload config | Configured chain name plus chain or network id, such as `base` and `8453`. |
| `dataset` | Application workload config | Native dataset key such as `evm.logs`. |
| `selector/filter` | Application workload config | Dataset selector, such as EVM log address and topic filters. |
| `block range` | Application checkpoint and fixture config | Inclusive start and end range for the current page. |
| `chunk size` | Application config | Must fit Datalens application quota and provider limits. |
| `cursor/checkpoint boundaries` | Application database | Defines where the application resumes after successful business writes. |

For historical business indexers, prefer finalized or safe ranges that Datalens can serve
through durable cache. `durable_only` is the default shape for repeatable historical
indexing. `safe_to_latest` or `latest_only` requests are explicit hot/latest contracts;
they may return live provider or hot-cache segments that are not durable coverage. Do
not advance a long-lived business checkpoint past data that the application cannot
tolerate replaying or reconciling after reorg-sensitive reads.

Chunk ranges should be capped by the smaller of:

- The application quota in the shared Datalens service config.
- The chain dataset provider limits.
- The application handler's transaction and retry budget.
- The finality boundary for the requested durable range.

## Business Database Pattern

Initialize the application database before starting the indexer. Apply migrations from
the application repository or service image, not from Datalens.

Use this loop for each workload:

1. Read the application checkpoint for the consumer or workload identity.
2. Derive the next inclusive range using configured start/end bounds and chunk size.
3. Query Datalens through the SDK or protocol surface.
4. Decode and transform native rows in application code.
5. In one application transaction, write business rows idempotently and update the
   application checkpoint.
6. Advance the checkpoint only after business writes succeed.
7. On restart or replay, tolerate duplicate native rows and duplicate business keys.

Handlers must be idempotent. Use stable unique keys such as transaction hash plus log
index, event cursor, message hash, proposal id plus voter, or another product-owned key.
Duplicate rows after restart, page retry, or explicit replay should be skipped or merged,
not counted twice.

Invalid decoded rows are application handler outcomes. Datalens may return a valid native
log row that the application cannot decode for its business contract. Record or count the
invalid row according to product policy, skip unsafe business writes, and advance the
checkpoint only when that skip decision is committed with the rest of the page outcome.

## Example Patterns

`examples/ormp-client/` demonstrates an ORMP-style external business indexer:

- It queries raw native EVM logs from `datalens serve` through the Rust SDK.
- It decodes `MessageAccepted` in application code.
- It owns SQLite migrations, `ormp_messages`, and `consumer_checkpoints`.
- It writes rows and checkpoint updates in one application transaction.
- It uses unique business keys so replay does not duplicate messages.

`examples/degov-client/` demonstrates a governance indexer:

- It queries raw native EVM logs for `VoteCast` ranges through the Rust SDK.
- It decodes votes locally and stores normalized vote rows plus original SDK event JSON.
- It owns vote tables, proposal projections, migrations, and checkpoints.
- It includes fixture workloads for data-positive live E2E checks across configured
  chains.

Treat these examples as application indexers, not Datalens core runtime code. They are
references for the external-service pattern: Datalens supplies cached native rows, while
the application supplies schema, business semantics, checkpointing, and product APIs.

## Configuration Model

Shared Datalens service config belongs in files such as `config/datalens.compose.toml`,
`config/datalens.dev.toml`, or `config/datalens.production.toml`.

Shared service config owns:

- Server bind address and enabled routes.
- Storage backend, bucket, prefix, region, and endpoint.
- Planner and writer limits.
- Metrics configuration and metrics token.
- Application registry entries, tokens, allowlisted chains, allowlisted datasets,
  operations, and quotas.
- Chain RPC URLs, finality settings, and dataset provider limits.

Application indexer config belongs in application-owned environment variables, config
files, fixture files, or deployment manifests.

Application config owns:

- Datalens endpoint, application name, and application token reference.
- Application database URL and migration execution.
- Workload chain identity, dataset, selector/filter, start/end bounds, chunk size, and
  checkpoint consumer name.
- Business handler settings, retry policy, observability labels, and product API config.

`.env.example` contains placeholders and local defaults only. It must not contain real
secrets. Copy values into an untracked local environment file or deployment secret
manager, then replace placeholder tokens such as `replace-with-local-token` with
environment-specific values outside git.

For local Compose, `config/datalens.compose.toml` defines shared Datalens identities such
as `ormp`, `live-smoke`, and `degov-live`. ORMP and DeGov application commands then pass
matching SDK environment variables such as `DATALENS_ENDPOINT`,
`DATALENS_APPLICATION`, and `DATALENS_TOKEN` plus their own `ORMP_*` or `DEGOV_*`
workload settings.

Fixture files are application-owned workload inputs. For example, a DeGov fixture can
list chain names, contract addresses, and block ranges that the application indexer
should run. The fixture is not a Datalens service config and does not grant access by
itself; the shared service must still authorize the application identity for the selected
chains and datasets.

## Local Smoke Checklist

Use this checklist for a new SDK-based business indexer before production rollout:

1. Start local object storage when using S3-compatible cache. Follow
   `docs/runbook/local-rustfs.md`.
2. Start the shared Datalens service with a local config, for example
   `cargo run -p datalens-cli -- serve --config config/datalens.dev.toml`.
3. Confirm the application identity exists in the service config when
   `applications.required = true`.
4. Run one bounded application fixture through the SDK-based indexer.
5. Verify application DB schema and business rows, or explicitly record that the selected
   fixture was structure-only with zero business rows.
6. Verify the application checkpoint advanced only after the business transaction
   succeeded.
7. Verify Datalens metrics or logs show the expected application label, chain, dataset,
   cache miss/fill, and any provider failures.
8. Restart the application indexer against the same database and verify duplicate rows
   are skipped or merged.
9. Re-run the same Datalens range and verify cache-hit behavior for already covered
   finalized or safe ranges.
10. Use `docs/runbook/e2e-native-query-flow.md` for deterministic PR readiness and live
    E2E cache lifecycle validation.

## Production Checklist

Before deploying an application indexer:

- Provision a Datalens application identity and bearer token in the shared service
  registry.
- Choose a stable application name for auth, metrics, and usage ledger labels.
- Allowlist only the chains, datasets, and operations the application needs.
- Configure range quotas, request rate quotas, and concurrent request quotas for the
  application workload.
- Configure chain RPC access, finality settings, and dataset limits in the shared
  Datalens service.
- Configure the production storage bucket, prefix, region, endpoint, and credentials for
  Datalens cache objects.
- Configure service and application observability separately. Datalens metrics prove
  cache and provider behavior; application metrics prove business rows, duplicates,
  invalid rows, and checkpoint progress.
- Deploy `datalens serve` as the shared cache service.
- Deploy each business indexer as a separate application service with its own database,
  migrations, secrets, scaling policy, and product API.
- Keep application database backups and Datalens object-store backups as separate
  operational concerns.

## Troubleshooting

| Symptom | Check | Action |
| --- | --- | --- |
| Unauthorized application | `applications.required`, application id/name, bearer token, chain allowlist, dataset allowlist | Use the configured application identity and token; add only the required chain and dataset permissions to the shared service config. |
| Empty range | Start/end bounds, finality cap, selector address/topic, known event range | Confirm the range is finalized or safe for durable reads and that the selector matches a contract that emitted the target event. |
| Invalid decoded event rows | ABI, event signature, topic order, payload shape, chain-specific log fields | Treat this as an application handler issue; record invalid rows and keep the Datalens native row path unchanged. |
| RPC failures | Datalens logs, provider errors, chain RPC URL, rate limits, dataset provider limits | Fix shared service chain access or reduce application chunk size; do not hide provider failures by advancing the application checkpoint. |
| Storage unavailable | Datalens storage config, S3 endpoint, bucket, prefix, credentials, `datalens doctor` | Restore shared cache storage access before relying on durable cache-hit behavior. |
| Checkpoint mismatch | Application DB checkpoint, reset flags, fixture start block, successful transaction boundary | Reset only the application checkpoint when replay is intentional; do not edit Datalens manifests to correct business checkpoints. |

## Non-Goals

- Do not revive an in-process custom processor model.
- Do not add application business handlers to the Datalens server runtime.
- Do not expose application-specific index GraphQL APIs from `datalens serve`.
- Do not store application checkpoints in the Datalens durable cache manifest.
- Do not commit real application tokens, RPC secrets, database passwords, private keys, or
  generated local outputs.
