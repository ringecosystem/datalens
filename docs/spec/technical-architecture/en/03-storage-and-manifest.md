# 03 - Storage And Manifest

This step designs how datalens remembers what it has cached. The important point is that
the durable archive is not "whatever files happen to exist locally". The durable archive
is the combination of object storage chunks plus manifest coverage records.

## Storage Role

Object storage is the long-term source of truth. The first implementation can use MinIO
locally and an S3-compatible service such as DigitalOcean Spaces later. The storage layer
should hide provider details behind a small interface so query, planner, and writer code
do not know whether the backend is MinIO, Spaces, R2, S3, or another compatible store.

Local disk is scratch space. It can hold downloaded chunks during a query, staged files
before upload, temporary retry artifacts, and local test fixtures. It should not decide
whether a range is covered. If the process restarts and local scratch files disappear,
the durable state should still be recoverable from object storage and manifests.

## Provisional Object Layout

The exact object layout can evolve, but the first implementation should use a predictable
shape:

```text
chains/<chain-kind>/<chain-name>/
├── manifest.json
└── datasets/
    └── <dataset>/<schema-version>/<coverage-key>/<range-key>.parquet
```

Examples:

```text
chains/evm/ethereum-mainnet/manifest.json
chains/evm/ethereum-mainnet/datasets/evm.logs/parquet-v1/block/addr-topic-7f3a91/018000000-018099999.parquet
chains/evm/darwinia/datasets/evm.blocks/parquet-v1/block/all/000000000-000099999.parquet
```

The layout uses `chain-kind` so future `tron` or `solana` adapters do not need to fit
inside an EVM-only namespace.

Application identity is not part of the first-stage durable object or manifest coverage
key. Applications share cache objects for the same chain, dataset, selector, range, and
finality. The application boundary is used for authentication, authorization, quota
validation, metrics, and future audit or attribution; per-application durable object
layout requires a separate storage design.

`range-key` is only the range portion of the object identity. For EVM block-number ranges,
`018000000-018099999` means blocks `18,000,000` through `18,099,999`, padded so object
listing remains sortable. It does not mean datalens has complete coverage for every
dataset or every contract in that block range.

`coverage-key` identifies the logical coverage shape for that chunk. For full block
headers it can be `all`. For filtered logs it should be a deterministic key derived from
the normalized filter set, such as addresses, topics, and field coverage. A second contract
queried over the same block range should therefore produce a different coverage key unless
the existing coverage already satisfies the second query.

## Manifest Coverage

The manifest describes durable safe or finalized historical coverage. First-stage durable
coverage must never represent latest, unstable, or hot data. A range can be written to
object storage or declared in the manifest only when its end height is less than or equal
to the adapter's safe/finalized height for that chain-family range model.

For EVM block ranges, the durable write invariant is:

```text
range.to_block <= adapter.safe_height().value
```

The adapter safe height must carry `Safe` or `Finalized` finality. Future chain families
may use non-EVM range kinds, but they must expose an equivalent safe/finalized height
before their durable coverage can be recorded.

A coverage entry should include:

- Chain kind, for example `evm`.
- Chain name or configured chain identity, including network id when configured.
- Dataset key, for example `evm.blocks`, `evm.logs`, `tron.events`, or
  `solana.transactions`.
- Covered ledger range, including range kind, start, and end.
- Schema or normalization version.
- Object encoding, for example `parquet-v1`.
- Selector fingerprint for lookup and selector canonical key for audit.
- Field coverage or canonical chunk shape.
- Object key for the durable chunk.
- Size, checksum, checksum algorithm, and write timestamp metadata when available.
- Finality metadata. The durable manifest may record only safe or finalized coverage.

Because datalens caches selected data, a chunk for `logs` with one address filter does not
imply full log coverage for the same block range or coverage for another contract. The
manifest must make that distinction explicit.

Manifest coverage must not be used for non-durable hot query results. Empty coverage has
the same finality requirement as data-object coverage: if the requested range is above the
safe/finalized height, datalens must not write an empty coverage record.

Application identity must not be part of the durable coverage key or data object key. The
durable cache remains shared by chain, dataset, selector, range, encoding, and finality so
two applications querying the same logical data can reuse the same stored object and
manifest coverage.

Application usage attribution belongs in a separate append-only usage ledger. The first
ledger format is JSON Lines under object storage, partitioned by application id, chain,
dataset, range kind, and Unix day. Ledger entries record the normalized application id,
chain identity, dataset key, selector fingerprint and canonical key, requested range,
finality, query outcome, cache outcome, fill outcome, row count, timestamp, and optional
request or trace id. API keys, tokens, raw credentials, and untrusted authentication
headers must not be stored in the ledger.

Ledger events are durable audit and accounting facts, not manifest coverage. A ledger
write must not change query result semantics or manifest coverage semantics, but a failed
ledger write is a storage write failure for the query path because usage attribution is a
durable accounting requirement. The first implementation is append-only: retries may add
additional events unless a future request-id idempotency layer is added.

The normal write path should not decompress an existing range chunk, append another
contract's rows, recompress it, and overwrite the object. That would make concurrent fills
hard to reason about and would turn every new filter into a read-modify-write operation.
The first implementation should write immutable logical chunks keyed by dataset, coverage
key, schema version, and range. Later offline compaction may merge compatible chunks, but
compaction is an optimization and must not be required for correctness.

## Maintenance Boundary

Durable storage maintenance is an explicit operator-triggered boundary. The first
implementation does not run a background scheduler and does not silently rewrite or delete
objects. Maintenance reports use stable JSON so CI and operations scripts can consume the
same output for local and S3-compatible object stores.

Maintenance operation types:

| Operation | First behavior | Writes storage |
| --- | --- | --- |
| `inspect_check` | Parse manifests and validate referenced objects. | No |
| `compact` | Report compatible compaction candidates; explicit execution may rewrite already committed small objects. | Execute only |
| `repair` | Report damaged coverage and recommended repair inputs. | No |
| `retention_prune` | Report retention policy and deletion candidates. | No, dry-run only |
| `usage_ledger_rollup` | Define append-only ledger rollup boundary. | No, model only |

The first CLI entry is `datalens inspect maintenance`. It is read-only and returns a
`maintenance` object containing check issues, compaction candidates, retention dry-run
state, and usage ledger rollup model. Write operations must require an explicit execute
mode or direct operator call; dry-run remains the default for object rewriting or
deletion.

Maintenance check must report:

- Manifest JSON decode failures.
- Manifest entry objects that do not exist.
- Object size and checksum mismatch against manifest metadata.
- Object decode failures.
- Contradictory coverage, such as overlapping logical coverage with incompatible
  finality or object encoding.

Every issue should include the chain, dataset key, selector fingerprint, range, object key
when available, issue kind, and message. Repair is report-only in the first version. It
must not refetch RPC data, delete objects, delete manifest entries, or mark entries damaged
without a future explicit repair command and user confirmation.

Compaction is a fallback maintenance path for already committed durable objects represented
by manifest coverage. It must not be used as the normal answer to below-threshold query
writes; the normal write path stages and flushes compatible rows before durable object
commit. Compaction candidates are compatible only when chain, dataset key, selector
fingerprint, selector canonical key, range kind, finality level, object encoding, and
schema version are compatible. Application identity is not part of durable object identity
in the current storage model, so it is not a compaction grouping key.

Empty coverage must not be converted into a data object. Compaction must preserve coverage
semantics and query results. Executed compaction must write the replacement object
successfully before updating the manifest. Delayed or skipped deletion of replaced objects
must not affect query correctness; old object deletion remains protected by dry-run or a
separate execute workflow.

Retention policy inputs may include chain, dataset key, range age, object age, usage
ledger activity, application attribution, and storage size. Retention must not delete an
object referenced by current manifest coverage. Deleting superseded or obsolete objects
requires proof that no effective manifest entry references the object. Usage ledger
activity may extend retention for recently used ranges, but ledger entries do not create
manifest coverage and do not override the current-object protection rule.

Usage ledger maintenance treats ledger JSON Lines as append-only audit and accounting
events. Future rollups may aggregate by application, chain, dataset, selector, range,
finality, and time partition. Ledger retention is related to durable data retention only
as an input signal; pruning ledger events must not prune durable data by itself. Ledger
entries and maintenance output must not include secrets, tokens, raw credentials, or raw
authorization headers.

## Chunk Range Sizing

Chunk ranges should not be a single global constant. The range size should be chosen per
chain family and dataset with four constraints in mind:

- Provider limit: the range must fit within the RPC/provider query limits for that
  dataset.
- Target object size: the writer should try to avoid producing many tiny objects when a
  sparse filter returns very few rows.
- Maximum scan span: the range must still have an upper bound so one fill task cannot scan
  an unreasonably large history window.
- Reuse boundary: the range should be stable enough that future equivalent queries can
  reuse the same coverage records.

For the first EVM logs implementation, use a configurable default rather than hardcoding a
permanent value. A practical starting point is:

- `max_range_blocks`: the largest range a single fill task may scan, for example
  `100_000` blocks when the provider allows it.
- `target_object_bytes`: the preferred compressed object size, for example 16-64 MiB.
- `min_object_rows`: the preferred minimum row count before flushing a non-empty object.
- `empty_coverage`: a manifest coverage entry for a range/filter pair that produced no
  rows.

Sparse filters are expected. If a contract has no matching logs in a range, the system
should not write a tiny empty Parquet file just to remember the miss. It should record an
empty coverage entry in the manifest, with row count `0` and no data object, so the same
range/filter does not need to be fetched again.

If a non-empty result is still too small, the writer may keep accumulating adjacent ranges
for the same dataset, selector coverage shape, finality level, and range kind until it
reaches the configured flush threshold. It should then delegate one immutable object write
for the combined range to storage and record that exact combined coverage through storage.

The first implementation uses `min_object_rows` as the primary sparse-result merge
threshold and a conservative JSON-encoded row estimate for `target_object_bytes` before
the final object encoding is written. The recorded manifest metadata stores the actual
encoded object size and SHA-256 checksum after storage encodes the object. Empty coverage
continues to be manifest-only and must not synthesize object size, checksum, or write
timestamp fields.

This means `018000000-018099999` is not a universal rule. It is an example of a range key.
The implementation should make the actual range sizing configurable and observable.

## Write Sequence

The writer coordinates this safe sequence, while storage owns object encoding, object key
construction, object bytes, and manifest repository updates:

1. Receive normalized fetched data for one planned fill segment.
2. Verify the segment is within the adapter's safe/finalized height before any durable
   write or manifest update.
3. Merge adjacent compatible segments when doing so improves object size.
4. Ask storage to write a data object when the segment has rows.
5. Ask storage to write only manifest empty coverage when the segment has no rows.
6. Let storage verify or trust the object write according to backend capabilities.
7. Let storage update the manifest coverage entry.
8. Return object metadata, empty coverage, and skipped range summaries to the caller.
9. Make the new coverage visible to future queries.

If the object write fails, manifest coverage must not change. If the object write succeeds
but manifest update fails, retry should be able to converge without corrupting coverage.

The first stage avoids durable rollback by refusing to persist unsafe ranges. If future
work needs stronger canonical-chain proof, coverage records can be extended with block
hash, parent hash, or another chain-family canonicality proof. That proof is an extension
to durable validation, not permission to write unstable latest data into durable coverage.

## What To Implement In This Step

The first storage implementation should include:

- A storage trait for object get, put, exists, list, and delete when needed.
- A local filesystem or MinIO-backed development implementation.
- Manifest data structures in `datalens-core` or `datalens-storage`.
- Coverage matching code that can answer: covered, partial, or missing.
- Idempotent write tests for the same logical chunk.
- Tests proving local scratch files are not treated as durable coverage.

This step does not need to choose every future Parquet column. It needs to make coverage
truth precise enough that query planning can safely depend on it.
