# Chain-Cache Storage Manifest

Purpose: Define the durable object storage, chunk coverage, and manifest contract for
query-driven structured historical caching.

Status: normative

Read this when: You are implementing cache hit/miss checks, chunk writers, manifest
updates, storage migrations, or planner behavior that depends on durable coverage.

Not this document: This document does not define final Parquet column schemas, implement
S3 or DigitalOcean Spaces code, require permanent local hot storage, or freeze the final
physical object layout.

Defines:

- Object storage durability authority.
- Provisional object key layout.
- Manifest responsibilities and required coverage meaning.
- Selective chunk coverage semantics.
- Manifest update and migration rules.

Paired translation: `../zh-CN/storage-manifest.md`

## Storage Authority

Object storage is the durable source of truth for chain-cache historical chunks and
manifests.

Local disk is temporary query cache, workspace cache, staging space, or implementation
scratch space only.

Implementations MUST NOT require local disk to be a persistent hot tier for correctness.

A chunk MUST NOT be treated as durable coverage until its object storage write has
succeeded and the manifest records the exact coverage that object represents.

A planner or cache lookup MUST decide cache hits and misses from manifest semantics and
object storage state, not from the presence of local files.

## Provisional Object Layout

The provisional object layout pattern is:

```text
chains/<chain-kind>/<chain-name>/manifest.json
chains/<chain-kind>/<chain-name>/datasets/<dataset>/<schema-version>/<chunk-key>
```

`chain-kind` identifies the chain family or adapter class, for example `evm`.

`chain-name` identifies the specific chain within that kind, for example `ethereum` or
`base`.

`dataset` identifies the normalized dataset kind whose chunks are stored under the key.

`schema-version` identifies the dataset schema version used to encode the chunk.

`chunk-key` identifies one concrete stored chunk for a declared dataset, schema version,
range, and filter coverage.

Final physical object layout MAY evolve as implementation requirements become clearer.

Manifest semantics are normative even if object keys, directory depth, file extensions,
partition naming, or chunk key encoding change later.

Any future physical layout MUST preserve enough information for the manifest to prove the
same chain identity, dataset, schema version, range, filter coverage, object key, and
checksum/version metadata.

## Manifest Responsibilities

Each chain manifest MUST identify the chain it governs.

Chain identity MUST include `chain-kind` and `chain-name`. Implementations MAY add
chain-family-specific identifiers, such as numeric EVM chain IDs, when needed to prevent
ambiguity.

Each manifest MUST record the schema version or schema versions that are valid for the
coverage records it contains.

Each manifest MUST record finalized height and known height separately when both concepts
are available for the chain.

`finalized height` means the highest block height the system is willing to treat as final
for durable historical coverage.

`known height` means the highest block height the system has observed, even if that height
is not final.

Each manifest MUST contain coverage records.

Each coverage record MUST declare:

| Field | Meaning |
| --- | --- |
| `dataset` | The dataset kind covered by the chunk, such as a normalized block, transaction, log, trace, or balance dataset. |
| `schema-version` | The schema version used to encode the chunk. |
| `block-range` | The inclusive or explicitly defined block range covered by the chunk. The range boundary convention MUST be stored or globally specified before implementation relies on it. |
| `filter-coverage` | The exact filter domain covered by the chunk. |
| `object-keys` | The object key or keys that hold the chunk data for this coverage record. |
| `checksum` | Integrity metadata for each object key, such as a content checksum. |
| `version-metadata` | Storage object version, etag, generation, manifest revision, writer version, or equivalent metadata needed for idempotency and migration checks. |

Coverage records MAY include extra chain-family or dataset-specific metadata, but that
metadata MUST NOT weaken the required dataset, range, filter, key, checksum, and version
meaning.

## Selective Coverage Semantics

Coverage is selective, not chain-global.

A chunk exists only for the declared `dataset`, `schema-version`, `block-range`, and
`filter-coverage`.

The existence of a chunk MUST NOT imply coverage for:

- Another dataset.
- Another schema version.
- Blocks outside the declared range.
- Filters outside the declared filter coverage.
- The full chain unless the coverage record explicitly declares a full-chain-equivalent
  filter domain for the requested dataset and range.

Cache hit checks MUST compare the planned request against coverage records by dataset,
schema version, block range, and filter coverage.

A request is a cache hit only for the portion whose requested dataset, schema version,
block range, and filters are fully contained by manifest coverage and whose referenced
objects satisfy required checksum/version checks.

A request is a cache miss for any portion that is not fully contained by the declared
coverage.

Implementations MUST NOT silently widen coverage during reads. If a chunk was written for
`logs` filtered to one address, that chunk MUST NOT satisfy a request for all logs, for a
different address, or for a broader address set.

When a filter domain cannot be represented precisely, the writer MUST record conservative
coverage that does not claim more than the chunk can answer.

## Manifest Update Rules

Manifest writes MUST be idempotent.

Repeating the same write for the same chain, dataset, schema version, block range, filter
coverage, object keys, checksum, and version metadata MUST leave manifest semantics
unchanged.

Manifest updates MUST NOT silently widen an existing coverage record.

If new data covers a broader range or broader filter domain, the writer MUST add or replace
coverage with an explicit record whose declared coverage matches the new durable objects.

If a writer discovers that existing coverage metadata is too broad, ambiguous, or wrong, it
MUST narrow, invalidate, supersede, or migrate that coverage before planners rely on it.

Coverage records MUST NOT point to objects before the corresponding object writes have
succeeded.

Manifest update ordering MUST prevent readers from treating missing or partially written
objects as durable coverage.

Concurrent writers MUST preserve manifest consistency. Implementations MAY use object
version preconditions, compare-and-swap, manifest revisions, locks, or equivalent
coordination.

## Schema and Version Migration

Schema changes MUST create a new `schema-version` unless the encoded chunk format remains
backward compatible for every reader that accepts the previous version.

Different schema versions MUST NOT be treated as interchangeable during cache hit checks.

Migration MAY rewrite chunks, add parallel chunks, or supersede coverage records.

Migration MUST preserve enough metadata for readers to distinguish old coverage from new
coverage.

Migration MUST NOT silently reinterpret old chunks as covering a new dataset, range, or
filter domain.

When a manifest changes its own shape, the manifest version MUST be explicit enough for
readers and migrators to parse, validate, and upgrade records without guessing.

## Non-Goals

This document MUST NOT be used as authority for final Parquet column schemas.

This document MUST NOT be used as authority for S3, DigitalOcean Spaces, or other object
storage driver implementation details.

This document MUST NOT be used to require permanent local hot storage.

This document MUST NOT be used as authority for the final physical object layout beyond the
provisional pattern and normative manifest semantics above.
