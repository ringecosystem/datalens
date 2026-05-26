# Chain-Cache Storage Manifest

Purpose: 定义 query-driven structured historical caching 的 durable object storage、chunk
coverage 与 manifest contract。

Status: normative

Read this when: 当你要实现 cache hit/miss checks、chunk writers、manifest updates、storage
migrations，或依赖 durable coverage 的 planner behavior 时阅读本文件。

Not this document: 本文件不定义 final Parquet column schemas，不实现 S3 或 DigitalOcean
Spaces code，不要求 permanent local hot storage，也不冻结 final physical object layout。

Defines:

- Object storage durability authority。
- Provisional object key layout。
- Manifest responsibilities and required coverage meaning。
- Selective chunk coverage semantics。
- Manifest update and migration rules。

Paired translation: `../en/storage-manifest.md`

## Storage Authority

Object storage 是 chain-cache historical chunks 与 manifests 的 durable source of truth。

Local disk 只可作为 temporary query cache、workspace cache、staging space 或
implementation scratch space。

Implementations MUST NOT require local disk to be a persistent hot tier for correctness。

Chunk MUST NOT 被视为 durable coverage，直到它的 object storage write 已经 succeeded，
并且 manifest 记录了该 object 表示的 exact coverage。

Planner 或 cache lookup MUST 从 manifest semantics 与 object storage state 判断 cache
hits 和 misses，而不是从 local files 是否存在判断。

## Provisional Object Layout

The provisional object layout pattern is:

```text
chains/<chain-kind>/<chain-name>/manifest.json
chains/<chain-kind>/<chain-name>/datasets/<dataset>/<schema-version>/<chunk-key>
```

`chain-kind` 标识 chain family 或 adapter class，例如 `evm`。

`chain-name` 标识该 kind 内的具体 chain，例如 `ethereum` 或 `base`。

`dataset` 标识该 key 下存储 chunks 的 normalized dataset kind。

`schema-version` 标识用于编码 chunk 的 dataset schema version。

`chunk-key` 标识一个 concrete stored chunk，该 chunk 属于 declared dataset、schema
version、range 与 filter coverage。

Final physical object layout MAY evolve as implementation requirements become clearer。

即使 object keys、directory depth、file extensions、partition naming 或 chunk key
encoding 之后改变，manifest semantics 仍是 normative。

任何未来 physical layout MUST preserve enough information，使 manifest 能证明同一组
chain identity、dataset、schema version、range、filter coverage、object key 与
checksum/version metadata。

## Manifest Responsibilities

每个 chain manifest MUST identify the chain it governs。

Chain identity MUST include `chain-kind` and `chain-name`。Implementations MAY add
chain-family-specific identifiers，例如 numeric EVM chain IDs，当需要防止歧义时使用。

每个 manifest MUST record the schema version or schema versions that are valid for the
coverage records it contains。

每个 manifest MUST 分别记录 finalized height 与 known height，只要该 chain 同时具备这
两个概念。

`finalized height` 表示 system 愿意作为 final durable historical coverage 处理的最高
block height。

`known height` 表示 system 已经 observed 的最高 block height，即使该 height 尚未 final。

每个 manifest MUST contain coverage records。

每个 coverage record MUST declare:

| Field | Meaning |
| --- | --- |
| `dataset` | Chunk 覆盖的 dataset kind，例如 normalized block、transaction、log、trace 或 balance dataset。 |
| `schema-version` | 用于编码 chunk 的 schema version。 |
| `block-range` | Chunk 覆盖的 inclusive 或 explicitly defined block range。Range boundary convention MUST be stored or globally specified before implementation relies on it。 |
| `filter-coverage` | Chunk 覆盖的 exact filter domain。 |
| `object-keys` | 保存此 coverage record 的 chunk data 的 object key 或 keys。 |
| `checksum` | 每个 object key 的 integrity metadata，例如 content checksum。 |
| `version-metadata` | Storage object version、etag、generation、manifest revision、writer version，或 idempotency 与 migration checks 所需的等价 metadata。 |

Coverage records MAY include extra chain-family or dataset-specific metadata，但该
metadata MUST NOT weaken the required dataset、range、filter、key、checksum 与 version
meaning。

## Selective Coverage Semantics

Coverage 是 selective，不是 chain-global。

Chunk 只存在于 declared `dataset`、`schema-version`、`block-range` 与
`filter-coverage`。

Chunk 的存在 MUST NOT imply coverage for:

- Another dataset。
- Another schema version。
- Blocks outside the declared range。
- Filters outside the declared filter coverage。
- The full chain unless the coverage record explicitly declares a full-chain-equivalent
  filter domain for the requested dataset and range。

Cache hit checks MUST compare the planned request against coverage records by dataset、
schema version、block range 与 filter coverage。

Request 只有在其 requested dataset、schema version、block range 与 filters 被 manifest
coverage 完全包含，且 referenced objects 满足 required checksum/version checks 的部分，
才是 cache hit。

任何未被 declared coverage 完全包含的部分都是 cache miss。

Implementations MUST NOT silently widen coverage during reads。如果 chunk 是针对
`logs` 且只 filtered to one address 写入，该 chunk MUST NOT satisfy a request for all
logs、for a different address，或 for a broader address set。

当 filter domain cannot be represented precisely，writer MUST record conservative
coverage that does not claim more than the chunk can answer。

## Manifest Update Rules

Manifest writes MUST be idempotent。

对同一 chain、dataset、schema version、block range、filter coverage、object keys、
checksum 与 version metadata 重复同一 write，MUST leave manifest semantics unchanged。

Manifest updates MUST NOT silently widen an existing coverage record。

如果 new data covers a broader range or broader filter domain，writer MUST add or replace
coverage with an explicit record whose declared coverage matches the new durable objects。

如果 writer discovers existing coverage metadata is too broad、ambiguous 或 wrong，它
MUST narrow、invalidate、supersede 或 migrate that coverage before planners rely on it。

Coverage records MUST NOT point to objects before the corresponding object writes have
succeeded。

Manifest update ordering MUST prevent readers from treating missing or partially written
objects as durable coverage。

Concurrent writers MUST preserve manifest consistency。Implementations MAY use object
version preconditions、compare-and-swap、manifest revisions、locks，或 equivalent
coordination。

## Schema and Version Migration

Schema changes MUST create a new `schema-version` unless the encoded chunk format remains
backward compatible for every reader that accepts the previous version。

Different schema versions MUST NOT be treated as interchangeable during cache hit checks。

Migration MAY rewrite chunks、add parallel chunks，或 supersede coverage records。

Migration MUST preserve enough metadata for readers to distinguish old coverage from new
coverage。

Migration MUST NOT silently reinterpret old chunks as covering a new dataset、range 或
filter domain。

When a manifest changes its own shape, the manifest version MUST be explicit enough for
readers and migrators to parse, validate, and upgrade records without guessing。

## Non-Goals

This document MUST NOT be used as authority for final Parquet column schemas。

This document MUST NOT be used as authority for S3、DigitalOcean Spaces，或其他 object
storage driver implementation details。

This document MUST NOT be used to require permanent local hot storage。

This document MUST NOT be used as authority for the final physical object layout beyond the
provisional pattern and normative manifest semantics above。
