# Durable Indexing Runtime

Purpose: Define the shared runtime contract for full durable indexing.

Status: normative

Read this when: implementing or reviewing full indexing for EVM, Solana, Tron, or another
chain adapter.

Not this document: This is not a scheduler design, provider implementation guide, or object
storage redesign.

Defines: `IndexJob`, `IndexPlan`, `IndexCursor`, `IndexCheckpoint`,
`IndexDatasetSelection`, `IndexChunk`, `IndexRunMode`, `IndexRunResult`, finality rules,
chunking rules, durable write rules, and indexing accounting.

## Authority

- Code-level contract: `crates/runtime/indexer/src/lib.rs`.
- Durable storage authority: manifest and coverage entries written through the existing
  durable writer and storage path.
- Cursor authority: resume progress only. Cursor state is not durable coverage.
- Chain adapter authority: `datalens_chain::ChainAdapter` capabilities and safe/finalized
  height boundaries.

## Runtime Concepts

| Concept | Required meaning |
| --- | --- |
| `IndexJob` | A single requested indexing run for one chain, one ledger range, one dataset selection, one application id, one run mode, and one retry policy. |
| `IndexPlan` | The executable, chain-neutral expansion of an `IndexJob` into chunks, skipped ranges, verification ranges, retry policy, and a safe/finalized finality boundary. |
| `IndexDatasetSelection` | Dataset scope. `Selected` is already resolved to concrete `DatasetKey` and `DatasetSelector` pairs. `AllSupported` must be resolved from adapter capabilities before planning. |
| `IndexChunk` | The smallest provider fetch and durable write retry boundary. A chunk owns one dataset, one selector, one ledger range, and one retry policy. |
| `IndexCursor` | Persisted resume progress for a job. It points to the next chunk ordinal and the last checkpointed range. It must not be treated as manifest coverage. |
| `IndexCheckpoint` | Accounting and resume marker emitted after chunk processing. It may summarize a durable write, but durable coverage exists only after manifest/storage writes succeed. |
| `IndexRunMode` | One of `backfill`, `resume`, `repair`, or `verify`. |
| `IndexRunResult` | Terminal run accounting: status, checkpoints, provider calls, rows written, skipped ranges, retries, and failures. |

## Run Modes

- `backfill`: fill a specified historical range that is at or below the safe/finalized
  finality boundary.
- `resume`: continue from a persisted `IndexCursor`; the cursor only selects where work
  resumes.
- `repair`: refill gaps discovered from manifest/coverage; repair must use the same
  chunking, finality, durable writer, and accounting rules as backfill.
- `verify`: validate durable coverage and basic object integrity without writing new
  durable data.

## Finality

- Durable writes require `FinalityLevel::Safe` or `FinalityLevel::Finalized`.
- `FinalityLevel::Latest` and chain-specific hot/latest boundaries are never durable
  writable.
- An `IndexPlan` must reject any requested range whose end exceeds the adapter-provided
  safe/finalized boundary.
- EVM and Tron attach with `LedgerRangeKind::Block`.
- Solana attaches with `LedgerRangeKind::Slot`.
- The requested ledger range kind must match the finality boundary range kind.

## Chunking And Retries

- Provider-safe chunk limits come from adapter capabilities or runtime config represented
  as provider limits.
- Chunking is by ledger range kind, not chain family.
- A retry retries one `IndexChunk`; it must not expand to unrelated datasets or ranges.
- Retry policy is explicit on the job and copied onto planned chunks.
- Retry/backoff behavior is runtime policy and must stay chain-neutral.

## Durable Write Contract

- Indexing writes normalized rows through the durable writer.
- Durable writes use the dataset key, selector, ledger range, and finality level from the
  plan/chunk.
- Manifest and coverage remain the source of truth for durable completeness.
- Empty coverage is valid only when the durable writer/storage path records it.
- Skipped ranges are ranges already covered by manifest/coverage or outside the permitted
  finality boundary. Skips are accounting, not new coverage.
- Query-driven fills and full indexing must share the same durable correctness model:
  provider rows are normalized into `DatasetRows`, then written through the durable writer.

## Verification

- `verify` mode must not write durable rows, empty coverage, manifests, or cursors.
- Verification reads manifest/coverage and checks basic object integrity such as object
  presence, row count metadata, checksum metadata when available, and range identity.
- Verification failures are reported in `IndexRunResult` accounting and must not mutate
  durable storage.

## Observability And Accounting

Indexing must account for:

- application id attribution;
- chain, chain kind, dataset, selector, chunk ordinal, and ledger range labels;
- indexed range progress;
- provider calls;
- rows written;
- skipped ranges;
- retries;
- failures.

## Chain Attachment

- Chain-specific implementations attach by translating adapter capabilities into
  `IndexDatasetProviderLimit` values and by normalizing provider responses into
  `DatasetRows`.
- EVM full indexing uses block ranges.
- Solana full indexing uses slot ranges.
- Tron full indexing uses block ranges after the Tron adapter contract exists.
- Chain-specific code must not redefine run modes, cursor semantics, finality rules,
  manifest authority, or durable write semantics.
