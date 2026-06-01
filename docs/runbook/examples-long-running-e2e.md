# Examples Long-Running E2E Report

Goal: Produce comparable reports for long-running `examples/` E2E workloads that
compare first-run provider fill behavior against second-run cache behavior.

Read this when: You are running every index/example under `examples/` for an extended
duration, normally about 30 minutes per example workload, and need a durable report
format.

Inputs:

- Example workload list from `examples/`.
- Datalens config path and service endpoint.
- Storage backend and prefix.
- RPC provider names with all tokens, query strings, credentials, and secrets redacted.
- First-run metrics collected against cold or empty relevant cache coverage.
- Second-run metrics collected against the same workload and existing cache coverage.

Depends on:

- `docs/runbook/e2e-native-query-flow.md` for deterministic PR readiness E2E checks.
- `docs/spec/technical-architecture/en/03-storage-and-manifest.md` for durable storage
  and manifest terminology.
- `docs/spec/technical-architecture/en/04-query-and-fill-flow.md` for cache fill and hit
  terminology.

Outputs:

- One report containing test metadata, workload inventory, per-example metrics, and a
  first-run versus second-run comparison.
- The report must cover every example, every chain, every dataset, and every configured
  selector exercised by the workload.

## Reporting Rules

- Use this document as the canonical report template; do not invent new top-level
  sections for routine reports.
- Report both `first_run` and `second_run` for the same workload identity.
- Use one workload row per `(example, run_mode, chain, dataset, selector)` tuple.
- Redact secrets before writing the report. RPC providers may be identified by provider
  name, hostname, or internal alias only. Do not include API keys, bearer tokens,
  credentials, signed URLs, query-string tokens, or full secret-bearing endpoint URLs.
- Preserve units in every metric cell. Use `n/a` only when a metric is not applicable to
  that chain, dataset, or storage backend.
- For bounded workloads, report configured target end block or slot. For unbounded
  duration-based workloads, report `unbounded` plus the actual end block or slot reached.
- Durable cache observations must describe finalized or safe coverage only. Reorg,
  latest, or hot-cache observations must not imply durable finality.

## Report Template

```md
# Examples Long-Running E2E Report: <run_id>

## Test Metadata

| Field | Value |
| --- | --- |
| Run id | <run_id> |
| Date/time | <UTC timestamp and timezone> |
| Git commit | <commit SHA> |
| Environment | <local | CI | cluster> |
| Datalens config path | <path> |
| Datalens service endpoint | <scheme://host:port or internal service name> |
| Storage backend and prefix | <backend, bucket/root, prefix> |
| RPC providers | <provider names or aliases only; secrets redacted> |
| Duration target | <normally about 30 minutes per example workload> |

## Executive Summary

| Result | Value |
| --- | --- |
| Examples covered | <count and names> |
| Chains covered | <count and names> |
| Datasets covered | <count and dataset keys> |
| First-run status | <passed | failed | partial> |
| Second-run status | <passed | failed | partial> |
| Main cache comparison | <short statement> |
| Blocking issues | <none or issue refs> |

## Workload Inventory

| Example | Run mode | Chain | Chain kind | Dataset key | Selector kind | Selector value | Start block/slot | Target end block/slot | Actual end block/slot | Actual covered ranges |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| <ormp-client> | <first_run> | <ethereum> | <evm> | <evm.logs> | <contract/event/topic> | <redacted or public selector> | <number> | <number or unbounded> | <number> | <ranges> |
| <ormp-client> | <second_run> | <ethereum> | <evm> | <evm.logs> | <contract/event/topic> | <same selector> | <number> | <number or unbounded> | <number> | <ranges> |
| <degov-client> | <first_run> | <ethereum> | <evm> | <evm.logs> | <contract/event/topic> | <redacted or public selector> | <number> | <number or unbounded> | <number> | <ranges> |
| <degov-client> | <second_run> | <ethereum> | <evm> | <evm.logs> | <contract/event/topic> | <same selector> | <number> | <number or unbounded> | <number> | <ranges> |

## Per-Workload Metrics

Repeat this section for every `(example, chain, dataset, selector)` tuple.

### <example> / <chain> / <dataset key> / <selector summary>

#### Timing Metrics

| Metric | First run | Second run | Notes |
| --- | --- | --- | --- |
| Total wall time | <duration> | <duration> |  |
| Time to first useful data | <duration> | <duration> |  |
| Provider fetch duration | <duration> | <duration> |  |
| Cache read duration | <duration> | <duration> |  |
| Durable write duration | <duration> | <duration> |  |
| Shutdown/staging flush duration | <duration> | <duration> |  |
| Processing rate | <blocks/min, slots/min, rows/min, or events/min> | <same unit> |  |

#### Data Volume Metrics

| Metric | First run | Second run | Notes |
| --- | --- | --- | --- |
| Provider calls | <count> | <count> |  |
| Rows fetched from provider | <count> | <count> |  |
| Rows written to durable or staged cache | <count> | <count> |  |
| Rows read from cache | <count> | <count> |  |
| Business/application rows inserted | <count> | <count> |  |
| Duplicate rows skipped | <count> | <count> |  |
| Invalid or skipped rows | <count> | <count> |  |
| Checkpoint before | <block/slot/range/cursor> | <block/slot/range/cursor> |  |
| Checkpoint after | <block/slot/range/cursor> | <block/slot/range/cursor> |  |

#### Cache Behavior Metrics

| Metric | First run | Second run | Notes |
| --- | --- | --- | --- |
| Cache hit ranges | <ranges> | <ranges> |  |
| Cache miss ranges | <ranges> | <ranges> |  |
| Provider fill ranges | <ranges> | <ranges> |  |
| Durable hit ranges | <ranges> | <ranges> |  |
| Staged hit ranges | <ranges or n/a> | <ranges or n/a> |  |
| Cache hit ratio | <percent> | <percent> |  |
| Provider calls | <count> | <count> |  |

Expected second-run behavior: the same data should primarily read from cache, provider
calls should drop sharply, and storage growth should be near zero for ranges already
covered by the first run.

#### Storage Metrics

| Metric | Before first run | After first run | After second run | Notes |
| --- | --- | --- | --- | --- |
| Object count | <count> | <count> | <count> |  |
| Storage bytes | <bytes> | <bytes> | <bytes> |  |
| New bytes written | <bytes> | <bytes> | <bytes> | Use `0` before first run. |
| Manifest coverage entry count | <count> | <count> | <count> |  |
| Data object count | <count> | <count> | <count> |  |
| Empty coverage count | <count> | <count> | <count> |  |
| Staged object count | <count or n/a> | <count or n/a> | <count or n/a> |  |
| Fragmentation or compaction notes | <notes> | <notes> | <notes> |  |

#### Stability Metrics

| Metric | First run | Second run | Notes |
| --- | --- | --- | --- |
| Error count | <count> | <count> |  |
| Retry count | <count> | <count> |  |
| Rate-limit count | <count> | <count> |  |
| Timeout count | <count> | <count> |  |
| Failed ranges | <ranges or none> | <ranges or none> |  |
| Finality-capped ranges | <ranges or none> | <ranges or none> |  |
| Reorg/hot-cache observations | <observations or none> | <observations or none> | Durable cache must remain finalized/safe only. |

## First-Run vs Second-Run Comparison

Include one row per `(example, chain, dataset, selector)` tuple and optional aggregate
rows when useful.

| Example | Chain | Dataset key | Selector summary | Total duration first | Total duration second | Provider calls first | Provider calls second | Rows processed first | Rows processed second | Cache hit ratio first | Cache hit ratio second | New storage bytes first | New storage bytes second | Business rows inserted first | Business rows inserted second | Duplicate rows skipped first | Duplicate rows skipped second | Errors/retries first | Errors/retries second | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| <example> | <chain> | <dataset> | <selector> | <duration> | <duration> | <count> | <count> | <count> | <count> | <percent> | <percent> | <bytes> | <bytes> | <count> | <count> | <count> | <count> | <errors>/<retries> | <errors>/<retries> | <pass/fail/partial> |

## Findings

- Cache behavior: <state whether second run primarily read from cache>.
- Provider behavior: <state whether provider calls dropped sharply on the second run>.
- Storage behavior: <state whether storage growth was near zero for already covered ranges>.
- Data behavior: <state whether business rows, duplicate skips, and invalid skips match expectations>.
- Stability behavior: <state errors, retries, rate limits, timeouts, failed ranges, and finality caps>.

## Raw Evidence

- Metrics source: <Prometheus snapshot, log path, CLI output, object listing, or dashboard link>.
- Storage listing source: <command or dashboard link>.
- Example logs: <paths or artifact links>.
- Redaction check: <who/what verified secrets were removed>.
```

## How to Read the Report

- Start with `Test Metadata` to confirm the same commit, config, endpoint, storage prefix,
  and provider set were used for both runs.
- Use `Workload Inventory` to verify complete coverage across examples, chains, datasets,
  selectors, and actual covered ranges.
- Use each `Per-Workload Metrics` section to diagnose a specific example, chain, dataset,
  or selector.
- Use `First-Run vs Second-Run Comparison` for acceptance. A healthy second run over
  already covered ranges has sharply lower provider calls, materially higher cache hit
  ratio, and near-zero new storage bytes.
- Treat any second-run provider fills as findings to explain unless they are caused by
  newly reached ranges, finality caps, explicit hot/latest reads, or intentionally
  uncached staged data.
- Treat durable cache entries as valid only for finalized or safe ranges. Record
  reorg-sensitive, latest, or hot-cache behavior as observations, not durable coverage.

## Completeness Checklist

- Every example under `examples/` is present.
- Include Rust SDK business indexer examples such as `ormp-client` and `degov-client`
  when they have chain, contract, event topic, start block, and optional end block
  configured for `datalens serve`.
- Every chain used by each example is present with chain name and chain kind.
- Every dataset key used by each example is present.
- Every selector is described by selector kind and value or redacted public-safe summary.
- Both `first_run` and `second_run` are present for every workload tuple.
- Timing, data volume, cache behavior, storage, and stability metrics are filled.
- The comparison table includes total duration, provider calls, rows processed, cache hit
  ratio, new storage bytes, business rows inserted, duplicate rows skipped, and
  errors/retries.
- For `ormp-client` and `degov-client`, record the application checkpoint as the
  next block number. If `*_RESET_CHECKPOINT=true` is used for a second-run replay,
  record duplicate business writes separately from normal resume runs that skip
  already completed ranges.
- RPC provider identifiers are names or aliases only, and all secrets are redacted.
