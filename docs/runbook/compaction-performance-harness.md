# Compaction Performance Harness

Read this when: You need a repeatable local baseline proving durable-cache
compaction does not add unacceptable query or write request impact.

The storage crate includes an integration smoke harness that builds a local
object-store dataset with many small data objects, manifest segments, and
coverage-index entries. It measures `read_rows`, `covered_ranges`, and
`write_rows` before compaction, after compaction, and while compaction is paused
at the source-read, compacted-object-write, replacement-publish, and
source-cleanup phases.

Run the default CI-sized smoke:

```sh
cargo test -p datalens-storage --test compaction_performance_harness -- --nocapture
```

The test prints a single JSON object with per-operation `p95_micros`,
`p99_micros`, `errors`, and total object-store operation counts. The smoke also
fails if read, coverage, or write p99 regresses beyond the configured threshold,
if the post-compaction read p99 exceeds the configured budget, if object-store
operation count exceeds the configured budget, or if any concurrent phase
returns an operation error.

Scale the workload for a heavier local baseline:

```sh
DATALENS_COMPACTION_HARNESS_OBJECTS=512 \
DATALENS_COMPACTION_HARNESS_READS=200 \
DATALENS_COMPACTION_HARNESS_COVERED=200 \
DATALENS_COMPACTION_HARNESS_WRITES=50 \
DATALENS_COMPACTION_HARNESS_P99_MICROS_BUDGET=5000000 \
DATALENS_COMPACTION_HARNESS_OBJECT_OP_BUDGET=50000 \
cargo test -p datalens-storage --test compaction_performance_harness -- --nocapture
```

Use the default smoke as the automated guardrail. Use the scaled command before
P3/P7 compaction changes when you need stronger acceptance evidence for p95,
p99, error rate, and object-store-operation deltas.
