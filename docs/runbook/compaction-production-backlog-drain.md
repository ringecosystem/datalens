# Production Compaction Backlog Drain

Goal: Drain an existing production backlog of small durable objects and manifest
segments with observable, pausable, staged compaction instead of relying on the default
background rate.

Read this when: Production already contains a large fragmented object-store prefix and
`storage.compaction` safety thresholds are ready for use.

Inputs: Production `datalens` binary, production config, metrics access for Datalens and
RustFS, object-store error metrics or logs, and the production S3/RustFS prefix.

Depends on: `docs/runbook/production.md` for config validation and backup/restore,
`docs/runbook/compaction-performance-harness.md` for local latency-impact validation,
and `datalens inspect maintenance` for read-only backlog inventory.

Verification: Each phase shows declining small-object and manifest-segment backlog,
RustFS list timeout rate disappears or materially drops, and Datalens query/write SLA
stays within the production budget.

## Preflight Inventory

1. Back up the complete object-store prefix using `docs/runbook/production.md`.
2. Capture a read-only maintenance snapshot:

   ```bash
   datalens inspect maintenance --config config/datalens.production.toml \
     > /tmp/datalens-maintenance-before.json
   ```

3. Record these fields from `.maintenance.compaction_backlog`:

   | Field | Use |
   | --- | --- |
   | `small_object_count` | Total compactable small objects below `min_object_bytes`. |
   | `manifest_segment_count` | Total manifest segment objects still increasing list pressure. |
   | `chains[].chain` | Chain-level backlog ownership. |
   | `chains[].datasets[].dataset_key` | Dataset-level backlog ownership. |
   | `chains[].datasets[].selectors[]` | Selector-level backlog ownership. |

4. Record runtime baselines before enabling or increasing drain rate:

   | Signal | Baseline window |
   | --- | --- |
   | Datalens query latency p95/p99 and error rate | 30 minutes |
   | Datalens write latency p95/p99 and error rate | 30 minutes |
   | Object-store timeout and 5xx rate, especially RustFS list timeouts | 30 minutes |
   | RustFS CPU and request queue pressure | 30 minutes |
   | Datalens CPU, memory, and restart count | 30 minutes |

## Phase 0: Disabled Hold

Use this phase when preflight inventory or service health is incomplete.

```toml
[storage.compaction]
enabled = false
```

Completion standard: backlog inventory is captured by chain, dataset, and selector;
backup is complete; query/write and object-store baselines are available.

## Phase 1: Low-Rate Observation

The checked-in production profile starts here:

```toml
[storage.compaction]
enabled = true
interval_ms = 300000
min_object_bytes = 1048576
max_merge_ranges = 8
max_tick_duration_ms = 5000
max_candidates_per_tick = 1
max_manifest_entries_per_tick = 1000
delete_source_objects = true
```

Run this phase for at least one baseline window after deploy. Inspect every 15 to 30
minutes:

```bash
datalens inspect maintenance --config config/datalens.production.toml \
  > /tmp/datalens-maintenance-phase-1.json
```

Completion standard: `small_object_count` and `manifest_segment_count` trend downward,
`storage compaction tick completed` logs show `tick_status` as `completed` or bounded
`partial`, RustFS list timeout rate does not increase, and Datalens query/write SLA
remains inside budget.

## Phase 2: Backpressure-Gated Increase

Only increase one control at a time after Phase 1 is stable. Prefer this order:

1. Reduce `interval_ms` from `300000` to `120000`.
2. Increase `max_candidates_per_tick` from `1` to `2`.
3. Increase `max_manifest_entries_per_tick` from `1000` to `2500`.
4. Increase `max_tick_duration_ms` from `5000` to `10000`.

Hold each change for at least one baseline window. The runtime already backs off after
failed compaction ticks by increasing sleep duration for consecutive failures; operator
backpressure must still gate deliberate rate increases based on the pause conditions
below.

Completion standard: backlog falls faster than Phase 1, RustFS timeout and 5xx rates
stay flat or decline, and Datalens CPU, memory, query latency, and write latency remain
inside budget.

## Phase 3: Drain Finish

Use this phase after backlog is small and stable:

```toml
[storage.compaction]
interval_ms = 60000
max_candidates_per_tick = 4
max_manifest_entries_per_tick = 5000
max_tick_duration_ms = 10000
```

Completion standard: remaining `small_object_count` is low and no longer operationally
material, `manifest_segment_count` is low enough that RustFS list timeout is gone or
materially reduced, and no Datalens SLA regression is visible over a full baseline
window.

## Pause And Rollback Conditions

Pause compaction immediately by deploying `storage.compaction.enabled = false` when any
condition below appears during a phase:

| Condition | Why it matters |
| --- | --- |
| Query p95 or p99 breaches production SLA for one sustained window | Compaction is competing with read path or object-store list/read capacity. |
| Write p95 or p99 breaches production SLA for one sustained window | Compaction is competing with manifest publication or object writes. |
| Object-store timeout or 5xx rate increases materially | RustFS/S3 is under backpressure; continuing can amplify retries and list pressure. |
| RustFS CPU stays saturated or request queues grow | Object-store capacity is the bottleneck, not compaction throughput. |
| Datalens CPU, memory, restart count, or OOM pressure rises materially | Service capacity is being consumed by maintenance work. |
| `source_delete_failures` or compaction tick failures repeat across windows | Cleanup or manifest publication is unhealthy and needs investigation first. |

Rollback to the previous phase settings, not directly to a faster phase, after the
signal returns to baseline. Keep the most recent maintenance snapshot and relevant logs
for diagnosis before resuming.

## Handoff State

For each phase handoff, preserve one before/after pair of
`datalens inspect maintenance` JSON files and the matching metric window. The handoff is
complete when it states current phase, active config values, backlog deltas, SLA status,
object-store timeout/5xx status, and whether the next action is hold, pause, rollback,
or increase.
