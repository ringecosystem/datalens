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
The checked-in production profile starts here so deployment does not automatically
begin draining backlog before the runtime owner has captured inventory and baselines.
It keeps the Phase 1 low-rate limits in the same file, but compaction remains paused
until runtime explicitly flips `enabled` after preflight.

```toml
[storage.compaction]
enabled = false
interval_ms = 300000
min_object_bytes = 1048576
max_merge_ranges = 8
max_tick_duration_ms = 5000
max_candidates_per_tick = 1
max_manifest_entries_per_tick = 1000
delete_source_objects = true
```

Completion standard: backlog inventory is captured by chain, dataset, and selector;
backup is complete; query/write and object-store baselines are available.

## Phase 1: Low-Rate Observation

After Phase 0 is complete, runtime starts the first active drain by changing only
`enabled`:

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

Before each increase, compare the current phase handoff against the baseline window.
Advance only when all gate decisions are `hold_or_increase`:

| Gate | Hold or increase | Pause or rollback |
| --- | --- | --- |
| Query latency | p95 and p99 remain inside the production SLA and error rate is flat. | p95 or p99 breaches SLA for one sustained window, or query errors increase. |
| Write latency | p95 and p99 remain inside the production SLA and error rate is flat. | p95 or p99 breaches SLA for one sustained window, or write errors increase. |
| Object store | Timeout and 5xx rates are flat or declining; RustFS list timeout is gone or lower than baseline. | Timeout or 5xx rate increases materially, especially RustFS list timeout. |
| RustFS capacity | CPU and request queues are flat or declining. | CPU stays saturated or request queues grow. |
| Datalens capacity | CPU, memory, and restart count are flat or declining. | CPU, memory, restart count, or OOM pressure rises materially. |
| Compaction health | Ticks complete or remain bounded `partial`; source cleanup failures do not repeat. | Tick failures, `source_delete_failures`, or cleanup failures repeat across windows. |

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

## Phase Handoff Template

Create one handoff record per phase change or hold decision. Keep it outside the repo if
it contains production metric links or incident system URLs.

| Field | Required content |
| --- | --- |
| Current phase | `phase_0_disabled_hold`, `phase_1_low_rate_observation`, `phase_2_backpressure_gated_increase`, or `phase_3_drain_finish`. |
| Active config | The deployed `storage.compaction` values for `enabled`, `interval_ms`, `max_candidates_per_tick`, `max_manifest_entries_per_tick`, `max_tick_duration_ms`, `max_merge_ranges`, and `delete_source_objects`. |
| Inventory snapshot | Paths or object keys for the before and after `datalens inspect maintenance` JSON files. |
| Backlog delta | Before/after `small_object_count`, `small_object_bytes`, and `manifest_segment_count`, plus the top changed `chain` / `dataset_key` / `selector_fingerprint` groups. |
| SLA status | Query and write p95/p99, error rate, and whether each stayed inside the production SLA for the full window. |
| Object-store status | Timeout rate, 5xx rate, RustFS list timeout rate, and whether each is gone, lower, flat, or worse than baseline. |
| Capacity status | RustFS CPU and queue pressure plus Datalens CPU, memory, restart count, and OOM pressure. |
| Compaction health | Tick status distribution, `processed_candidates`, `compacted_objects`, `deleted_source_objects`, and `source_delete_failures`. |
| Gate decision | `hold_or_increase`, `hold_same_phase`, `pause`, or `rollback_to_previous_phase`. |
| Next action | The exact config change, pause command, rollback target, or next inspection time. |

Minimal handoff example:

```text
Current phase: phase_1_low_rate_observation
Active config: enabled=true interval_ms=300000 max_candidates_per_tick=1 max_manifest_entries_per_tick=1000 max_tick_duration_ms=5000 max_merge_ranges=8 delete_source_objects=true
Inventory snapshot: /tmp/datalens-maintenance-before.json -> /tmp/datalens-maintenance-phase-1.json
Backlog delta: small_object_count 420000 -> 382000; small_object_bytes 96 GiB -> 84 GiB; manifest_segment_count 18000 -> 14200; top groups ethereum/evm.logs/all, ethereum/evm.blocks/all
SLA status: query p95/p99 inside SLA, write p95/p99 inside SLA, error rate flat
Object-store status: timeout flat, 5xx flat, RustFS list timeout lower than baseline
Capacity status: RustFS CPU flat, Datalens CPU/memory flat, no restarts
Compaction health: completed/partial ticks only, source_delete_failures=0
Gate decision: hold_or_increase
Next action: reduce interval_ms to 120000 and observe one baseline window
```
