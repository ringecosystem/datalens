# Runbook Index

Goal: Route agents to executable datalens procedures.

Read this when: You need a sequence for setup, validation, rollout, recovery, or
troubleshooting.

Depends on: `docs/policy.md` for placement and authoring rules.

Verification: Each runbook should include concrete validation steps and expected
outputs.

## What belongs in `docs/runbook/`

- Local setup and validation sequences.
- Operational procedures.
- Migration and rollout steps.
- Troubleshooting and recovery flows.

## Current runbooks

- `chain-adapter-conformance.md`: checklist for wiring a chain adapter into the shared
  conformance suite before durable or hot query path enablement.
- `application-integration-handbook.md`: handbook for building and operating
  SDK-based application indexers that call the shared Datalens service while owning their
  own business database, handlers, and checkpoints.
- `e2e-native-query-flow.md`: local validation for the initial native EVM block and
  log query flow, deterministic cache behavior checks, multi-chain indexing gates, and
  the full durable cache lifecycle E2E with optional RustFS/S3-compatible coverage.
- `examples-long-running-e2e.md`: canonical report template for long-running
  `examples/` E2E workloads that compare first-run provider fill behavior with
  second-run cache behavior.
- `local-rustfs.md`: local RustFS object storage setup, bucket initialization, S3 test
  variables, stop, and cleanup commands.
- `postgres-indexer-smoke.md`: opt-in PostgreSQL database setup and indexer smoke
  target for schema creation, idempotent writes, query filters, and GraphQL events.
- `production.md`: production-shaped binary/container build, config doctor, runtime
  endpoint smoke, backup/restore, and release gate commands.
