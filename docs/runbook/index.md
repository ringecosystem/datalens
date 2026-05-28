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

- `e2e-native-query-flow.md`: local validation for the initial native EVM block and
  log query flow, including deterministic cache behavior checks and the full durable
  cache lifecycle E2E with optional RustFS/S3-compatible coverage.
- `local-rustfs.md`: local RustFS object storage setup, bucket initialization, S3 test
  variables, stop, and cleanup commands.
