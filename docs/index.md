# Documentation Index

Purpose: Route agents to the smallest correct datalens documentation surface for the
current task.

Audience: Documentation in this repository is written for AI agents and LLM workflows.

## Read order

- Read `README.md` first when you need the repository scope.
- Read `docs/policy.md` for document contracts, placement rules, and naming rules.
- Read `docs/spec/http-api.md` for the HTTP contract.
- Read `docs/spec/data-model.md` for schema invariants.
- Read `docs/spec/configuration.md` for TOML keys.
- Read `docs/spec/metrics.md` for Prometheus names.
- Read `docs/runbook/local-development.md` to start a workstation.
- Read `docs/runbook/deployment.md` for a long-running process.
- Read `docs/runbook/operations.md` for follow halt, budget, and backfill.
- Read `docs/runbook/client-cutover.md` for client switch order. Do not execute production cutover from this tree.
- Read `docs/reference/crates.md` for crate ownership.
- Read `docs/decisions/postgres-redesign.md` for why PostgreSQL replaced the previous store.

## Retrieval rules

- Optimize for agent routing and execution, not narrative flow.
- Keep one authoritative document per topic. Link instead of copying.
- Source code and configuration outrank documentation.
- Start each document with a short routing header that says what the document is for,
  when to read it, and what it does not cover.
