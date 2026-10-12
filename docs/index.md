# Documentation Index

Purpose: Route agents to the smallest correct datalens documentation surface for the
current task.

Audience: Documentation in this repository is written for AI agents and LLM workflows.

## Read order

- Read `README.md` first when you need the repository scope.
- Read `docs/policy.md` for document contracts, placement rules, and naming rules.
- Read `docs/superpowers/plans/2026-10-12-datalens-postgres-redesign.md` for the
  PostgreSQL redesign: schema, flows, API, configuration, and crate layout. It is the
  authority until the spec, runbook, reference, and decision lanes are rewritten.

## Retrieval rules

- Optimize for agent routing and execution, not narrative flow.
- Keep one authoritative document per topic. Link instead of copying.
- Source code and configuration outrank documentation.
- Start each document with a short routing header that says what the document is for,
  when to read it, and what it does not cover.
