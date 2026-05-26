# Documentation Index

Purpose: Route agents to the smallest correct datalens documentation surface for the
current task.

Audience: Documentation in this repository is written for AI agents and LLM workflows.
The split below is by question type, not by human-versus-agent audience.

## Read order

- Read `README.md` first when you need the repository scope.
- Read `docs/policy.md` for document contracts, placement rules, and naming rules.
- Then choose one primary lane:
  - `docs/spec/index.md` when the question is "what must be true?"
  - `docs/runbook/index.md` when the question is "which sequence should I execute?"
  - `docs/reference/index.md` when the question is "how is it currently organized?"
  - `docs/decisions/index.md` when the question is "why was it designed this way?"

## Routing matrix

- Need contracts, invariants, schemas, state machines, conventions, or required behavior
  -> `docs/spec/`
- Need the technical architecture development plan and reading order
  -> `docs/spec/technical-architecture/en/00-reading-order.md` or
  `docs/spec/technical-architecture/zh-CN/00-reading-order.md`
- Need runbooks, migrations, validation steps, troubleshooting, or operational sequences
  -> `docs/runbook/`
- Need current repository layout, ownership boundaries, or implementation surface maps
  -> `docs/reference/`
- Need durable design rationale or tradeoff history
  -> `docs/decisions/`
- Need documentation placement or authoring rules
  -> `docs/policy.md`

## Retrieval rules

- Optimize for agent routing and execution, not narrative flow.
- Keep one authoritative document per topic. Link instead of copying.
- Keep runtime authority explicit: source code, configuration, and `docs/spec/` outrank
  runbook, reference, and decision material.
- Start each document with a short routing header that says what the document is for,
  when to read it, and what it does not cover.
- Keep links explicit and stable.
