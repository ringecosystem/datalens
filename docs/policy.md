# Documentation Policy

Purpose: Define the repository-wide documentation taxonomy, naming rules, and placement
rules for durable agent-facing content.

Audience: Documentation under `docs/` is written for AI agents and LLM workflows. The
split below is by question type, not by reader type.

## Primary taxonomy

This repository standardizes on four primary documentation lanes:

| Lane | Location | Answers | Holds |
| --- | --- | --- | --- |
| Spec | `docs/spec/` | What must be true? | Contracts, schemas, invariants, conventions, required behavior |
| Runbook | `docs/runbook/` | Which sequence should I execute? | Operational procedures, rollout steps, validation flows, recovery steps |
| Reference | `docs/reference/` | How is it currently organized or implemented? | Repository layout, surface maps, current implementation boundaries |
| Decisions | `docs/decisions/` | Why is it shaped this way? | Durable design choices, tradeoffs, and consequences |

## Lane ownership

- Each documentation lane owns exactly one question type.
- A lane may link to another lane's authority, but it must not restate that lane's
  authoritative content.
- `spec` defines truth, not procedure, current state, or rationale.
- `runbook` defines procedure, not truth, current state, or rationale.
- `reference` defines current state, not truth, procedure, or rationale.
- `decisions` defines rationale, not truth, procedure, or current state.
- If a document starts answering a second question type, split it and link to the
  authoritative lane instead of stretching one document across lanes.

## Placement rules

- If a document defines correctness, it belongs in `docs/spec/`.
- If a document defines operator actions, it belongs in `docs/runbook/`.
- If a document describes current structure, ownership, or implementation boundaries, it
  belongs in `docs/reference/`.
- If a document records durable rationale or tradeoffs, it belongs in `docs/decisions/`.
- Do not duplicate authoritative content across lanes. Link to the source of truth.

## Authoring rules

- Write for AI agents and LLM execution, not for narrative human reading flow.
- Optimize for retrieval, routing, and exact execution over prose style.
- Use stable terms for the same concept. Do not drift between synonyms for important
  state names, commands, files, roles, or surfaces.
- Prefer short declarative bullets, tables, and headers over long mixed-purpose prose.
- Put authority, scope, inputs, outputs, and non-goals near the top of the document when
  they matter.
- Keep commands, paths, state names, labels, and config keys explicit and literal.
- Keep one authoritative document per topic. Other documents should link to it rather
  than paraphrasing it.
- If human readability and machine routing conflict, prefer the machine-readable form.

## Rust code organization

- Keep `src/` focused on implementation code.
- Put Rust tests in the owning crate's `tests/` directory by default.
- Split crate-level tests by behavior or module boundary, using names such as
  `dataset_key.rs`, `ledger_range.rs`, `query_flow.rs`, or `manifest.rs`.
- Do not add large `#[cfg(test)] mod tests` blocks to `src/lib.rs` or `src/main.rs`.
- If a test needs a non-public helper, first decide whether that helper is part of a
  stable public or internal contract; do not widen APIs only to reach incidental
  implementation details.
- Keep each Rust source file at or below 800 lines as a repository style limit.
- When a Rust source file approaches or exceeds 800 lines, split by responsibility,
  move tests into `tests/`, or adjust module boundaries.
- Do not delete necessary behavior or compress readable code only to satisfy the line
  limit.

## Naming rules

- Directory names express document type.
- File names express stable topic.
- Use lowercase kebab-case for document file names.
- Do not encode temporary versions such as `v0`, `v1`, or `draft2` into stable file
  names.
- Do not repeat the directory class in the file name when the topic is already clear.
  Prefer `runtime.md` under `docs/spec/` over `runtime-spec.md`.

## Document headers

Every document should start with a short routing header.

Spec header:

- `Purpose`
- `Status: normative`
- `Read this when`
- `Not this document`
- `Defines`

Runbook header:

- `Goal`
- `Read this when`
- `Preconditions` or `Inputs`
- `Depends on`
- `Verification` or `Outputs`

Reference header:

- `Purpose`
- `Read this when`
- `Not this document`
- `Covers`

Decision header:

- `Status`
- `Date`
- `Question`
- `Decision`
- `Consequences`

## Canonical entry points

- Unified router: `docs/index.md`
- Normative router: `docs/spec/index.md`
- Procedural router: `docs/runbook/index.md`
- Current-state router: `docs/reference/index.md`
- Rationale router: `docs/decisions/index.md`

## Update workflow

- Behavior or schema change: update the relevant spec.
- Procedure change: update the relevant runbook.
- Structural or ownership change: update the relevant reference doc.
- Durable design or packaging change: update the relevant decision doc.
- If a document drifts across lanes, split it instead of stretching one document to do
  several jobs.
- If a document repeats another lane's authority, remove the duplicate text and replace
  it with a link to the source of truth.
