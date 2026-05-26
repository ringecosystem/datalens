# Spec Index

Purpose: Route agents to normative documents that define datalens repository truth.

Question this index answers: "what must remain true?"

## Use this index when

- You need an invariant, contract, schema, enum, state model, interface, convention, or
  required behavior.
- You are deciding whether code, configuration, or a technical proposal is correct.
- A guide says "see the governing spec" and you need the authoritative source.

## Do not use this index when

- You need step-by-step instructions, maintenance actions, migrations, or incident
  response.
- You want rationale only, without an authoritative contract.
- You need current layout or implementation boundaries; read `docs/reference/index.md`.
- You need design rationale or tradeoffs; read `docs/decisions/index.md`.

## What belongs in `docs/spec/`

- Contracts and invariants.
- Data shapes, canonical field names, enums, defaults, units, and limits.
- State transitions and protocol rules.
- Project conventions that tests, code, docs, and operators should treat as
  authoritative.

## Spec document contract

Start each spec with a compact routing header:

- `Purpose`
- `Status: normative`
- `Read this when`
- `Not this document`
- `Defines`

Then keep the body explicit:

- Prefer concrete nouns over pronouns.
- Separate facts from rationale.
- Include canonical names exactly as code or data uses them.
- Include a small example when it removes ambiguity.
- Link to related guides instead of embedding procedures.

## Current governing specs

- Chain-cache project charter:
  - English: `docs/spec/chain-cache/en/project-charter.md`
  - Simplified Chinese: `docs/spec/chain-cache/zh-CN/project-charter.md`
- Chain-cache documentation map and bilingual contract:
  `docs/spec/chain-cache/en/documentation-map.md` and
  `docs/spec/chain-cache/zh-CN/documentation-map.md`
- Chain-cache architecture and module boundaries:
  `docs/spec/chain-cache/en/architecture.md` and
  `docs/spec/chain-cache/zh-CN/architecture.md`
- Chain-cache storage manifest and selective coverage semantics:
  `docs/spec/chain-cache/en/storage-manifest.md` and
  `docs/spec/chain-cache/zh-CN/storage-manifest.md`
