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

- Technical architecture reading order and development roadmap:
  - English: `docs/spec/technical-architecture/en/00-reading-order.md`
  - Simplified Chinese: `docs/spec/technical-architecture/zh-CN/00-reading-order.md`
- Technical architecture project direction:
  `docs/spec/technical-architecture/en/01-project-direction.md` and
  `docs/spec/technical-architecture/zh-CN/01-project-direction.md`
- Technical architecture system architecture:
  `docs/spec/technical-architecture/en/02-system-architecture.md` and
  `docs/spec/technical-architecture/zh-CN/02-system-architecture.md`
- Technical architecture storage and manifest plan:
  `docs/spec/technical-architecture/en/03-storage-and-manifest.md` and
  `docs/spec/technical-architecture/zh-CN/03-storage-and-manifest.md`
- Technical architecture query and fill flow:
  `docs/spec/technical-architecture/en/04-query-and-fill-flow.md` and
  `docs/spec/technical-architecture/zh-CN/04-query-and-fill-flow.md`
- Technical architecture chain adapters and EVM plan:
  `docs/spec/technical-architecture/en/05-chain-adapters-and-evm.md` and
  `docs/spec/technical-architecture/zh-CN/05-chain-adapters-and-evm.md`
- Technical architecture API, SDK, and compatibility plan:
  `docs/spec/technical-architecture/en/06-api-sdk-and-compatibility.md` and
  `docs/spec/technical-architecture/zh-CN/06-api-sdk-and-compatibility.md`
- Technical architecture reorg-aware hot cache plan:
  `docs/spec/technical-architecture/en/07-hot-cache-layer.md` and
  `docs/spec/technical-architecture/zh-CN/07-hot-cache-layer.md`
- Production runtime packaging, configuration, endpoint, storage, operations, and release
  boundary:
  `docs/spec/production-runtime.md`
- Durable full-indexing runtime contract:
  `docs/spec/durable-indexing-runtime.md`
