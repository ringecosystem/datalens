# Chain-Cache Documentation Map

Purpose: Define the stable documentation map, bilingual publication contract, and routing
rules for durable chain-cache specification documents.

Status: normative

Read this when: You are adding, translating, reviewing, or locating a chain-cache spec.

Not this document: This document does not define chain-cache architecture, storage,
query, API, SDK, or implementation behavior beyond routing summaries.

Defines:

- Required bilingual publication for every durable chain-cache spec document.
- Canonical directory layout for chain-cache specs.
- Planned spec topics and the owner question for each topic.
- Translation parity, cross-language linking, and future-document rules.

Paired translation: `../zh-CN/documentation-map.md`

## Bilingual Contract

- Every durable chain-cache spec document MUST be produced in both English and
  Simplified Chinese.
- The canonical language directories are `en` and `zh-CN`.
- The canonical path pattern is `docs/spec/chain-cache/{en,zh-CN}/<topic>.md`.
- A chain-cache spec is not complete until both language files exist and satisfy the
  translation parity rules in this document.
- Temporary notes, issue comments, PR descriptions, and implementation scratch work do not
  become durable chain-cache specs unless they are placed under the canonical path pattern.

## Canonical Directory Layout

```text
docs/spec/chain-cache/
├── en/
│   └── <topic>.md
└── zh-CN/
    └── <topic>.md
```

- `<topic>` MUST be a stable lowercase kebab-case file name.
- The same `<topic>.md` file name MUST exist in both language directories.
- Do not place chain-cache spec documents directly under `docs/spec/chain-cache/`.
- Do not create phase, draft, or issue-number directories for durable chain-cache specs.

## Planned Spec Documents

| Document | Owner question | Informs |
| --- | --- | --- |
| `documentation-map.md` | Where do durable chain-cache specs belong, and how are the bilingual files kept in parity? | HBX-5; all future chain-cache documentation work |
| `architecture.md` | What are the canonical chain-cache module boundaries and request/fill data flow? | Workspace crate creation; module ownership; chain-family extension planning |
| `storage-manifest.md` | What durable object storage, chunk coverage, and manifest semantics decide cache hit/miss correctness? | Storage implementation; planner coverage checks; chunk writer idempotency and migration planning |
| `overview.md` | What is the chain-cache responsibility boundary and what must remain out of scope? | Early architecture framing; implementation planning before storage, query, API, or SDK work |
| `storage.md` | What storage model, persistence contract, and durability invariants must chain-cache implementations satisfy? | Storage implementation phase; schema and migration planning; persistence tests |
| `ingestion.md` | What inputs, normalization rules, and cache population behavior must ingestion satisfy? | Ingestion implementation phase; data validation tests; pipeline integration planning |
| `query.md` | What lookup semantics, filtering rules, ordering rules, and miss behavior must query paths satisfy? | Query implementation phase; correctness tests; performance-oriented follow-up work |
| `query-cache-behavior.md` | What lifecycle, cache-hit, partial-hit, miss, and missing-range materialization behavior must query-driven cache execution satisfy? | Query planner implementation; writer sequencing; manifest coverage tests; response streaming behavior |
| `api.md` | What service or crate-facing API contracts must callers rely on? | API implementation phase; integration tests; caller migration planning |
| `sdk.md` | What SDK-facing behavior, naming, and compatibility expectations must external consumers rely on? | SDK implementation phase; release readiness; compatibility tests |
| `operations.md` | What observable states, limits, and operational invariants must deployments preserve? | Runbook planning; validation gates; production readiness work |

## Translation Parity Rules

- Matching language files MUST use the same heading hierarchy and heading order.
- Matching language files MUST contain the same normative statements.
- Matching language files MUST contain the same examples unless language-specific wording
  is required to make the example correct.
- Matching language files MUST keep the same path names, config keys, command names, field
  names, enum values, and code identifiers.
- When one language changes a normative statement, the paired language file MUST be updated
  in the same change set.
- If exact wording cannot match because of language grammar, preserve the same requirement,
  condition, exception, and scope.

## Cross-Language Links

- Each chain-cache spec SHOULD link to its paired translation near the top of the document.
- English specs link to `../zh-CN/<topic>.md`.
- Simplified Chinese specs link to `../en/<topic>.md`.
- Links to another chain-cache spec SHOULD target the same language directory first.
- When a cross-topic link needs to reference the other language, make that language switch
  explicit in the link text.

## Adding Future Specs

- Add a new durable chain-cache spec only when its owner question is not already answered
  by an existing planned document.
- Add both language files in the same change set using
  `docs/spec/chain-cache/{en,zh-CN}/<topic>.md`.
- Add the new `<topic>.md` row to the planned spec table in both documentation maps.
- Keep routing summaries in indexes and this map; put the normative content in the topic
  spec.
- If a proposed document is procedural, current-state descriptive, or rationale-only, place
  it in the appropriate documentation lane instead of `docs/spec/chain-cache/`.
