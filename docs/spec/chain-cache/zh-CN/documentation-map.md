# Chain-Cache Documentation Map

Purpose: 定义 durable chain-cache specification documents 的稳定文件地图、双语发布契约与路由规则。

Status: normative

Read this when: 当你要新增、翻译、审查或定位 chain-cache spec 时阅读本文件。

Not this document: 本文件不定义 chain-cache architecture、storage、query、API、SDK 或
implementation behavior，除非是路由摘要。

Defines:

- 每一份 durable chain-cache spec document 的必要双语发布要求。
- chain-cache specs 的 canonical directory layout。
- 已规划的 spec topics，以及每一个 topic 的 owner question。
- Translation parity、cross-language linking 与 future-document rules。

Paired translation: `../en/documentation-map.md`

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
| `documentation-map.md` | Durable chain-cache specs 应放在哪里，以及双语文件如何维持 parity？ | HBX-5；所有未来 chain-cache documentation work |
| `architecture.md` | Canonical chain-cache module boundaries 与 request/fill data flow 是什么？ | Workspace crate creation；module ownership；chain-family extension planning |
| `chain-adapter-contract.md` | Chain adapters 必须提供什么，同时如何避免 EVM-specific concepts 进入 chain-neutral modules？ | Chain adapter design；EVM adapter implementation planning；future chain-family extension checks |
| `overview.md` | chain-cache responsibility boundary 是什么，以及哪些内容必须维持 out of scope？ | Early architecture framing；storage、query、API 或 SDK work 之前的 implementation planning |
| `storage.md` | chain-cache implementations 必须满足哪些 storage model、persistence contract 与 durability invariants？ | Storage implementation phase；schema and migration planning；persistence tests |
| `ingestion.md` | ingestion 必须满足哪些 inputs、normalization rules 与 cache population behavior？ | Ingestion implementation phase；data validation tests；pipeline integration planning |
| `query.md` | query paths 必须满足哪些 lookup semantics、filtering rules、ordering rules 与 miss behavior？ | Query implementation phase；correctness tests；performance-oriented follow-up work |
| `query-cache-behavior.md` | query-driven cache execution 必须满足哪些 lifecycle、cache-hit、partial-hit、miss 与 missing-range materialization behavior？ | Query planner implementation；writer sequencing；manifest coverage tests；response streaming behavior |
| `api.md` | callers 必须依赖哪些 service 或 crate-facing API contracts？ | API implementation phase；integration tests；caller migration planning |
| `sdk.md` | external consumers 必须依赖哪些 SDK-facing behavior、naming 与 compatibility expectations？ | SDK implementation phase；release readiness；compatibility tests |
| `operations.md` | deployments 必须保留哪些 observable states、limits 与 operational invariants？ | Runbook planning；validation gates；production readiness work |

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
