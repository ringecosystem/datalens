# Non-EVM First Adapter

Status: accepted

Date: 2026-05-28

Question: Which non-EVM chain family should datalens implement first after the chain
adapter conformance suite: Solana or Tron?

Decision: Implement Solana first, in `crates/adapters/solana`, with slot-based datasets and
adapter JSON rows. Keep Tron as the second candidate after Solana proves that the
chain-neutral contracts can handle a non-block-height ledger model.

Consequences: HBX-60 should implement the Solana adapter MVP below and should not start a
Tron adapter until the Solana work has passed the shared conformance suite.

## Source Basis

- Solana RPC documents commitment levels, public RPC limits, and local validator
  endpoints in the RPC overview:
  <https://solana.com/docs/rpc>.
- Solana HTTP RPC documents `getBlock`, `getBlocksWithLimit`,
  `getSignaturesForAddress`, `getTransaction`, `getSlot`, and `getBlockCommitment`:
  <https://solana.com/docs/rpc/http>.
- Tron consensus documents solidified blocks as irreversible after enough Super
  Representatives build on the block:
  <https://developers.tron.network/docs/concensus>.
- Tron transaction docs distinguish `/wallet/*` latest data from `/walletsolidity/*`
  solidified data:
  <https://developers.tron.network/docs/tron-protocol-transaction>.
- Tron event docs define contract log/event structure and event subscription surfaces:
  <https://developers.tron.network/v4.0/docs/event-decoding-in-transaction-info>.
- TronWeb event docs expose contract event selectors, pagination fingerprints, and
  confirmed/unconfirmed filters:
  <https://tronweb.network/docu/docs/API%20List/event/getEventsByContractAddress/>.

## Candidate Comparison

| Area | Solana | Tron |
| --- | --- | --- |
| Ledger unit | Slot, with optional block height in block payloads. Skipped slots are normal. | Block number, close to EVM-style range semantics. |
| Transactions | `getBlock` can return transactions for a slot; `getTransaction` fetches by signature; `getSignaturesForAddress` supports address-oriented discovery. | Block and transaction APIs are block-number oriented; transaction receipts expose contract result and logs. |
| Events/logs | No EVM-style indexed log table. Program logs and instruction metadata exist inside transaction metadata. Address discovery is via account keys or program-owned account queries, not a native topic log scan. | Smart-contract logs have address, topics, data, and event decoding similar to EVM logs. TronWeb event queries support contract address, event name, block number, timestamps, and confirmation filters. |
| Finality | Native commitment vocabulary: `processed`, `confirmed`, `finalized`. `getSlot` can query the highest slot at a requested commitment. | Latest FullNode data may be unconfirmed; solidified data is exposed through Solidity APIs and is irreversible after SR confirmation. |
| Reorg signal | Block payload includes `blockhash`, `previousBlockhash`, and `parentSlot`. `getBlockCommitment` exposes stake-weighted commitment for a slot. | Block payloads include hash and parent hash. Event subscription payloads include a `removed` flag for deleted logs, but durable historical APIs should use solidified blocks. |
| RPC limits | Public Solana endpoints are shared infrastructure and may return rate-limit or blocked responses; production use needs dedicated/private RPC. Raw address history requires paginated signature discovery. | Tron event queries use pagination fingerprints and confirmation filters; node type matters because FullNode and Solidity data have different finality. |
| Datalens fit | Stresses the current adapter abstraction because slots, instructions, accounts, and program selectors are non-EVM. Good validation target for chain neutrality. | Easier first adapter because block, contract, and topic semantics resemble EVM; weaker as a chain-abstraction stress test. |

## Decision Rationale

Solana is the better first non-EVM adapter because it exercises the abstraction work that
Tron would mostly avoid. The current code already has `LedgerRangeKind::Slot`,
`DatasetSelector::Other`, `QueryRows::AdapterJson`, and Solana dataset keys. A Solana MVP
can prove that datalens supports non-block range kinds, non-topic selectors, adapter JSON
rows, and finalized durable boundaries without changing the durable/hot cache principles.

Tron remains valuable, especially for contract event indexing, but as the first
non-EVM chain it would mostly validate another EVM-like event surface. Tron should follow
once the Solana adapter has confirmed that non-EVM row and selector shapes do not force
core rewrites.

## Solana Dataset Mapping

The Solana MVP should expose these datasets:

| Dataset key | Range kind | Row shape | First selector support | Notes |
| --- | --- | --- | --- | --- |
| `solana.slots` | `slot` | `AdapterJson` | `all` | One row per returned block slot. Include `slot`, `block_height`, `blockhash`, `previous_blockhash`, `parent_slot`, `block_time`, transaction count, and commitment used. |
| `solana.transactions` | `slot` | `AdapterJson` | `solana_address`, `solana_program` | Rows are transaction summaries from `getBlock` or address-discovered signatures. Include signature, slot, block hash, err/status, fee, account keys, loaded addresses, and raw transaction JSON when needed. |
| `solana.instructions` | `slot` | `AdapterJson` | `solana_program` | Rows flatten top-level and inner instructions. Include transaction signature, instruction index path, program id, account indexes/keys, data, and parsed JSON when the provider returns it. |
| `solana.account_updates` | `slot` | `AdapterJson` | `all`, `solana_address`, `solana_program`, `solana_signature` | Rows are finalized balance updates reconstructed from `getBlock` transaction `meta.preBalances`, `meta.postBalances`, `meta.preTokenBalances`, and `meta.postTokenBalances`. Include slot, signature, transaction index, account index, account, update kind, before/after amounts, block hash, source, selector kind, and commitment. |

Do not map Solana rows into `EvmBlocks` or `EvmLogs`. `EvmBlocks` assumes block numbers,
and `EvmLogs` assumes EVM address/topic/data structure. Solana rows should use
`QueryRows::AdapterJson` until a chain-neutral typed row model exists.

`solana.account_updates` is a balance-update dataset, not a complete account-data mutation
feed. The authoritative durable source is finalized `getBlock` with full transaction
details. Standard HTTP RPC metadata is sufficient for lamport and SPL-token balance
updates because those before/after arrays are emitted per transaction; it is not
sufficient for arbitrary account data, owner, executable, or rent epoch state changes.
Those broader account-state updates require a separate provider or index source before
they can be added to this dataset contract.

## Solana Selector Model

Use `DatasetSelector::Other` with stable adapter keys:

| Selector kind | Canonical key shape | Applies to | Meaning |
| --- | --- | --- | --- |
| `solana_all` | `all` | `solana.slots`, `solana.account_updates` | Fetch every available block slot or balance update in the requested slot range. |
| `solana_address` | `address/<base58-pubkey>` | `solana.transactions`, `solana.account_updates` | For transactions, discover finalized signatures referencing an account with `getSignaturesForAddress`, then fetch transaction details. For account updates, match rows whose normalized account key equals the address. |
| `solana_program` | `program/<base58-pubkey>` | `solana.transactions`, `solana.instructions`, `solana.account_updates` | Match transactions or instructions whose account keys or instruction program id include the program. For account updates, return balance rows from matching transactions. |
| `solana_signature` | `signature/<base58-signature>` | `solana.transactions`, `solana.account_updates` | Match one known transaction signature. Useful for deterministic fixtures and narrow lookups, not broad range queries. |

Selector fingerprints must be short digest keys derived from the canonical key. Raw
addresses and signatures must not appear in metrics labels.

## Solana Finality And Reorg Model

Solana durable writes should use `finalized` only in the first MVP:

- `latest_height()` returns `ChainHeight { range_kind: Slot, finality: Latest }` from
  `getSlot` with `processed` or the provider default latest-equivalent commitment.
- `cache_safe_height()` returns `Finalized`, not `Safe`, by calling `getSlot` with
  `finalized`.
- `finalized_height()` returns the same finalized slot boundary.
- Durable fill may write only ranges where `range.end <= finalized_slot`.

The range between latest and finalized can roll back. The hot cache can support Solana in
a later issue by storing per-slot block metadata and validating canonicality with:

- `getBlock(slot, commitment = finalized)` for finalized canonical block metadata.
- `getBlock(slot, commitment = confirmed or processed)` for hot/latest candidates.
- `blockhash`, `previousBlockhash`, and `parentSlot` as the minimum `ReorgSignal`.
- `getBlockCommitment(slot)` as optional diagnostic evidence, not the durable boundary.

This satisfies the HBX-58 conformance intent if the Solana adapter reports
`HeightRangeKind::Slot`, supports canonical slot lookup, and returns stable unsupported
errors for block-range or EVM-log requests.

## Tron Dataset Mapping

Tron should remain the second candidate with this expected mapping:

| Dataset key | Range kind | Row shape | Selector support | Notes |
| --- | --- | --- | --- | --- |
| `tron.blocks` | `block` | `AdapterJson` initially | `all` | Could later map to a chain-neutral block row if one is introduced. Include block number, hash, parent hash, timestamp, witness, and transaction count. |
| `tron.transactions` | `block` | `AdapterJson` | `tron_address`, `tron_contract` | Include transaction id, block number, contract type, result, fee, and raw transaction info. |
| `tron.events` | `block` | `AdapterJson` initially | `tron_contract`, `tron_event`, `tron_topic` | Similar to EVM logs, but keep adapter JSON until address encoding, result decoding, and confirmation source are made explicit. |

Tron finality should use the Solidity/solidified boundary:

- `latest_height()` reads FullNode latest block height.
- `cache_safe_height()` and `finalized_height()` read the latest solidified block through
  Solidity APIs.
- Latest FullNode data above the solidified block has fork risk and belongs only in hot
  cache after Tron canonical block lookup is implemented.

Tron can support durable finalized cache cleanly, but the first implementation would
spend more effort separating FullNode, Solidity, TronGrid, and event service semantics
than validating the non-EVM abstraction.

## MVP Scope For HBX-60

HBX-60 should implement only:

- `crates/adapters/solana` as `datalens-solana`.
- Configurable HTTP JSON-RPC endpoint and chain identity.
- Capabilities for `solana.slots`, `solana.transactions`, and `solana.instructions`.
- `latest_height`, `cache_safe_height`, and `finalized_height` using slot finality.
- Bounded slot-range fetches backed by fixture provider responses.
- Adapter JSON rows with deterministic ordering.
- Provider-limit classification for oversized ranges and address signature pagination
  limits.
- Canonical slot lookup and reorg signal methods for conformance fixtures.

HBX-60 should not implement:

- Public RPC integration tests.
- WebSocket subscriptions.
- Account update streaming.
- Hot cache enablement.
- Decoding every program-specific instruction schema.
- Tron code.

## Required Core And Chain Extensions

Expected changes before or during HBX-60:

- Add `ReorgSignal::slot(...)` or make `ReorgSignal` construction ergonomic for
  `HeightRangeKind::Slot`.
- Ensure `CanonicalBlock` can represent slot-based canonicality, either by documenting
  `height` as range-kind-specific or by adding `range_kind`.
- Add Solana selector constructors or helper functions around `DatasetSelector::Other`
  inside `crates/adapters/solana`; do not add raw Solana types to `datalens-core` unless the
  contract becomes shared.
- Update conformance assertions so they are dataset/range-kind parameterized instead of
  EVM-only.
- Keep Solana rows as `AdapterJson`; do not expand `QueryRows` with typed Solana rows in
  the first adapter.

## Required Conformance Fixtures

Add Solana fixtures that cover:

- Slot range with a skipped slot.
- Finalized slot lower than latest slot.
- Block metadata with `blockhash`, `previousBlockhash`, and `parentSlot`.
- Transaction with top-level instructions and inner instructions.
- Program selector hit and miss.
- Address selector pagination limit and provider-limit classification.
- Same slot with different hash for latest reorg signal behavior.
- Unsupported EVM log selector and unsupported block range errors.

The fixture provider must be local-only and deterministic. It must not call public Solana
RPC endpoints.

## HBX-60 Preconditions

- HBX-58 conformance suite must allow non-EVM dataset keys, `HeightRangeKind::Slot`, and
  `DatasetSelector::Other`.
- The Solana adapter must have a deterministic fixture provider before any durable or hot
  path is enabled.
- Durable cache writes must continue to require `Safe` or `Finalized`, with Solana MVP
  using `Finalized`.
- Hot cache behavior must remain unsupported for Solana until canonical slot lookup and
  rollback metadata are implemented and validated.
