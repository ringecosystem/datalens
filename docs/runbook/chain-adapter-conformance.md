# Chain Adapter Conformance

Goal: Explain how to attach a chain adapter implementation to the shared conformance
suite before enabling it in durable or hot query paths.

Read this when: You are adding or changing an implementation of `ChainAdapter`.

Preconditions: The adapter can run against a deterministic fixture or mock provider and
does not require public RPC access for contract tests.

Depends on: `crates/chain/src/lib.rs` for the adapter contract and
`crates/chain-conformance/src/lib.rs` for reusable assertions.

Verification: Run the adapter-specific conformance test plus `cargo test --workspace`.

## Checklist

- Add the adapter crate as a test target that can construct the adapter with fixture
  provider URLs or in-memory provider state.
- Add `datalens-chain-conformance` as a `dev-dependency`.
- Build a fixture provider that can return deterministic data for supported datasets,
  finality boundaries, provider limit errors, and reorg signals.
- Call the reusable assertions from an integration test:
  - `assert_capability_conformance`
  - `assert_fetch_conformance`
  - `assert_finality_conformance`
  - `assert_reorg_signal_conformance`
  - `assert_metadata_conformance`
- Keep adapter-specific fixture construction inside the adapter crate's `tests/`
  directory unless the fixture is reusable across chain families.
- Verify unsupported datasets, selector kinds, range kinds, finality methods, and reorg
  signals return stable `DatalensErrorKind` values.
- Verify fetched rows are stable sorted, limited to the requested range, and carry the
  minimum metadata needed by durable cache, hot cache, and promotion flows.
- Do not enable durable cache writes unless the adapter returns a safe or finalized
  boundary accepted by `validate_durable_range`.
- Do not enable hot query paths unless the adapter capability declares reorg signals and
  the conformance suite can read block or chain-family equivalent hash and parent data.

## Current Implementation

- `datalens-chain-conformance` owns the reusable assertion helpers and fixture model.
- `crates/evm/tests/conformance.rs` covers the EVM adapter.
- `crates/solana/tests/conformance.rs` covers the Solana slot-based adapter.
- `crates/tron/tests/conformance.rs` covers the Tron block-based MVP adapter.
- The EVM fixture provider is local-only and serves JSON-RPC responses from a loopback
  test server.
- Solana and Tron use deterministic in-memory fixture providers by default, so
  conformance tests do not require public RPC access.
