use datalens_chain::{
    AdapterCapabilities, ChainHeight, DatasetCapability, FinalityKind, HeightRangeKind,
    SelectorKind,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, LedgerRange, NetworkId,
};

use datalens_planner::*;

#[test]
fn test_native_query_input_is_planner_boundary() {
    let input = NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_logs(),
        ledger_range: LedgerRange::blocks(10, 12).expect("valid range"),
        selector: datalens_chain::DatasetSelector::try_evm_logs(datalens_core::LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None],
        })
        .expect("valid selector"),
        response_shape: ResponseShape::LegacyEvmLogs,
        field_selection: FieldSelection::All,
    };

    assert_eq!(input.chain, ethereum_identity());
    assert_eq!(input.dataset_key, DatasetKey::evm_logs());
    assert_eq!(
        input.ledger_range,
        LedgerRange::blocks(10, 12).expect("valid range")
    );
    assert_eq!(input.selector.kind(), SelectorKind::EvmLogs);
    assert_eq!(input.response_shape, ResponseShape::LegacyEvmLogs);
}

#[test]
fn test_native_planner_rejects_range_beyond_durable_boundary() {
    let input = NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        ledger_range: LedgerRange::blocks(9, 10).expect("valid range"),
        selector: datalens_chain::DatasetSelector::all(),
        response_shape: ResponseShape::LegacyEvmBlocks,
        field_selection: FieldSelection::All,
    };

    let error = NativePlanner::new(NativePlannerConfig {
        max_query_range_len: 10,
        default_chunk_range_len: 2,
    })
    .plan(
        input,
        &capabilities(),
        ChainHeight::block(9).with_finality(FinalityKind::Safe),
    )
    .expect_err("unsafe range is rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("safe/finalized height"));
}

#[test]
fn test_native_planner_builds_executable_plan_from_capabilities() {
    let input = NativeQueryInput {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_logs(),
        ledger_range: LedgerRange::blocks(1, 4).expect("valid range"),
        selector: datalens_chain::DatasetSelector::try_evm_logs(datalens_core::LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None],
        })
        .expect("valid selector"),
        response_shape: ResponseShape::LegacyEvmLogs,
        field_selection: FieldSelection::All,
    };

    let plan = NativePlanner::new(NativePlannerConfig {
        max_query_range_len: 10,
        default_chunk_range_len: 3,
    })
    .plan(
        input,
        &capabilities(),
        ChainHeight::block(4).with_finality(FinalityKind::Finalized),
    )
    .expect("native plan");

    assert_eq!(plan.chain, ethereum_identity());
    assert_eq!(plan.dataset_key, DatasetKey::evm_logs());
    assert_eq!(
        plan.ledger_range,
        LedgerRange::blocks(1, 4).expect("valid range")
    );
    assert_eq!(plan.selector.kind(), SelectorKind::EvmLogs);
    assert_eq!(plan.response_shape, ResponseShape::LegacyEvmLogs);
    assert_eq!(
        plan.range_split,
        RangeSplitStrategy::MaxLedgerSpan {
            max_len: 2,
            supports_adapter_split: true,
        }
    );
    assert_eq!(
        plan.capability_requirements,
        vec![AdapterCapabilityRequirement {
            dataset_key: DatasetKey::evm_logs(),
            selector: SelectorKind::EvmLogs,
            range_kind: HeightRangeKind::Block,
        }]
    );
    assert_eq!(
        plan.finality_policy,
        FinalityPolicy::DurableCache {
            boundary: ChainHeight::block(4).with_finality(FinalityKind::Finalized),
        }
    );
    assert_eq!(
        plan.split_ranges(vec![LedgerRange::blocks(1, 4).expect("valid range")])
            .expect("split ranges"),
        vec![
            LedgerRange::blocks(1, 2).expect("valid range"),
            LedgerRange::blocks(3, 4).expect("valid range"),
        ]
    );
}

fn capabilities() -> AdapterCapabilities {
    AdapterCapabilities::new(ethereum_identity())
        .with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_blocks())
                .with_selector(SelectorKind::All)
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(2)
                .with_safe_height(true)
                .with_finalized_height(true)
                .with_range_split(true),
        )
        .with_dataset_capability(
            DatasetCapability::new(DatasetKey::evm_logs())
                .with_selector(SelectorKind::EvmLogs)
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(2)
                .with_max_addresses_per_query(1)
                .with_max_topics_per_query(4)
                .with_safe_height(true)
                .with_finalized_height(true)
                .with_range_split(true),
        )
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}
