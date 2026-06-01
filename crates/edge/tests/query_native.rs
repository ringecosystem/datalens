mod support;

use support::query::*;

#[test]
fn test_query_native_executes_non_evm_plan_without_evm_route_validation() {
    let root = temp_storage_root("native-non-evm");
    let source = MockSource::default()
        .with_chain(tron_identity())
        .with_native_rows(vec![serde_json::json!({"event": "transfer"})]);
    let service = QueryService::new_named(
        LocalStorage::new(&root),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "tron",
        ChainConfig {
            kind: "tron".to_owned(),
            chain_id: 1,
            rpc_urls: vec!["http://example.invalid".to_owned()],
            trongrid: Default::default(),
            finality: datalens_edge::config::FinalityConfig::Auto,
            datasets: DatasetsConfig {
                blocks: datalens_edge::config::BlocksDatasetConfig {
                    enabled: false,
                    max_batch_blocks: 2,
                },
                logs: LogsDatasetConfig {
                    enabled: false,
                    query_strategy: Default::default(),
                    max_get_logs_range_blocks: 2,
                    max_block_scan_range_blocks: 2,
                    max_addresses_per_query: 2,
                },
            },
        },
    );
    let input = NativeQueryInput {
        chain: tron_identity(),
        dataset_key: DatasetKey::tron_events(),
        ledger_range: LedgerRange::blocks(1, 1).expect("valid range"),
        selector: DatasetSelector::try_other(
            AdapterKey::try_new("tron-events").expect("valid selector kind"),
            "tron-events/all",
            "tron-events/all",
        )
        .expect("valid selector"),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    };

    let response = service.query_native(input).expect("native query succeeds");

    assert_eq!(response.chain, tron_identity());
    assert_eq!(response.dataset_key, DatasetKey::tron_events());
    assert_eq!(
        response.cache.missing_ranges,
        vec![LedgerRange::blocks(1, 1).expect("valid range")]
    );
    assert_eq!(response.rows.dataset_key(), &DatasetKey::tron_events());
    assert_eq!(response.rows.row_count(), 1);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Native(
            DatasetKey::tron_events(),
            LedgerRange::blocks(1, 1).expect("valid range")
        )]
    );
}

#[test]
fn test_query_native_reuses_staged_non_evm_rows_before_shutdown_flush() {
    let root = temp_storage_root("native-staged-non-evm");
    let source = MockSource::default()
        .with_chain(tron_identity())
        .with_native_rows(vec![serde_json::json!({"event": "transfer"})]);
    let service = QueryService::new_named(
        LocalStorage::new(&root),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: datalens_edge::config::WriterStagingConfig {
                enabled: true,
                ..Default::default()
            },
        },
        "tron",
        ChainConfig {
            kind: "tron".to_owned(),
            chain_id: 1,
            rpc_urls: vec!["http://example.invalid".to_owned()],
            trongrid: Default::default(),
            finality: datalens_edge::config::FinalityConfig::Auto,
            datasets: DatasetsConfig {
                blocks: datalens_edge::config::BlocksDatasetConfig {
                    enabled: false,
                    max_batch_blocks: 2,
                },
                logs: LogsDatasetConfig {
                    enabled: false,
                    query_strategy: Default::default(),
                    max_get_logs_range_blocks: 2,
                    max_block_scan_range_blocks: 2,
                    max_addresses_per_query: 2,
                },
            },
        },
    );
    let input = NativeQueryInput {
        chain: tron_identity(),
        dataset_key: DatasetKey::tron_events(),
        ledger_range: LedgerRange::blocks(1, 1).expect("valid range"),
        selector: DatasetSelector::try_other(
            AdapterKey::try_new("tron-events").expect("valid selector kind"),
            "tron-events/all",
            "tron-events/all",
        )
        .expect("valid selector"),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    };

    let first = service
        .query_native(input.clone())
        .expect("first query fetches provider rows");
    let second = service
        .query_native(input.clone())
        .expect("second query reads staged rows");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(1, 1).expect("valid range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(1, 1).expect("valid range")]
    );
    assert_eq!(second.rows.row_count(), 1);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Native(
            DatasetKey::tron_events(),
            LedgerRange::blocks(1, 1).expect("valid range")
        )]
    );
    assert!(
        LocalStorage::new(&root)
            .manifest()
            .expect("manifest")
            .entries
            .is_empty()
    );

    service
        .flush_staged_writes_for_shutdown()
        .expect("shutdown flush persists staged rows");
    assert_eq!(
        LocalStorage::new(&root)
            .manifest()
            .expect("manifest")
            .entries
            .len(),
        1
    );
}
