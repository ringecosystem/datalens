use datalens_chain::{ChainAdapter, SelectorKind};
use std::sync::{Arc, Mutex};

use datalens_core::{
    DatalensError, DatalensErrorKind, DatasetKey, LedgerRange, QueryFinalityRequirement, QueryRows,
    QueryStrategy,
};
use datalens_executor::{NativeQueryExecutionConfig, NativeQueryExecutor};
use datalens_metrics::ApplicationIdentity;
use datalens_planner::{FieldSelection, NativePlannerConfig, NativeQueryInput};
use datalens_runtime_indexer::{
    InMemoryIndexCursorStore, IndexDatasetRequest, IndexDatasetSelection, IndexFinalityRequirement,
    IndexJob, IndexJobId, IndexRetryPolicy, IndexRunMode, IndexRunStatus, IndexRuntime,
    IndexRuntimeConfig,
};
use datalens_storage::LocalStorage;
use datalens_tron::{
    TronAdapter, TronBlock, TronContractEvent, TronContractEventPage, TronContractEventRequest,
    TronEventFilter, TronFinality, TronFixtureProviderRpc, TronProvider, tron_all_selector,
    tron_event_selector,
};
use datalens_writer::{DurableWriterConfig, WriteStagingConfig};

#[test]
fn test_tron_full_indexing_writes_all_durable_datasets() {
    let storage = LocalStorage::new(temp_storage_root("full-indexing"));
    let adapter = TronAdapter::with_fixture_defaults();
    let runtime = runtime(adapter.clone(), storage.clone());

    let result = runtime
        .run(full_tron_job(10, 12, IndexRunMode::Backfill))
        .expect("index Tron datasets");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(result.accounting.chunks_planned, 4);
    assert_eq!(result.accounting.rows_written, 6);

    let selector = tron_all_selector().expect("selector");
    for (dataset_key, expected_rows) in [
        (DatasetKey::tron_blocks(), 3),
        (DatasetKey::tron_transactions(), 1),
        (DatasetKey::tron_transaction_infos(), 1),
        (DatasetKey::tron_events(), 1),
    ] {
        assert_eq!(
            storage
                .covered_ranges(
                    adapter.capabilities().chain(),
                    &dataset_key,
                    &selector,
                    LedgerRange::blocks(10, 12).expect("range"),
                )
                .expect("coverage"),
            vec![LedgerRange::blocks(10, 12).expect("range")]
        );
        assert_eq!(
            storage
                .read_rows(
                    adapter.capabilities().chain(),
                    &dataset_key,
                    &selector,
                    LedgerRange::blocks(10, 12).expect("range"),
                )
                .expect("read rows")
                .row_count(),
            expected_rows
        );
    }
}

#[test]
fn test_tron_empty_events_record_durable_coverage() {
    let storage = LocalStorage::new(temp_storage_root("empty-events"));
    let adapter = TronAdapter::with_fixture_defaults();
    let runtime = runtime(adapter.clone(), storage.clone());

    let result = runtime
        .run(selected_tron_job(
            DatasetKey::tron_events(),
            11,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("index empty events");

    assert_eq!(result.accounting.rows_written, 0);
    assert_eq!(
        storage
            .covered_ranges(
                adapter.capabilities().chain(),
                &DatasetKey::tron_events(),
                &tron_all_selector().expect("selector"),
                LedgerRange::blocks(11, 12).expect("range"),
            )
            .expect("coverage"),
        vec![LedgerRange::blocks(11, 12).expect("range")]
    );
}

#[test]
fn test_small_tron_json_rows_survive_query_executor_restart_with_staging_enabled() {
    let storage_root = temp_storage_root("small-json-restart");
    let first_adapter =
        TronAdapter::with_provider(test_tron_chain(), CountingTronProvider::default());
    let first_executor = NativeQueryExecutor::new(
        LocalStorage::new(&storage_root),
        first_adapter.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 10,
            },
            writer: staged_writer_config(),
        },
    );
    let input = NativeQueryInput {
        chain: first_adapter.capabilities().chain().clone(),
        dataset_key: DatasetKey::tron_events(),
        ledger_range: LedgerRange::blocks(10, 12).expect("range"),
        selector: tron_all_selector().expect("selector"),
        field_selection: FieldSelection::All,
        finality: QueryFinalityRequirement::DurableOnly,
    };

    let first = first_executor
        .execute(input.clone())
        .expect("first query fills small JSON rows");
    assert_eq!(first.rows.row_count(), 1);
    first_executor
        .wait_for_durable_promotions()
        .expect("first query durable promotion");

    let second_provider = CountingTronProvider::default();
    let second_executor = NativeQueryExecutor::new(
        LocalStorage::new(&storage_root),
        TronAdapter::with_provider(test_tron_chain(), second_provider.clone()),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 10,
            },
            writer: staged_writer_config(),
        },
    );
    let second = second_executor
        .execute(input)
        .expect("restarted query reads durable JSON rows");

    assert_eq!(
        second.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(10, 12).expect("range")]
    );
    assert_eq!(second.cache.provider_fill_ranges, Vec::<LedgerRange>::new());
    assert_eq!(second.rows.row_count(), 1);
    assert_eq!(second_provider.data_fetch_calls(), 0);
}

#[test]
fn test_tron_event_selector_is_normalized_deterministic_and_storage_safe() {
    let first = tron_event_selector(TronEventFilter {
        contract_addresses: vec![
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
            "41ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD".to_owned(),
        ],
        event_names: vec!["MessageDispatched".to_owned(), "MessageAccepted".to_owned()],
    })
    .expect("selector");
    let second = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["MessageAccepted".to_owned(), "MessageDispatched".to_owned()],
    })
    .expect("selector");

    assert_eq!(first, second);
    assert!(matches!(first.kind(), SelectorKind::Other(kind) if kind.as_str() == "tron_events"));
    assert_eq!(
        first.canonical_key(),
        "contracts/41abcdefabcdefabcdefabcdefabcdefabcdefabcd/events/MessageAccepted+MessageDispatched"
    );
    assert!(first.fingerprint().starts_with("tron-events/"));
    assert!(
        first
            .fingerprint()
            .chars()
            .all(|character| character.is_ascii_alphanumeric()
                || character == '/'
                || character == '-')
    );
}

#[test]
fn test_tron_event_selector_filters_fallback_rows_by_contract_and_event_name() {
    let adapter = TronAdapter::with_fixture_defaults();
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 12).expect("range"),
            selector.clone(),
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["contract_address"],
        "41abcdefabcdefabcdefabcdefabcdefabcdefabcd"
    );
    assert_eq!(rows[0]["event_name"], "Transfer");
    assert_eq!(rows[0]["parent_hash"], "0000000000000009-tron-hash");
    assert_eq!(rows[0]["block_timestamp"], 1_700_000_010_u64);
    assert_eq!(rows[0]["confirmed"], true);
    assert_eq!(rows[0]["source"]["provider"], "tron_block_scan");
}

#[test]
fn test_tron_event_selector_accepts_ormp_topics_for_fallback() {
    let adapter = TronAdapter::with_fixture_defaults();
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec![
            "HashImported".to_owned(),
            "MessageAccepted".to_owned(),
            "MessageAssigned".to_owned(),
            "MessageDispatched".to_owned(),
            "MessageRecv".to_owned(),
            "MessageSent".to_owned(),
            "SignatureSubmittion".to_owned(),
        ],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 12).expect("range"),
            selector.clone(),
        ))
        .expect("fetch events");

    assert_eq!(response.rows.row_count(), 0);
    assert!(response.provider_diagnostics.calls > 0);

    let cached = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 12).expect("range"),
            selector,
        ))
        .expect("fetch cached empty events");

    assert_eq!(cached.rows.row_count(), 0);
    assert!(cached.provider_diagnostics.calls > 0);
}

#[test]
fn test_tron_event_selector_maps_ormp_topics_for_block_scan() {
    for (event_name, topic0) in [
        (
            "HashImported",
            "ea087580bb17f433441f3b6c0c0b80cae92ee74a8d7f50050388646d9ffd1431",
        ),
        (
            "MessageSent",
            "40195d26d027672e04e23e34282d68c3d43ea138415b24c54fcdb9c2573e5975",
        ),
        (
            "MessageRecv",
            "a931ec14fe958397dcb26e285e56292c13d77907712b51bbaa24cfc9349b789d",
        ),
        (
            "MessageAccepted",
            "cfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18",
        ),
        (
            "MessageAssigned",
            "3832f95736b288316c84b775a004a9d17177362548ce253cba9acb4801875f4d",
        ),
        (
            "MessageDispatched",
            "62b1dc20fd6f1518626da5b6f9897e8cd4ebadbad071bb66dc96a37c970087a8",
        ),
        (
            "SignatureSubmittion",
            "8b3975e4768e70d323e926e2cef0676fc9a3250437d9b8f90b52c770f0d7545f",
        ),
    ] {
        let adapter = TronAdapter::with_provider(
            TronAdapter::with_fixture_defaults()
                .capabilities()
                .chain()
                .clone(),
            KnownEventBlockScanProvider::new(topic0, false),
        )
        .with_events_query_strategy(QueryStrategy::BlockRange);
        let selector = tron_event_selector(TronEventFilter {
            contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
            event_names: vec![event_name.to_owned()],
        })
        .expect("selector");

        let response = adapter
            .fetch(datalens_chain::ChainFetchRequest::new(
                adapter.capabilities().chain().clone(),
                DatasetKey::tron_events(),
                LedgerRange::blocks(10, 10).expect("range"),
                selector,
            ))
            .expect("fetch events");

        let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
            panic!("expected adapter JSON rows");
        };
        assert_eq!(rows.len(), 1, "{event_name}");
        assert_eq!(rows[0]["event_name"], event_name);
        assert_eq!(rows[0]["event_signature"], topic0);
    }
}

#[test]
fn test_tron_event_selector_without_known_topic_mapping_does_not_write_empty_fallback_coverage() {
    let storage = LocalStorage::new(temp_storage_root("unknown-topic-filtered-events"));
    let provider = ContractEventFixtureProvider::with_error(DatalensErrorKind::UnsupportedDataset);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider,
    );
    let runtime = runtime(adapter.clone(), storage.clone());
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["DefinitelyUnknownEvent".to_owned()],
    })
    .expect("selector");

    let error = runtime
        .run(tron_job(
            vec![IndexDatasetRequest {
                dataset_key: DatasetKey::tron_events(),
                selector: selector.clone(),
            }],
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect_err("unknown event-name fallback is unsupported");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert_eq!(
        storage
            .covered_ranges(
                adapter.capabilities().chain(),
                &DatasetKey::tron_events(),
                &selector,
                LedgerRange::blocks(10, 12).expect("range"),
            )
            .expect("coverage"),
        Vec::<LedgerRange>::new()
    );
}

#[test]
fn test_tron_contract_event_provider_success_uses_trongrid_rows() {
    let provider =
        ContractEventFixtureProvider::with_contract_events(vec![contract_event("tron-grid-tx")]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["transaction_id"], "tron-grid-tx");
    assert_eq!(rows[0]["source"]["provider"], "trongrid_contract_events");
    assert_eq!(provider.contract_event_calls(), 1);
    let requests = provider.contract_event_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].event_name.as_deref(), Some("Transfer"));
}

#[test]
fn test_tron_contract_event_provider_empty_success_merges_known_ormp_block_scan_rows() {
    let provider = KnownEventBlockScanProvider::new(
        "cfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18",
        true,
    );
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["MessageAccepted".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["transaction_id"], "tron-tx-10");
    assert_eq!(rows[0]["event_name"], "MessageAccepted");
    assert_eq!(
        rows[0]["event_signature"],
        "cfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18"
    );
    assert_eq!(rows[0]["source"]["provider"], "tron_block_scan");
    assert_eq!(provider.contract_event_calls(), 1);
}

#[test]
fn test_tron_block_range_strategy_skips_trongrid_contract_events() {
    let provider =
        ContractEventFixtureProvider::with_contract_events(vec![contract_event("tron-grid-tx")]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    )
    .with_events_query_strategy(QueryStrategy::BlockRange);
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert!(!rows.is_empty());
    assert_eq!(provider.contract_event_calls(), 0);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"tron block_range event query strategy used".to_owned())
    );
}

#[test]
fn test_tron_contract_event_provider_matches_base58_response_address() {
    let provider = ContractEventFixtureProvider::with_contract_events(vec![TronContractEvent {
        contract_address: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_owned(),
        event_name: Some("Transfer".to_owned()),
        event_signature: Some(
            "Transfer(address indexed from, address indexed to, uint256 value)".to_owned(),
        ),
        indexed_fields: Vec::new(),
        non_indexed_fields: serde_json::json!({
            "from": "0x9f3f3ab197bf6b8c05069520b28b48cc2852eba6",
            "to": "0xfe0460cf2611ce2f29a7b8cded20e3dee3c7bae4",
            "value": "256860200"
        }),
        transaction_id: Some("tron-grid-tx".to_owned()),
        block_number: 10,
        block_hash: None,
        transaction_index: 0,
        event_index: 0,
        confirmed: true,
        raw: serde_json::json!({
            "contract_address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            "event_name": "Transfer"
        }),
    }]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider,
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["41a614f803b6fd780986a42c78ec9c7f77e6ded13c".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["contract_address"],
        "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
    );
    assert_eq!(rows[0]["parent_hash"], "0000000000000009-tron-hash");
    assert_eq!(rows[0]["block_timestamp"], 1_700_000_010_u64);
}

#[test]
fn test_tron_contract_event_provider_splits_multi_block_ranges() {
    let provider = ContractEventFixtureProvider::with_contract_event_pages(vec![
        TronContractEventPage {
            events: Vec::new(),
            next_fingerprint: None,
            provider_calls: 1,
        },
        TronContractEventPage {
            events: Vec::new(),
            next_fingerprint: None,
            provider_calls: 1,
        },
        TronContractEventPage {
            events: Vec::new(),
            next_fingerprint: None,
            provider_calls: 1,
        },
    ]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 12).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let requests = provider.contract_event_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].range,
        LedgerRange::blocks(10, 12).expect("range")
    );
    assert_eq!(requests[0].start_timestamp, Some(1_700_000_010));
    assert_eq!(requests[0].end_timestamp, Some(1_700_000_012));
}

#[test]
fn test_tron_contract_event_provider_multi_event_filter_queries_all_events_per_contract_block() {
    let first_contract = "41abcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let second_contract = "41bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let provider = ContractEventFixtureProvider::with_contract_event_pages(vec![
        TronContractEventPage {
            events: vec![
                contract_event_for("accepted-10", first_contract, "MessageAccepted", 10),
                contract_event_for("transfer-10", first_contract, "Transfer", 10),
                contract_event_for("dispatched-11", first_contract, "MessageDispatched", 11),
            ],
            next_fingerprint: None,
            provider_calls: 1,
        },
        TronContractEventPage {
            events: vec![
                contract_event_for("transfer-other-10", second_contract, "Transfer", 10),
                contract_event_for("accepted-other-11", second_contract, "MessageAccepted", 11),
            ],
            next_fingerprint: None,
            provider_calls: 1,
        },
    ]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec![first_contract.to_owned(), second_contract.to_owned()],
        event_names: vec!["MessageAccepted".to_owned(), "MessageDispatched".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 11).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["transaction_id"], "accepted-10");
    assert_eq!(rows[0]["event_name"], "MessageAccepted");
    assert_eq!(rows[1]["transaction_id"], "dispatched-11");
    assert_eq!(rows[1]["event_name"], "MessageDispatched");
    assert_eq!(rows[2]["transaction_id"], "accepted-other-11");
    assert_eq!(rows[2]["event_name"], "MessageAccepted");
    assert_eq!(provider.contract_event_calls(), 2);
    assert_eq!(response.provider_diagnostics.calls, 4);

    let requests = provider.contract_event_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.event_name.is_none()));
    assert_eq!(requests[0].contract_address, first_contract);
    assert_eq!(
        requests[0].range,
        LedgerRange::blocks(10, 11).expect("range")
    );
    assert_eq!(requests[0].start_timestamp, Some(1_700_000_010));
    assert_eq!(requests[0].end_timestamp, Some(1_700_000_011));
    assert_eq!(
        requests[1].range,
        LedgerRange::blocks(10, 11).expect("range")
    );
    assert_eq!(requests[1].contract_address, second_contract);
    assert_eq!(requests[1].start_timestamp, Some(1_700_000_010));
    assert_eq!(requests[1].end_timestamp, Some(1_700_000_011));
}

#[test]
fn test_tron_contract_event_provider_multi_event_filter_scales_page_cap() {
    let provider = ContractEventFixtureProvider::with_contract_event_pages(vec![
        TronContractEventPage {
            events: vec![contract_event_for(
                "irrelevant-1",
                "41abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "Transfer",
                10,
            )],
            next_fingerprint: Some("page-2".to_owned()),
            provider_calls: 1,
        },
        TronContractEventPage {
            events: vec![contract_event_for(
                "accepted-10",
                "41abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "MessageAccepted",
                10,
            )],
            next_fingerprint: None,
            provider_calls: 1,
        },
    ]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    )
    .with_max_contract_event_pages(1);
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["MessageAccepted".to_owned(), "MessageDispatched".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["transaction_id"], "accepted-10");
    assert_eq!(provider.contract_event_calls(), 2);
    let requests = provider.contract_event_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.event_name.is_none()));
}

#[test]
fn test_tron_contract_event_provider_multi_page_success_uses_all_pages() {
    let provider = ContractEventFixtureProvider::with_contract_event_pages(vec![
        TronContractEventPage {
            events: vec![contract_event("tron-grid-tx-1")],
            next_fingerprint: Some("page-2".to_owned()),
            provider_calls: 1,
        },
        TronContractEventPage {
            events: vec![contract_event("tron-grid-tx-2")],
            next_fingerprint: None,
            provider_calls: 1,
        },
    ]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fetch events");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["transaction_id"], "tron-grid-tx-1");
    assert_eq!(rows[1]["transaction_id"], "tron-grid-tx-2");
    for row in rows {
        assert_eq!(row["block_hash"], "000000000000000a-tron-hash");
        assert_eq!(row["parent_hash"], "0000000000000009-tron-hash");
        assert_eq!(row["block_timestamp"], 1_700_000_010_u64);
    }
    assert_eq!(provider.contract_event_calls(), 2);
    assert_eq!(response.provider_diagnostics.calls, 3);
}

#[test]
fn test_tron_contract_event_provider_repeated_fingerprint_fails_without_fallback() {
    let provider =
        ContractEventFixtureProvider::with_contract_event_pages(vec![TronContractEventPage {
            events: vec![contract_event("tron-grid-tx")],
            next_fingerprint: Some("same-page".to_owned()),
            provider_calls: 1,
        }])
        .with_loop_guard(4);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let error = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect_err("repeated fingerprint should fail");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(
        error
            .message
            .contains("repeated TronGrid contract event fingerprint")
    );
    assert_eq!(provider.contract_event_calls(), 2);
}

#[test]
fn test_tron_contract_event_provider_page_cap_fails_without_durable_coverage() {
    let storage = LocalStorage::new(temp_storage_root("contract-event-page-cap"));
    let provider = ContractEventFixtureProvider::with_contract_event_pages(vec![
        TronContractEventPage {
            events: vec![contract_event("tron-grid-tx-1")],
            next_fingerprint: Some("page-2".to_owned()),
            provider_calls: 1,
        },
        TronContractEventPage {
            events: vec![contract_event("tron-grid-tx-2")],
            next_fingerprint: Some("page-3".to_owned()),
            provider_calls: 1,
        },
    ]);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    )
    .with_max_contract_event_pages(1);
    let runtime = runtime(adapter.clone(), storage.clone());
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let error = runtime
        .run(tron_job(
            vec![IndexDatasetRequest {
                dataset_key: DatasetKey::tron_events(),
                selector: selector.clone(),
            }],
            10,
            10,
            IndexRunMode::Backfill,
        ))
        .expect_err("page cap should fail indexing");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(error.message.contains("TronGrid contract event page limit"));
    assert_eq!(
        storage
            .covered_ranges(
                adapter.capabilities().chain(),
                &DatasetKey::tron_events(),
                &selector,
                LedgerRange::blocks(10, 10).expect("range"),
            )
            .expect("coverage"),
        Vec::<LedgerRange>::new()
    );
}

#[test]
fn test_tron_contract_event_provider_rate_limit_does_not_fall_back_to_block_scan() {
    let provider = ContractEventFixtureProvider::with_error(DatalensErrorKind::RateLimited);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let error = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect_err("rate limit should surface");

    assert_eq!(error.kind, DatalensErrorKind::RateLimited);
    assert_eq!(provider.contract_event_calls(), 1);
}

#[test]
fn test_tron_contract_event_provider_timeout_does_not_fall_back_to_block_scan() {
    let provider = ContractEventFixtureProvider::with_error(DatalensErrorKind::ProviderTimeout);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let error = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect_err("provider timeout should surface");

    assert_eq!(error.kind, DatalensErrorKind::ProviderTimeout);
    assert_eq!(provider.contract_event_calls(), 1);
}

#[test]
fn test_tron_contract_event_provider_auth_errors_do_not_fall_back_to_block_scan() {
    for kind in [
        DatalensErrorKind::AuthenticationFailed,
        DatalensErrorKind::Unauthorized,
    ] {
        let provider = ContractEventFixtureProvider::with_error(kind.clone());
        let adapter = TronAdapter::with_provider(
            TronAdapter::with_fixture_defaults()
                .capabilities()
                .chain()
                .clone(),
            provider.clone(),
        );
        let selector = tron_event_selector(TronEventFilter {
            contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
            event_names: vec!["Transfer".to_owned()],
        })
        .expect("selector");

        let error = adapter
            .fetch(datalens_chain::ChainFetchRequest::new(
                adapter.capabilities().chain().clone(),
                DatasetKey::tron_events(),
                LedgerRange::blocks(10, 10).expect("range"),
                selector,
            ))
            .expect_err("auth failure should surface");

        assert_eq!(error.kind, kind);
        assert_eq!(provider.contract_event_calls(), 1);
    }
}

#[test]
fn test_tron_contract_event_provider_safe_provider_failure_falls_back_to_block_scan() {
    let provider = ContractEventFixtureProvider::with_error_message(
        DatalensErrorKind::ProviderFailure,
        "TronGrid contract events HTTP error 500: upstream failed",
    );
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider.clone(),
    );
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let response = adapter
        .fetch(datalens_chain::ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("range"),
            selector,
        ))
        .expect("fallback fetch");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["transaction_id"], "tron-tx-10");
    assert_eq!(rows[0]["source"]["provider"], "tron_block_scan");
    assert_eq!(provider.contract_event_calls(), 1);
}

#[test]
fn test_tron_contract_event_provider_parse_error_does_not_fall_back_to_block_scan() {
    let storage = LocalStorage::new(temp_storage_root("malformed-contract-events"));
    let provider = ContractEventFixtureProvider::with_error(DatalensErrorKind::InvalidRequest);
    let adapter = TronAdapter::with_provider(
        TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        provider,
    );
    let runtime = runtime(adapter.clone(), storage.clone());
    let selector = tron_event_selector(TronEventFilter {
        contract_addresses: vec!["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned()],
        event_names: vec!["Transfer".to_owned()],
    })
    .expect("selector");

    let error = runtime
        .run(tron_job(
            vec![IndexDatasetRequest {
                dataset_key: DatasetKey::tron_events(),
                selector: selector.clone(),
            }],
            10,
            10,
            IndexRunMode::Backfill,
        ))
        .expect_err("malformed contract event response is not fallback-safe");

    assert_eq!(error.kind, DatalensErrorKind::InvalidRequest);
    assert_eq!(
        storage
            .covered_ranges(
                adapter.capabilities().chain(),
                &DatasetKey::tron_events(),
                &selector,
                LedgerRange::blocks(10, 10).expect("range"),
            )
            .expect("coverage"),
        Vec::<LedgerRange>::new()
    );
}

#[test]
fn test_tron_resume_after_partial_indexing_is_idempotent() {
    let storage = LocalStorage::new(temp_storage_root("resume"));
    let cursor_store = InMemoryIndexCursorStore::default();
    let adapter = TronAdapter::with_fixture_defaults();
    let first = IndexRuntime::new(
        adapter.clone(),
        storage.clone(),
        cursor_store.clone(),
        writer_config(),
    );

    first
        .run(selected_tron_job(
            DatasetKey::tron_blocks(),
            10,
            10,
            IndexRunMode::Backfill,
        ))
        .expect("seed first chunk");

    let resumed = IndexRuntime::new(
        adapter.clone(),
        storage.clone(),
        cursor_store,
        writer_config(),
    );
    let result = resumed
        .run(selected_tron_job(
            DatasetKey::tron_blocks(),
            10,
            12,
            IndexRunMode::Resume,
        ))
        .expect("resume");

    assert_eq!(result.status, IndexRunStatus::Completed);
    assert_eq!(
        storage
            .read_rows(
                adapter.capabilities().chain(),
                &DatasetKey::tron_blocks(),
                &tron_all_selector().expect("selector"),
                LedgerRange::blocks(10, 12).expect("range"),
            )
            .expect("read rows")
            .row_count(),
        3
    );

    let rerun = resumed
        .run(selected_tron_job(
            DatasetKey::tron_blocks(),
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("rerun");
    assert_eq!(rerun.accounting.chunks_planned, 0);
}

#[test]
fn test_tron_indexing_chunks_according_to_provider_limit() {
    let storage = LocalStorage::new(temp_storage_root("provider-limit"));
    let adapter = TronAdapter::with_provider_limits(TronFixtureProviderRpc, 1);
    let runtime = runtime(adapter, storage);

    let result = runtime
        .run(selected_tron_job(
            DatasetKey::tron_transactions(),
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("provider limit split succeeds");

    assert_eq!(result.accounting.chunks_planned, 3);
    assert_eq!(result.accounting.provider_limit_splits, 0);
}

#[test]
fn test_tron_events_capability_uses_event_range_without_changing_blocks() {
    let adapter = TronAdapter::with_provider(test_tron_chain(), TronFixtureProviderRpc)
        .with_max_block_range_len(1000)
        .with_max_event_range_len(5000);
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities
            .dataset(&DatasetKey::tron_blocks())
            .expect("blocks capability")
            .max_range_len(),
        Some(1000)
    );
    assert_eq!(
        capabilities
            .dataset(&DatasetKey::tron_events())
            .expect("events capability")
            .max_range_len(),
        Some(5000)
    );
}

#[test]
fn test_tron_durable_query_reads_indexed_transactions() {
    let root = temp_storage_root("query-indexed");
    let storage = LocalStorage::new(&root);
    let adapter = TronAdapter::with_fixture_defaults();
    runtime(adapter.clone(), storage.clone())
        .run(selected_tron_job(
            DatasetKey::tron_transactions(),
            10,
            12,
            IndexRunMode::Backfill,
        ))
        .expect("index transactions");

    let executor = NativeQueryExecutor::new(
        storage,
        adapter.clone(),
        NativeQueryExecutionConfig {
            planner: NativePlannerConfig {
                max_query_range_len: 10,
                default_chunk_range_len: 10,
            },
            writer: writer_config(),
        },
    );
    let result = executor
        .execute(NativeQueryInput {
            chain: adapter.capabilities().chain().clone(),
            dataset_key: DatasetKey::tron_transactions(),
            ledger_range: LedgerRange::blocks(10, 12).expect("range"),
            selector: tron_all_selector().expect("selector"),
            field_selection: FieldSelection::All,
            finality: QueryFinalityRequirement::DurableOnly,
        })
        .expect("query indexed transactions");

    let QueryRows::AdapterJson { rows, .. } = result.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["transaction_id"], "tron-tx-10");
    assert_eq!(rows[0]["block_number"], 10);
}

fn runtime<P>(
    adapter: TronAdapter<P>,
    storage: LocalStorage,
) -> IndexRuntime<TronAdapter<P>, LocalStorage, InMemoryIndexCursorStore>
where
    P: TronProvider,
{
    IndexRuntime::new(
        adapter,
        storage,
        InMemoryIndexCursorStore::default(),
        writer_config(),
    )
}

fn full_tron_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    let selector = tron_all_selector().expect("selector");
    tron_job(
        vec![
            DatasetKey::tron_blocks(),
            DatasetKey::tron_transactions(),
            DatasetKey::tron_transaction_infos(),
            DatasetKey::tron_events(),
        ]
        .into_iter()
        .map(|dataset_key| IndexDatasetRequest {
            dataset_key,
            selector: selector.clone(),
        })
        .collect(),
        start,
        end,
        run_mode,
    )
}

fn selected_tron_job(
    dataset_key: DatasetKey,
    start: u64,
    end: u64,
    run_mode: IndexRunMode,
) -> IndexJob {
    tron_job(
        vec![IndexDatasetRequest {
            dataset_key,
            selector: tron_all_selector().expect("selector"),
        }],
        start,
        end,
        run_mode,
    )
}

fn tron_job(
    datasets: Vec<IndexDatasetRequest>,
    start: u64,
    end: u64,
    run_mode: IndexRunMode,
) -> IndexJob {
    IndexJob {
        id: IndexJobId::new("tron-indexing-fixture").expect("job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: TronAdapter::with_fixture_defaults()
            .capabilities()
            .chain()
            .clone(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: IndexDatasetSelection::Selected(datasets),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 3 },
        run_mode,
        retry_policy: IndexRetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn writer_config() -> DurableWriterConfig {
    DurableWriterConfig {
        target_object_bytes: 4096,
        min_object_rows: 1,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

fn staged_writer_config() -> DurableWriterConfig {
    DurableWriterConfig {
        target_object_bytes: 1024 * 1024,
        min_object_rows: 1000,
        record_empty_coverage: true,
        staging: WriteStagingConfig {
            enabled: true,
            min_rows: Some(1000),
            ..Default::default()
        },
    }
}

fn test_tron_chain() -> datalens_core::ChainIdentity {
    TronAdapter::with_fixture_defaults()
        .capabilities()
        .chain()
        .clone()
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-tron-indexing-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}

#[derive(Clone, Default)]
struct CountingTronProvider {
    inner: TronFixtureProviderRpc,
    data_fetch_calls: Arc<Mutex<usize>>,
}

impl CountingTronProvider {
    fn data_fetch_calls(&self) -> usize {
        *self.data_fetch_calls.lock().expect("data fetch calls")
    }
}

impl TronProvider for CountingTronProvider {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError> {
        self.inner.latest_block(finality)
    }

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError> {
        *self.data_fetch_calls.lock().expect("data fetch calls") += 1;
        self.inner.get_block_by_number(number, finality)
    }

    fn get_transaction_info_by_id(
        &self,
        tx_id: &str,
    ) -> Result<Option<serde_json::Value>, DatalensError> {
        *self.data_fetch_calls.lock().expect("data fetch calls") += 1;
        self.inner.get_transaction_info_by_id(tx_id)
    }

    fn supports_contract_event_query(&self) -> bool {
        self.inner.supports_contract_event_query()
    }

    fn get_contract_events(
        &self,
        request: TronContractEventRequest,
    ) -> Result<TronContractEventPage, DatalensError> {
        *self.data_fetch_calls.lock().expect("data fetch calls") += 1;
        self.inner.get_contract_events(request)
    }

    fn provider_name(&self) -> &'static str {
        "counting-tron-fixture"
    }
}

#[derive(Clone, Debug)]
struct ContractEventFixtureProvider {
    pages: Vec<TronContractEventPage>,
    error: Option<DatalensError>,
    loop_guard: Option<usize>,
    contract_event_calls: Arc<Mutex<usize>>,
    contract_event_requests: Arc<Mutex<Vec<TronContractEventRequest>>>,
}

impl ContractEventFixtureProvider {
    fn with_contract_events(events: Vec<TronContractEvent>) -> Self {
        Self::with_contract_event_pages(vec![TronContractEventPage {
            events,
            next_fingerprint: None,
            provider_calls: 1,
        }])
    }

    fn with_contract_event_pages(pages: Vec<TronContractEventPage>) -> Self {
        Self {
            pages,
            error: None,
            loop_guard: None,
            contract_event_calls: Arc::new(Mutex::new(0)),
            contract_event_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_error(error: DatalensErrorKind) -> Self {
        Self::with_error_message(error, "contract event failure")
    }

    fn with_error_message(error: DatalensErrorKind, message: impl Into<String>) -> Self {
        Self {
            pages: Vec::new(),
            error: Some(DatalensError::new(error, message.into())),
            loop_guard: None,
            contract_event_calls: Arc::new(Mutex::new(0)),
            contract_event_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_loop_guard(mut self, loop_guard: usize) -> Self {
        self.loop_guard = Some(loop_guard);
        self
    }

    fn contract_event_calls(&self) -> usize {
        *self
            .contract_event_calls
            .lock()
            .expect("contract event calls")
    }

    fn contract_event_requests(&self) -> Vec<TronContractEventRequest> {
        self.contract_event_requests
            .lock()
            .expect("contract event requests")
            .clone()
    }
}

impl TronProvider for ContractEventFixtureProvider {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError> {
        TronFixtureProviderRpc.latest_block(finality)
    }

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError> {
        TronFixtureProviderRpc.get_block_by_number(number, finality)
    }

    fn get_transaction_info_by_id(
        &self,
        tx_id: &str,
    ) -> Result<Option<serde_json::Value>, DatalensError> {
        TronFixtureProviderRpc.get_transaction_info_by_id(tx_id)
    }

    fn supports_contract_event_query(&self) -> bool {
        true
    }

    fn get_contract_events(
        &self,
        request: TronContractEventRequest,
    ) -> Result<TronContractEventPage, DatalensError> {
        let mut contract_event_calls = self
            .contract_event_calls
            .lock()
            .expect("contract event calls");
        *contract_event_calls += 1;
        let call_index = *contract_event_calls - 1;
        self.contract_event_requests
            .lock()
            .expect("contract event requests")
            .push(request);
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self
            .loop_guard
            .is_some_and(|loop_guard| *contract_event_calls > loop_guard)
        {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "contract event fixture loop guard exceeded",
            ));
        }
        Ok(self
            .pages
            .get(call_index)
            .or_else(|| self.pages.last())
            .cloned()
            .unwrap_or_else(|| TronContractEventPage {
                events: Vec::new(),
                next_fingerprint: None,
                provider_calls: 1,
            }))
    }

    fn provider_name(&self) -> &'static str {
        "contract-event-fixture"
    }
}

#[derive(Clone)]
struct KnownEventBlockScanProvider {
    topic0: &'static str,
    supports_contract_event_query: bool,
    contract_event_calls: Arc<Mutex<usize>>,
}

impl KnownEventBlockScanProvider {
    fn new(topic0: &'static str, supports_contract_event_query: bool) -> Self {
        Self {
            topic0,
            supports_contract_event_query,
            contract_event_calls: Arc::new(Mutex::new(0)),
        }
    }

    fn contract_event_calls(&self) -> usize {
        *self
            .contract_event_calls
            .lock()
            .expect("contract event calls")
    }
}

impl TronProvider for KnownEventBlockScanProvider {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError> {
        TronFixtureProviderRpc.latest_block(finality)
    }

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError> {
        TronFixtureProviderRpc.get_block_by_number(number, finality)
    }

    fn get_transaction_info_by_id(
        &self,
        tx_id: &str,
    ) -> Result<Option<serde_json::Value>, DatalensError> {
        if tx_id != "tron-tx-10" {
            return Ok(None);
        }
        Ok(Some(serde_json::json!({
            "id": "tron-tx-10",
            "blockNumber": 10,
            "blockTimeStamp": 1_700_000_010_u64,
            "receipt": {
                "result": "SUCCESS",
            },
            "log": [{
                "address": "41abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "topics": [
                    self.topic0
                ],
                "data": "0000000000000000000000000000000000000000000000000000000000000001"
            }],
        })))
    }

    fn supports_contract_event_query(&self) -> bool {
        self.supports_contract_event_query
    }

    fn get_contract_events(
        &self,
        _request: TronContractEventRequest,
    ) -> Result<TronContractEventPage, DatalensError> {
        *self
            .contract_event_calls
            .lock()
            .expect("contract event calls") += 1;
        Ok(TronContractEventPage {
            events: Vec::new(),
            next_fingerprint: None,
            provider_calls: 1,
        })
    }

    fn provider_name(&self) -> &'static str {
        "known-event-block-scan-fixture"
    }
}

fn contract_event(transaction_id: &str) -> TronContractEvent {
    contract_event_for(
        transaction_id,
        "41abcdefabcdefabcdefabcdefabcdefabcdefabcd",
        "Transfer",
        10,
    )
}

fn contract_event_for(
    transaction_id: &str,
    contract_address: &str,
    event_name: &str,
    block_number: u64,
) -> TronContractEvent {
    TronContractEvent {
        contract_address: contract_address.to_owned(),
        event_name: Some(event_name.to_owned()),
        event_signature: Some(format!("{event_name}(address,address,uint256)")),
        indexed_fields: Vec::new(),
        non_indexed_fields: serde_json::json!({"value":"1"}),
        transaction_id: Some(transaction_id.to_owned()),
        block_number,
        block_hash: Some(format!("{block_number:016x}-tron-hash")),
        transaction_index: 4,
        event_index: 5,
        confirmed: true,
        raw: serde_json::json!({"event_name": event_name}),
    }
}
