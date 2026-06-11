mod support;

use support::query::*;

#[test]
fn test_query_blocks_miss_persists_then_equivalent_hit_uses_cache() {
    let storage = LocalStorage::new(temp_storage_root("blocks-miss-hit"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let service = service(storage, source.clone());
    let request = blocks_request(10, 11);

    let first = service
        .query_native(request.clone())
        .expect("first query succeeds");
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let second = service
        .query_native(request)
        .expect("second query succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(10, 11).expect("range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(10, 11).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 11))]
    );
    assert_eq!(block_numbers(&second), vec![10, 11]);
}

#[test]
fn test_query_blocks_partial_hit_fetches_only_missing_range() {
    let storage = LocalStorage::new(temp_storage_root("blocks-partial"));
    let source = MockSource::default().with_blocks(vec![
        block(1, "0x01"),
        block(2, "0x02"),
        block(3, "0x03"),
        block(4, "0x04"),
    ]);
    let service = service(storage, source.clone());

    service
        .query_native(blocks_request(1, 2))
        .expect("seed cache");
    service
        .wait_for_durable_promotions()
        .expect("seed promotion drain");
    source.clear_calls();
    let response = service
        .query_native(blocks_request(1, 4))
        .expect("partial query");

    assert_eq!(
        response.cache.hit_ranges,
        vec![LedgerRange::blocks(1, 2).expect("range")]
    );
    assert_eq!(
        response.cache.missing_ranges,
        vec![LedgerRange::blocks(3, 4).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(3, 4))]
    );
    assert_eq!(block_numbers(&response), vec![1, 2, 3, 4]);
}

#[test]
fn test_query_empty_logs_records_empty_coverage_without_data_object() {
    let storage = LocalStorage::new(temp_storage_root("logs-empty"));
    let root = storage.root().to_path_buf();
    let source = MockSource::default();
    let service = service(storage, source.clone());
    let request = logs_request(50, 52, vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]);

    let first = service
        .query_native(request.clone())
        .expect("empty log query succeeds");
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let second = service
        .query_native(request)
        .expect("empty log query hits cache");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(50, 52).expect("range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(50, 52).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Logs(BlockRange::expect_new(50, 51)),
            SourceCall::Logs(BlockRange::expect_new(52, 52)),
        ]
    );
    assert_eq!(log_indexes(&second), Vec::<u64>::new());
    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_logs_miss_persists_then_equivalent_hit_uses_cache() {
    let storage = LocalStorage::new(temp_storage_root("logs-miss-hit"));
    let source = MockSource::default().with_logs(vec![
        log(
            20,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        ),
        log(
            20,
            1,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            vec![TOPIC_A],
        ),
        log(
            21,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_B],
        ),
    ]);
    let service = service(storage, source.clone());
    let request = logs_request_with_topics(
        20,
        21,
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        vec![Some(vec![TOPIC_A])],
    );

    let first = service
        .query_native(request.clone())
        .expect("first log query succeeds");
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");
    let second = service
        .query_native(request)
        .expect("second log query succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(20, 21).expect("range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(20, 21).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(20, 21))]
    );
    assert_eq!(
        log_addresses(&second),
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    );
    assert_eq!(log_indexes(&second), vec![0]);
}

#[test]
fn test_query_staged_non_empty_range_survives_service_restart() {
    let root = temp_storage_root("staged-restart-non-empty");
    let source = MockSource::default().with_blocks(vec![block(10, "0x10")]);
    let service = service_with_writer_config(
        LocalStorage::new(&root),
        source.clone(),
        staging_writer_config(),
    );

    let first = service
        .query_native(blocks_request(10, 10))
        .expect("first query succeeds");
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");
    assert_eq!(block_numbers(&first), vec![10]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
    source.clear_calls();

    let restarted = service_with_writer_config(
        LocalStorage::new(&root),
        source.clone(),
        staging_writer_config(),
    );
    let second = restarted
        .query_native(blocks_request(10, 10))
        .expect("restarted query reads durable cache");

    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(10, 10).expect("valid range")]
    );
    assert_eq!(second.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert_eq!(block_numbers(&second), vec![10]);
}

#[test]
fn test_query_empty_coverage_survives_service_restart() {
    let root = temp_storage_root("staged-restart-empty");
    let source = MockSource::default();
    let request = logs_request(50, 50, vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]);
    let service = service_with_writer_config(
        LocalStorage::new(&root),
        source.clone(),
        staging_writer_config(),
    );

    service
        .query_native(request.clone())
        .expect("first empty query succeeds");
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");
    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(50, 50))]
    );
    source.clear_calls();

    let restarted = service_with_writer_config(
        LocalStorage::new(&root),
        source.clone(),
        staging_writer_config(),
    );
    let second = restarted
        .query_native(request)
        .expect("restarted empty query reads durable coverage");

    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(50, 50).expect("valid range")]
    );
    assert_eq!(second.cache.missing_ranges, Vec::<LedgerRange>::new());
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert_eq!(log_indexes(&second), Vec::<u64>::new());
}

#[test]
fn test_query_range_limit_rejection_returns_invalid_input() {
    let service = service(
        LocalStorage::new(temp_storage_root("range-limit")),
        MockSource::default(),
    );
    let error = service
        .query_native(blocks_request(1, 5))
        .expect_err("range is too large");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_query_rejects_range_above_safe_height_without_fetch_or_cache_write() {
    let root = temp_storage_root("unsafe-range");
    let source = MockSource::default()
        .with_blocks(vec![block(99, "0x63"), block(100, "0x64")])
        .with_safe_height(99, FinalityKind::Safe);
    let service = service(LocalStorage::new(&root), source.clone());

    let error = service
        .query_native(blocks_request(99, 100))
        .expect_err("unsafe range is rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("safe/finalized height"));
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_allows_range_at_safe_height_and_writes_cache() {
    let root = temp_storage_root("safe-range");
    let source = MockSource::default()
        .with_blocks(vec![block(98, "0x62"), block(99, "0x63")])
        .with_safe_height(99, FinalityKind::Safe);
    let service = service(LocalStorage::new(&root), source.clone());

    let response = service
        .query_native(blocks_request(98, 99))
        .expect("safe range succeeds");
    service
        .wait_for_durable_promotions()
        .expect("promotion drain");

    assert_eq!(block_numbers(&response), vec![98, 99]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(98, 99))]
    );
    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_rejects_empty_unsafe_range_without_empty_coverage() {
    let root = temp_storage_root("unsafe-empty-coverage");
    let source = MockSource::default().with_safe_height(49, FinalityKind::Safe);
    let service = service(LocalStorage::new(&root), source.clone());

    let error = service
        .query_native(logs_request(
            50,
            50,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("unsafe empty range is rejected");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert_eq!(source.calls(), Vec::<SourceCall>::new());
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_query_accepts_finalized_adapter_height_without_evm_specific_assumption() {
    let root = temp_storage_root("finalized-range");
    let source = MockSource::default()
        .with_blocks(vec![block(7, "0x07")])
        .with_safe_height(7, FinalityKind::Finalized);
    let service = service(LocalStorage::new(&root), source.clone());

    let response = service
        .query_native(blocks_request(7, 7))
        .expect("finalized range succeeds");

    assert_eq!(block_numbers(&response), vec![7]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(7, 7))]
    );
}

#[test]
fn test_registry_routes_query_to_requested_chain_service() {
    let root = temp_storage_root("registry-route");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let ethereum_source = MockSource::default()
        .with_blocks(vec![block(10, "0x0a")])
        .with_chain(ethereum_identity());
    let polygon_source = MockSource::default()
        .with_blocks(vec![block(20, "0x14")])
        .with_chain(polygon_identity());
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            storage.clone(),
            ethereum_source.clone(),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            storage.clone(),
            polygon_source.clone(),
            "polygon",
            137,
        ))
        .expect("register polygon");

    let ethereum = registry
        .query_native(blocks_request_for(ethereum_identity(), 10, 10))
        .expect("ethereum query succeeds");
    let polygon = registry
        .query_native(blocks_request_for(polygon_identity(), 20, 20))
        .expect("polygon query succeeds");

    assert_eq!(block_numbers(&ethereum), vec![10]);
    assert_eq!(block_numbers(&polygon), vec![20]);
    assert_eq!(
        ethereum_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 10))]
    );
    assert_eq!(
        polygon_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(20, 20))]
    );
}

#[test]
fn test_registry_lists_only_registered_chains() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("registry-list-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("registry-list-polygon")),
            MockSource::default().with_chain(polygon_identity()),
            "polygon",
            137,
        ))
        .expect("register polygon");

    assert_eq!(
        registry.chain_names(),
        vec!["ethereum".to_owned(), "polygon".to_owned()]
    );
}

#[tokio::test]
async fn test_chains_route_lists_registered_chains() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("chains-route-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("chains-route-polygon")),
            MockSource::default().with_chain(polygon_identity()),
            "polygon",
            137,
        ))
        .expect("register polygon");

    let response = router(registry)
        .oneshot(
            Request::builder()
                .uri("/v1/chains")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    assert_eq!(
        value,
        serde_json::json!({ "chains": ["ethereum", "polygon"] })
    );
}

#[tokio::test]
async fn test_discovery_route_lists_chain_identities_and_native_dataset_capabilities() {
    let registry = QueryServiceRegistry::new()
        .with_service(service_named_with_datasets(
            LocalStorage::new(temp_storage_root("discovery-ethereum")),
            MockSource::default().with_chain(ethereum_identity()),
            "ethereum",
            1,
            true,
            false,
        ))
        .expect("register ethereum")
        .with_service(QueryService::new_named(
            LocalStorage::new(temp_storage_root("discovery-solana")),
            SolanaAdapter::with_fixture_defaults(),
            planner_config(),
            writer_config(),
            "solana-mainnet-beta",
            solana_chain_config(),
        ))
        .expect("register solana")
        .with_service(QueryService::new_named(
            LocalStorage::new(temp_storage_root("discovery-tron")),
            TronAdapter::with_fixture_defaults(),
            planner_config(),
            writer_config(),
            "tron-mainnet",
            ChainConfig {
                kind: "tron".to_owned(),
                chain_id: 1,
                rpc_url: None,
                rpc_urls: vec!["http://example.invalid".to_owned()],
                rpc: None,
                warmup: Default::default(),
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
                        header_fetch_mode: "batch".to_owned(),
                        header_fetch_concurrency: 8,
                        header_fetch_batch_size: 20,
                        header_cache_max_entries: 50_000,
                    },
                },
            },
        ))
        .expect("register tron");

    let response = router(registry)
        .oneshot(
            Request::builder()
                .uri("/v1/discovery")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("route response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    assert_eq!(
        value,
        serde_json::json!({
            "chains": [
                {
                    "identity": ethereum_identity(),
                    "datasets": [
                        {
                            "dataset_key": "evm.blocks",
                            "range_kinds": [{"kind": "block"}],
                            "selectors": ["all"],
                            "enabled": true
                        }
                    ]
                },
                {
                    "identity": solana_identity(),
                    "datasets": [
                        {
                            "dataset_key": "solana.slots",
                            "range_kinds": [{"kind": "slot"}],
                            "selectors": ["solana_all", "all"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "solana.blocks",
                            "range_kinds": [{"kind": "slot"}],
                            "selectors": ["solana_all", "all"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "solana.transactions",
                            "range_kinds": [{"kind": "slot"}],
                            "selectors": ["all", "solana_address", "solana_program", "solana_signature"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "solana.instructions",
                            "range_kinds": [{"kind": "slot"}],
                            "selectors": ["all", "solana_program"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "solana.account_updates",
                            "range_kinds": [{"kind": "slot"}],
                            "selectors": ["all", "solana_all", "solana_address", "solana_program", "solana_signature"],
                            "enabled": true
                        }
                    ]
                },
                {
                    "identity": tron_mainnet_identity(),
                    "datasets": [
                        {
                            "dataset_key": "tron.blocks",
                            "range_kinds": [{"kind": "block"}],
                            "selectors": ["tron_all"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "tron.transactions",
                            "range_kinds": [{"kind": "block"}],
                            "selectors": ["tron_all"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "tron.transaction_infos",
                            "range_kinds": [{"kind": "block"}],
                            "selectors": ["tron_all"],
                            "enabled": true
                        },
                        {
                            "dataset_key": "tron.events",
                            "range_kinds": [{"kind": "block"}],
                            "selectors": ["tron_all", "tron_events"],
                            "enabled": true
                        }
                    ]
                }
            ]
        })
    );
}

#[test]
fn test_registry_rejects_unknown_chain_without_falling_back_to_first_service() {
    let ethereum_source = MockSource::default()
        .with_blocks(vec![block(10, "0x0a")])
        .with_chain(ethereum_identity());
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            LocalStorage::new(temp_storage_root("registry-unknown")),
            ethereum_source.clone(),
            "ethereum",
            1,
        ))
        .expect("register ethereum");

    let error = registry
        .query_native(blocks_request_for(polygon_identity(), 10, 10))
        .expect_err("polygon is not registered");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert!(error.message.contains("polygon"));
    assert_eq!(ethereum_source.calls(), Vec::<SourceCall>::new());
}

#[test]
fn test_registry_dataset_disabled_is_scoped_to_target_chain() {
    let root = temp_storage_root("registry-disabled-dataset");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let ethereum_source = MockSource::default()
        .with_logs(vec![log(
            10,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        )])
        .with_chain(ethereum_identity());
    let polygon_source = MockSource::default()
        .with_logs(vec![log(
            10,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        )])
        .with_chain(polygon_identity());
    let registry = QueryServiceRegistry::new()
        .with_service(service_named_with_datasets(
            storage.clone(),
            ethereum_source.clone(),
            "ethereum",
            1,
            true,
            false,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            storage.clone(),
            polygon_source.clone(),
            "polygon",
            137,
        ))
        .expect("register polygon");

    let ethereum_error = registry
        .query_native(logs_request_for(
            ethereum_identity(),
            10,
            10,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("ethereum logs are disabled");
    let polygon = registry
        .query_native(logs_request_for(
            polygon_identity(),
            10,
            10,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect("polygon logs are enabled");

    assert_eq!(ethereum_error.kind, DatalensErrorKind::UnsupportedDataset);
    assert_eq!(ethereum_source.calls(), Vec::<SourceCall>::new());
    assert_eq!(log_indexes(&polygon), vec![0]);
    assert_eq!(
        polygon_source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(10, 10))]
    );
}

#[test]
fn test_registry_shared_storage_keeps_manifest_isolated_by_chain_identity() {
    let root = temp_storage_root("registry-manifest-isolation");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            storage.clone(),
            MockSource::default()
                .with_blocks(vec![block(1, "0x01")])
                .with_chain(ethereum_identity()),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(service_named(
            storage.clone(),
            MockSource::default()
                .with_blocks(vec![block(1, "0x89")])
                .with_chain(polygon_identity()),
            "polygon",
            137,
        ))
        .expect("register polygon");

    registry
        .query_native(blocks_request_for(ethereum_identity(), 1, 1))
        .expect("ethereum query succeeds");
    registry
        .query_native(blocks_request_for(polygon_identity(), 1, 1))
        .expect("polygon query succeeds");
    registry
        .wait_for_durable_promotions()
        .expect("promotion drain");

    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(
        root.join("chains/evm/polygon/137/manifest-segments")
            .exists()
    );
    assert!(root.join("chains/evm/ethereum/1/datasets").exists());
    assert!(root.join("chains/evm/polygon/137/datasets").exists());
}

#[test]
fn test_registry_routes_evm_and_solana_native_queries_side_by_side() {
    let root = temp_storage_root("registry-evm-solana");
    let storage: Arc<dyn StorageRepository> = Arc::new(LocalStorage::new(&root));
    let ethereum_source = MockSource::default()
        .with_blocks(vec![block(1, "0x01")])
        .with_chain(ethereum_identity());
    let solana = SolanaAdapter::with_fixture_defaults();
    let solana_chain = solana.capabilities().chain().clone();
    let registry = QueryServiceRegistry::new()
        .with_service(service_named(
            storage.clone(),
            ethereum_source.clone(),
            "ethereum",
            1,
        ))
        .expect("register ethereum")
        .with_service(QueryService::new_named(
            storage.clone(),
            solana,
            PlannerConfig {
                max_query_range_blocks: 10,
                default_chunk_range_blocks: 10,
            },
            WriterConfig {
                target_object_bytes: 1024,
                min_object_rows: 1,
                record_empty_coverage: true,
                staging: Default::default(),
            },
            "solana-mainnet-beta",
            solana_chain_config(),
        ))
        .expect("register solana");

    let ethereum = registry
        .query_native(blocks_request_for(ethereum_identity(), 1, 1))
        .expect("ethereum query succeeds");
    let solana_response = registry
        .query_native(NativeQueryInput {
            chain: solana_chain,
            dataset_key: DatasetKey::solana_slots(),
            ledger_range: LedgerRange::slots(10, 12).expect("valid range"),
            selector: solana_all_selector().expect("selector"),
            field_selection: FieldSelection::All,
            finality: QueryFinalityRequirement::DurableOnly,
        })
        .expect("solana query succeeds");
    registry
        .wait_for_durable_promotions()
        .expect("promotion drain");

    assert_eq!(block_numbers(&ethereum), vec![1]);
    let QueryRows::AdapterJson { rows, .. } = solana_response.rows.rows() else {
        panic!("expected Solana adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["slot"].as_u64().expect("slot"))
            .collect::<Vec<_>>(),
        vec![10, 12]
    );
    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(
        root.join("chains/solana/solana-mainnet-beta/mainnet-beta/manifest-segments")
            .exists()
    );
}
