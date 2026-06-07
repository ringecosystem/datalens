mod support;

use support::lifecycle::*;

#[test]
fn test_local_lifecycle_records_metrics_for_miss_fill_hit_and_provider_error() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let source = MockSource::default().with_blocks(vec![block(10), block(11)]);
    let lifecycle_service = service(
        LocalStorage::new(temp_storage_root("metrics-lifecycle")),
        source.clone(),
    )
    .with_metrics(recorder.clone());
    let request = blocks_request(10, 11);

    lifecycle_service
        .query_native(request.clone())
        .expect("miss fills cache");
    lifecycle_service
        .query_native(request)
        .expect("hit reads cache");

    let metrics = recorder.encode().expect("metrics text");
    assert!(metrics.contains(
        r#"datalens_query_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_query_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="hit"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_cache_coverage_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="miss"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_cache_coverage_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="hit"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_fill_total{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks",outcome="filled"} 1"#
    ));
    assert!(metrics.contains(
        r#"datalens_application_chain_latest_requested_block{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks"} 11"#
    ));
    assert!(metrics.contains(
        r#"datalens_application_chain_latest_filled_block{application="unknown",chain="ethereum",chain_kind="evm",dataset="evm.blocks"} 11"#
    ));
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 11))]
    );

    let error_source = MockSource::default().with_error(DatalensErrorKind::ProviderTimeout);
    let error_service = service(
        LocalStorage::new(temp_storage_root("metrics-error")),
        error_source,
    )
    .with_metrics(recorder.clone());
    let error = error_service
        .query_native(blocks_request(20, 20))
        .expect_err("provider error");
    assert_eq!(error.kind, DatalensErrorKind::ProviderTimeout);

    let metrics = recorder.encode().expect("metrics text");
    assert!(metrics.contains(
        r#"datalens_provider_error_total{chain="ethereum",chain_kind="evm",dataset="evm.blocks",error_kind="provider_timeout"} 1"#
    ));
}

#[tokio::test]
async fn test_api_lifecycle_routes_expose_health_aliases_chains_query_and_metrics() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let source = MockSource::default().with_blocks(vec![block(30)]);
    let service = service(
        LocalStorage::new(temp_storage_root("api-lifecycle")),
        source.clone(),
    )
    .with_metrics(recorder);
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let app = router(registry);

    for path in ["/health", "/healthz"] {
        let health = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).expect("request"))
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(
            body_json(health.into_body()).await,
            serde_json::json!({ "status": "ok" })
        );
    }

    let chains = app
        .clone()
        .oneshot(
            Request::get("/v1/chains")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("chains response");
    assert_eq!(chains.status(), StatusCode::OK);
    let chains_body = body_json(chains.into_body()).await;
    assert_eq!(chains_body["chains"], serde_json::json!(["ethereum"]));

    let query = app
        .clone()
        .oneshot(
            Request::post("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body(blocks_request(30, 30))))
                .expect("request"),
        )
        .await
        .expect("query response");
    assert_eq!(query.status(), StatusCode::OK);

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = body_text(metrics.into_body()).await;
    assert!(body.contains("# HELP datalens_query_total"));
    assert!(body.contains(r#"chain="ethereum""#));
    assert!(body.contains(r#"chain_kind="evm""#));
    assert!(body.contains(r#"dataset="evm.blocks""#));
}

#[tokio::test]
async fn test_service_lifecycle_stops_scheduler_before_shutdown_flush() {
    let root = temp_storage_root("api-lifecycle-scheduler-before-flush");
    let storage = LocalStorage::new(&root);
    let source = MockSource::default().with_blocks(vec![block(10)]);
    let service = QueryService::new(
        storage.clone(),
        source,
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: WriterStagingConfig {
                enabled: true,
                min_rows: Some(3),
                target_object_bytes: None,
                max_staged_ranges: None,
                max_staged_rows: None,
                max_staged_age_ms: None,
                flush_on_shutdown: true,
                max_staged_bytes: None,
            },
        },
        chain_config(1),
    );
    let registry = QueryServiceRegistry::new()
        .with_service(service)
        .expect("registry");
    let events = Arc::new(Mutex::new(Vec::new()));
    let scheduler = ControlledShutdownScheduler {
        registry: registry.clone(),
        events: events.clone(),
    };

    ServiceLifecycle::new(registry.clone())
        .with_warmup_scheduler(scheduler)
        .shutdown()
        .expect("shutdown lifecycle");

    assert_eq!(
        *events.lock().expect("events"),
        vec!["scheduler_shutdown".to_owned()]
    );
    let manifest = storage.manifest().expect("manifest after shutdown");
    assert_eq!(
        manifest.entries.len(),
        1,
        "scheduler-created staged rows should be flushed during final shutdown flush"
    );
    let hit = registry
        .query_native(blocks_request(10, 10))
        .expect("query hits flushed scheduler rows");
    assert_eq!(
        hit.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(10, 10).expect("range")]
    );
    let flush = registry
        .flush_staged_writes_for_shutdown()
        .expect("second shutdown flush");
    assert!(
        flush.iter().all(|result| result.data_objects.is_empty()),
        "no staged rows should remain after lifecycle shutdown"
    );
}

#[test]
fn test_local_lifecycle_covers_multichain_storage_isolation_and_unknown_chain() {
    let root = temp_storage_root("multi-chain");
    let ethereum_source = MockSource::default()
        .with_chain(ethereum_identity())
        .with_blocks(vec![block(40)]);
    let polygon_source = MockSource::default()
        .with_chain(polygon_identity())
        .with_blocks(vec![BlockHeader {
            hash: "0xpolygon".to_owned(),
            ..block(40)
        }]);
    let ethereum = service_named(
        LocalStorage::new(&root),
        ethereum_source.clone(),
        "ethereum",
        chain_config(1),
    );
    let polygon = service_named(
        LocalStorage::new(&root),
        polygon_source.clone(),
        "polygon",
        chain_config(137),
    );

    ethereum
        .query_native(NativeQueryInput {
            chain: ethereum_identity(),
            ..blocks_request(40, 40)
        })
        .expect("ethereum query");
    polygon
        .query_native(NativeQueryInput {
            chain: polygon_identity(),
            ..blocks_request(40, 40)
        })
        .expect("polygon query");

    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(
        root.join("chains/evm/polygon/137/manifest-segments")
            .exists()
    );
    assert_eq!(
        ethereum_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(40, 40))]
    );
    assert_eq!(
        polygon_source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(40, 40))]
    );

    let error = ethereum
        .query_native(NativeQueryInput {
            chain: polygon_identity(),
            ..blocks_request(40, 40)
        })
        .expect_err("unknown chain for route");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_empty_logs_lifecycle_records_empty_coverage_without_data_object_and_hits_cache() {
    let root = temp_storage_root("empty-logs");
    let source = MockSource::default();
    let service = service(LocalStorage::new(&root), source.clone());
    let request = logs_request(50, 51);

    let first = service
        .query_native(request.clone())
        .expect("empty logs miss");
    let second = service.query_native(request).expect("empty logs hit");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(50, 51).expect("range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(50, 51).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(50, 51))]
    );
    assert!(
        root.join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_local_lifecycle_returns_provider_rows_when_durable_write_fails_without_coverage() {
    let root = temp_storage_root("write-failure-provider-rows");
    let source = MockSource::default().with_blocks(vec![block(65)]);
    let service = QueryService::new_named(
        FailingWriteStorage::new(LocalStorage::new(&root)),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
    );

    let response = service
        .query_native(blocks_request(65, 65))
        .expect("provider rows are returned despite durable write failure");

    assert_eq!(block_numbers(&response), vec![65]);
    assert_eq!(
        response.cache.missing_ranges,
        vec![LedgerRange::blocks(65, 65).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(65, 65))]
    );
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_local_lifecycle_durable_hit_reads_through_cache_after_manifest_coverage() {
    let root = temp_storage_root("read-through-lifecycle");
    let store = CountingObjectStore::new(root);
    let storage = DurableStorage::from_object_store_with_read_through_cache_config(
        store.clone(),
        ReadThroughCacheConfig::enabled(16),
    );
    let source = MockSource::default().with_blocks(vec![block(70), block(71)]);
    let service = QueryService::new_named(
        storage.clone(),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
    );
    let request = blocks_request(70, 71);

    let first = service
        .query_native(request.clone())
        .expect("miss fills cache");
    let object_key = storage
        .manifest()
        .expect("manifest")
        .entries
        .into_iter()
        .find_map(|entry| entry.object_key)
        .expect("object key");
    let second = service
        .query_native(request.clone())
        .expect("first durable hit");
    let third = service.query_native(request).expect("second durable hit");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(70, 71).expect("range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(70, 71).expect("range")]
    );
    assert_eq!(
        third.cache.hit_ranges,
        vec![LedgerRange::blocks(70, 71).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(70, 71))]
    );
    assert_eq!(store.read_count(&object_key), 1);
}

#[test]
fn test_s3_lifecycle_is_gated_and_uses_dedicated_prefix() {
    let Some(config) = s3_test_config() else {
        return;
    };
    let store = S3ObjectStore::from_config(config).expect("build S3 object store");
    cleanup_s3_prefix(&store);
    let storage = DurableStorage::from_object_store(store.clone());
    let source = MockSource::default().with_blocks(vec![block(60)]);
    let service = QueryService::new_named(
        storage,
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 8,
            default_chunk_range_blocks: 4,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        "ethereum",
        chain_config(1),
    );
    let request = blocks_request(60, 60);

    let first = service
        .query_native(request.clone())
        .expect("S3 miss fills cache");
    let second = service.query_native(request).expect("S3 hit reads cache");

    assert_eq!(
        first.cache.missing_ranges,
        vec![LedgerRange::blocks(60, 60).expect("range")]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![LedgerRange::blocks(60, 60).expect("range")]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(60, 60))]
    );

    let manifest = DurableStorage::from_object_store(store.clone())
        .manifest()
        .expect("S3 manifest");
    assert_eq!(manifest.entries.len(), 1);
    let entry = &manifest.entries[0];
    assert_eq!(entry.chain, ethereum_identity());
    assert_eq!(entry.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(entry.row_count, 1);
    assert!(entry.object_key.is_some());
    assert!(entry.object_size_bytes.is_some_and(|size| size > 0));
    assert_eq!(entry.checksum_algorithm.as_deref(), Some("sha256"));

    cleanup_s3_prefix(&store);
}
