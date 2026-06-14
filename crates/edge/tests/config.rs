use datalens_edge::config::DatalensConfig;
use datalens_storage::ParquetCompression;

#[test]
fn test_config_production_ethereum_rpc_pool_and_log_reliability() {
    set_production_config_env();

    let config_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/datalens.production.toml")
        .canonicalize()
        .expect("production config path");
    let config = DatalensConfig::from_file(&config_path).expect("production config should parse");

    let chain = &config.chains["ethereum"];
    assert_eq!(
        chain.primary_rpc_url(),
        Some("http://primary.example.invalid")
    );
    assert_eq!(
        chain.secondary_rpc_urls(),
        &["http://secondary.example.invalid".to_owned()]
    );
    assert!(chain.datasets.logs.reliability_enabled);
}

#[test]
fn test_config_query_native_namespace_controls_native_graphql_settings() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[query.native]")).expect("query native config should parse");

    assert!(!config.query.native.graphql_enabled);
    assert_eq!(config.query.native.path, "/native/graphql");
    assert!(!config.query.native.playground_enabled);
    assert_eq!(config.query.native.playground_path, "/native/graphiql");
    assert!(!config.query.index.graphql_enabled);
    assert_eq!(config.query.index.path, "/index/graphql");
    assert!(!config.query.index.playground_enabled);
    assert_eq!(config.query.index.playground_path, "/index/graphiql");
}

#[test]
fn test_config_query_index_namespace_controls_index_graphql_settings() {
    let config: DatalensConfig = toml::from_str(&config_text("[query.index]").replace(
        r#"graphql_enabled = false
        path = "/native/graphql"
        playground_enabled = false
        playground_path = "/native/graphiql""#,
        r#"graphql_enabled = true
        path = "/indexes/graphql"
        playground_enabled = true
        playground_path = "/indexes/graphiql""#,
    ))
    .expect("query index config should parse");

    assert!(!config.query.native.graphql_enabled);
    assert!(config.query.index.graphql_enabled);
    assert_eq!(config.query.index.path, "/indexes/graphql");
    assert!(config.query.index.playground_enabled);
    assert_eq!(config.query.index.playground_path, "/indexes/graphiql");
}

#[test]
fn test_config_rejects_embedded_application_index_config_under_service_index() {
    let error = toml::from_str::<DatalensConfig>(&config_text("[query.native]").replace(
        r#"
        [chains.ethereum]"#,
        r#"
        [index.application.client]
        endpoint = "http://127.0.0.1:3000"
        application = "ormp"
        token_env = "PATH"

        [index.application.index]
        name = "ormp"
        dataset = "evm.logs"
        finality = "durable"
        chunk_blocks = 100

        [[index.application.sources]]
        chain = "ethereum"
        family = "evm"
        chain_id = 1
        from_block = 1
        to_block = 10
        addresses = []
        topics = []

        [index.application.output.jsonl]
        path = ".data/indexes/ormp/events.jsonl"

        [index.application.checkpoint]
        path = ".data/indexes/ormp/checkpoint.json"

        [chains.ethereum]"#,
    ))
    .expect_err("service config must not embed application index config");

    assert!(error.to_string().contains("unknown field `application`"));
}

#[test]
fn test_config_edge_graphql_namespace_is_not_supported() {
    let error = toml::from_str::<DatalensConfig>(&config_text("[edge.graphql]"))
        .expect_err("old edge graphql config namespace should be rejected");

    assert!(error.to_string().contains("unknown field `graphql`"));
}

#[test]
fn test_config_storage_root_field_is_not_supported() {
    let config = config_text("[query.native]").replace(
        r#"
        [storage.local]
        root = ".tmp/datalens-config-test"
"#,
        r#"        root = ".tmp/datalens-config-test"
"#,
    );
    let error = toml::from_str::<DatalensConfig>(&config)
        .expect_err("top-level storage root should be rejected");

    assert!(error.to_string().contains("unknown field `root`"));
}

#[test]
fn test_config_storage_parquet_compression_defaults_to_disabled() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[query.native]")).expect("config should parse");

    assert_eq!(config.storage.parquet.compression, ParquetCompression::None);
}

#[test]
fn test_config_warmup_follow_query_lifecycle_thresholds_default_to_unset() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[query.native]")).expect("config should parse");

    assert_eq!(config.warmup.follow_query_idle_threshold_blocks, None);
    assert_eq!(config.warmup.follow_query_resume_threshold_blocks, None);
}

#[test]
fn test_config_evm_log_header_fetch_mode_defaults_to_batch() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[query.native]")).expect("config should parse");

    assert_eq!(
        config.chains["ethereum"].datasets.logs.header_fetch_mode,
        "batch"
    );
}

#[test]
fn test_config_evm_log_reliability_defaults_to_enabled() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[query.native]")).expect("config should parse");

    assert!(config.chains["ethereum"].datasets.logs.reliability_enabled);
    assert!(
        config.chains["ethereum"]
            .datasets
            .logs
            .receipt_fallback_enabled
    );
}

#[test]
fn test_config_evm_log_reliability_can_be_disabled() {
    let input = config_text("[query.native]").replace(
        r#"enabled = true
        max_get_logs_range_blocks = 10"#,
        r#"enabled = true
        reliability_enabled = false
        max_get_logs_range_blocks = 10"#,
    );
    let config: DatalensConfig = toml::from_str(&input).expect("config should parse");

    assert!(!config.chains["ethereum"].datasets.logs.reliability_enabled);
}

#[test]
fn test_config_evm_log_receipt_fallback_can_be_disabled() {
    let input = config_text("[query.native]").replace(
        r#"enabled = true
        max_get_logs_range_blocks = 10"#,
        r#"enabled = true
        receipt_fallback_enabled = false
        max_get_logs_range_blocks = 10"#,
    );
    let config: DatalensConfig = toml::from_str(&input).expect("config should parse");

    assert!(config.chains["ethereum"].datasets.logs.reliability_enabled);
    assert!(
        !config.chains["ethereum"]
            .datasets
            .logs
            .receipt_fallback_enabled
    );
}

#[test]
fn test_config_legacy_rpc_urls_provides_primary_rpc() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[query.native]")).expect("config should parse");

    let chain = &config.chains["ethereum"];
    assert_eq!(chain.primary_rpc_url(), Some("http://example.invalid"));
    assert!(chain.secondary_rpc_urls().is_empty());
    assert_eq!(
        chain.rpc_provider_urls(),
        vec!["http://example.invalid".to_owned()]
    );
}

#[test]
fn test_config_legacy_rpc_url_provides_primary_rpc() {
    let input = config_text("[query.native]").replace(
        r#"rpc_urls = ["http://example.invalid"]"#,
        r#"rpc_url = "http://legacy.example.invalid""#,
    );
    let config: DatalensConfig = toml::from_str(&input).expect("config should parse");

    let chain = &config.chains["ethereum"];
    assert_eq!(
        chain.primary_rpc_url(),
        Some("http://legacy.example.invalid")
    );
    assert!(chain.secondary_rpc_urls().is_empty());
}

#[test]
fn test_config_rpc_pool_parses_primary_and_secondary_urls() {
    let input = config_text("[query.native]").replace(
        r#"rpc_urls = ["http://example.invalid"]"#,
        r#"[chains.ethereum.rpc]
        primary_url = "http://primary.example.invalid"
        secondary_urls = [
            "http://secondary-a.example.invalid",
            "http://secondary-b.example.invalid",
        ]"#,
    );
    let config: DatalensConfig = toml::from_str(&input).expect("config should parse");

    let chain = &config.chains["ethereum"];
    assert_eq!(
        chain.primary_rpc_url(),
        Some("http://primary.example.invalid")
    );
    assert_eq!(
        chain.secondary_rpc_urls(),
        &[
            "http://secondary-a.example.invalid".to_owned(),
            "http://secondary-b.example.invalid".to_owned()
        ]
    );
    assert_eq!(
        chain.rpc_provider_urls(),
        vec![
            "http://primary.example.invalid".to_owned(),
            "http://secondary-a.example.invalid".to_owned(),
            "http://secondary-b.example.invalid".to_owned()
        ]
    );
}

#[test]
fn test_config_storage_parquet_compression_accepts_zstd_and_snappy() {
    for codec in ["zstd", "snappy"] {
        let input = config_text("[query.native]").replace(
            r#"
        [planner]"#,
            &format!(
                r#"
        [storage.parquet]
        compression = "{codec}"

        [planner]"#
            ),
        );
        let config: DatalensConfig = toml::from_str(&input).expect("config should parse");

        assert_eq!(config.storage.parquet.compression.as_str(), codec);
    }
}

#[test]
fn test_config_storage_parquet_compression_accepts_none() {
    let input = config_text("[query.native]").replace(
        r#"
        [planner]"#,
        r#"
        [storage.parquet]
        compression = "none"

        [planner]"#,
    );
    let config: DatalensConfig = toml::from_str(&input).expect("config should parse");

    assert_eq!(config.storage.parquet.compression, ParquetCompression::None);
}

#[test]
fn test_config_storage_parquet_compression_rejects_unknown_codec() {
    let input = config_text("[query.native]").replace(
        r#"
        [planner]"#,
        r#"
        [storage.parquet]
        compression = "gzip"

        [planner]"#,
    );
    let error =
        toml::from_str::<DatalensConfig>(&input).expect_err("unknown codec should be rejected");

    assert!(error.to_string().contains("unknown variant `gzip`"));
}

fn config_text(query_header: &str) -> String {
    format!(
        r#"
        [server]
        bind = "127.0.0.1:0"

        [storage]
        backend = "local"

        [storage.local]
        root = ".tmp/datalens-config-test"

        [planner]
        max_query_range_blocks = 100
        default_chunk_range_blocks = 10

        [writer]
        target_object_bytes = 1024
        min_object_rows = 1
        record_empty_coverage = true

        {query_header}
        graphql_enabled = false
        path = "/native/graphql"
        playground_enabled = false
        playground_path = "/native/graphiql"

        [chains.ethereum]
        kind = "evm"
        chain_id = 1
        rpc_urls = ["http://example.invalid"]

        [chains.ethereum.datasets.blocks]
        enabled = true
        max_batch_blocks = 10

        [chains.ethereum.datasets.logs]
        enabled = true
        max_get_logs_range_blocks = 10
        max_addresses_per_query = 2
        "#
    )
}

fn set_production_config_env() {
    unsafe {
        std::env::set_var("DATALENS_S3_BUCKET", "datalens");
        std::env::set_var("DATALENS_S3_PREFIX", "test");
        std::env::set_var("DATALENS_S3_REGION", "auto");
        std::env::set_var("DATALENS_S3_ENDPOINT_URL", "http://127.0.0.1:9000");
        std::env::set_var("DATALENS_METRICS_TOKEN", "replace-with-metrics-token");
        std::env::set_var("DATALENS_PUBLIC_APP_TOKEN", "replace-with-public-token");
        std::env::set_var(
            "DATALENS_ETHEREUM_RPC_URL",
            "http://primary.example.invalid",
        );
        std::env::set_var(
            "DATALENS_ETHEREUM_SECONDARY_RPC_URL",
            "http://secondary.example.invalid",
        );
    }
}
