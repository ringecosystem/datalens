use datalens_edge::config::DatalensConfig;

#[test]
fn test_config_edge_graphql_namespace_controls_graphql_settings() {
    let config: DatalensConfig =
        toml::from_str(&config_text("[edge.graphql]")).expect("edge graphql config should parse");

    assert!(!config.edge.graphql.enabled);
    assert!(!config.edge.graphql.playground_enabled);
}

#[test]
fn test_config_api_graphql_namespace_is_not_supported() {
    let old_header = format!("[{}.graphql]", "api");
    let error = toml::from_str::<DatalensConfig>(&config_text(&old_header))
        .expect_err("old api graphql config namespace should be rejected");

    assert!(error.to_string().contains("unknown field `api`"));
}

#[test]
fn test_config_storage_root_field_is_not_supported() {
    let config = config_text("[edge.graphql]").replace(
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

fn config_text(graphql_header: &str) -> String {
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

        {graphql_header}
        enabled = false
        playground_enabled = false

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
