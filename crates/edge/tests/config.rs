use datalens_edge::config::DatalensConfig;

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
fn test_config_embeds_application_index_config_under_service_index() {
    let config: DatalensConfig = toml::from_str(&config_text("[query.native]").replace(
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
    .expect("service config with embedded application index should parse");

    assert!(config.index.application.is_some());
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
