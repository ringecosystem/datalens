use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};

use datalens_core::{ChainId, ChainName, Dataset};
use datalens_server::config::{Config, ConfigError, FinalityConfig, Role, Secret};

const MINIMAL: &str = r#"
[server]
bind = "127.0.0.1:3000"
roles = ["api"]

[database]
url = "${DB_URL}"

[metrics]
bearer_token = "metrics-${SUFFIX}"

[chains.arbitrum]
chain_id = 42161
rpc_url = "${ARB_RPC}"
finality = { mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 256 }
chunk_blocks = 10000
max_get_logs_range_blocks = 10000
max_addresses_per_query = 32
provider = { rps = 20, burst = 40, max_in_flight = 8 }

[[applications]]
id = "degov-live"
token = "${TOKEN}"
chains = ["arbitrum"]
datasets = ["evm.logs"]
quota = { max_range_blocks = 200000, max_requests_per_minute = 1800, max_concurrent_requests = 96, max_subscriptions = 200 }
"#;

fn env(name: &str) -> Option<String> {
    match name {
        "DB_URL" => Some("postgres://u:p@db/datalens".into()),
        "SUFFIX" => Some("abc".into()),
        "ARB_RPC" => Some("http://erpc/arbitrum".into()),
        "TOKEN" => Some("live-token".into()),
        _ => None,
    }
}

fn parse(text: &str) -> Result<Config, ConfigError> {
    Config::from_toml(text, &env)
}

fn error_of(text: &str) -> String {
    parse(text).unwrap_err().to_string()
}

#[test]
fn minimal_config_parses_with_defaults_and_interpolation() {
    let config = parse(MINIMAL).unwrap();
    assert_eq!(config.server.bind.to_string(), "127.0.0.1:3000");
    assert_eq!(config.server.roles, BTreeSet::from([Role::Api]));
    assert_eq!(
        config.database.url,
        Secret::new("postgres://u:p@db/datalens")
    );
    assert_eq!(config.database.max_connections.get(), 32);
    assert_eq!(config.metrics.bearer_token, Secret::new("metrics-abc"));
    assert_eq!(config.query.max_wait_ms, 15_000);
    assert_eq!(config.query.page_limit.get(), 10_000);
    assert_eq!(config.query.follow_attach_blocks, 1_000);
    assert_eq!(config.follow.subscription_ttl_s.get(), 900);
    assert_eq!(config.follow.unsafe_retention_s.get(), 86_400);

    let arbitrum = &config.chains[&ChainName::new("arbitrum").unwrap()];
    assert_eq!(arbitrum.chain_id, ChainId::new(42161).unwrap());
    assert_eq!(arbitrum.rpc_url.expose(), "http://erpc/arbitrum");
    assert_eq!(
        arbitrum.finality,
        FinalityConfig::Lag {
            safe_lag_blocks: 64,
            finalized_lag_blocks: 256
        }
    );
    assert_eq!(arbitrum.tick_ms.get(), 2_000);
    assert_eq!(arbitrum.reorg_window_blocks.get(), 512);
    assert_eq!(arbitrum.provider.rps, NonZeroU32::new(20).unwrap());
    assert_eq!(arbitrum.provider.daily_calls, None);
    assert_eq!(
        arbitrum.provider.min_range_blocks,
        NonZeroU64::new(16).unwrap()
    );

    let app = &config.applications[0];
    assert_eq!(app.id, "degov-live");
    assert_eq!(app.token.expose(), "live-token");
    assert_eq!(app.datasets, BTreeSet::from([Dataset::EvmLogs]));
    assert!(!app.quota.allow_unfiltered_logs);
    assert_eq!(app.quota.max_fill_blocks_per_minute, None);
}

#[test]
fn secrets_are_redacted_in_debug_output() {
    let debug = format!("{:?}", parse(MINIMAL).unwrap());
    assert!(!debug.contains("live-token"), "{debug}");
    assert!(!debug.contains("postgres://u:p@db"), "{debug}");
    assert!(debug.contains("Secret(***)"));
}

#[test]
fn rpc_tags_finality_mode() {
    let text = MINIMAL.replace(
        r#"finality = { mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 256 }"#,
        r#"finality = { mode = "rpc_tags" }"#,
    );
    let config = parse(&text).unwrap();
    assert_eq!(
        config.chains[&ChainName::new("arbitrum").unwrap()].finality,
        FinalityConfig::RpcTags
    );
}

#[test]
fn missing_env_is_named_and_dollar_without_brace_is_literal() {
    assert_eq!(
        error_of(&MINIMAL.replace("${TOKEN}", "${UNSET_TOKEN}")),
        "environment variable UNSET_TOKEN is not set"
    );
    let config = parse(&MINIMAL.replace("${TOKEN}", "a$b")).unwrap();
    assert_eq!(config.applications[0].token.expose(), "a$b");
    assert_eq!(
        error_of(&MINIMAL.replace("${TOKEN}", "${TOKEN")),
        "unterminated `${` placeholder"
    );
    assert_eq!(
        error_of(&MINIMAL.replace("${TOKEN}", "${1X}")),
        "invalid placeholder name \"1X\""
    );
}

#[test]
fn unknown_fields_are_rejected() {
    assert!(error_of(&MINIMAL.replace("chunk_blocks", "chunk_size")).contains("unknown field"));
    assert!(
        error_of(&format!("{MINIMAL}\n[storage]\nbackend = \"s3\"\n")).contains("unknown field")
    );
    let text = MINIMAL.replace(r#"mode = "lag","#, r#"mode = "lag", extra = 1,"#);
    assert!(
        error_of(&text).contains("unknown field"),
        "{}",
        error_of(&text)
    );
}

#[test]
fn empty_roles_and_unknown_role_are_rejected() {
    assert_eq!(
        error_of(&MINIMAL.replace(r#"roles = ["api"]"#, "roles = []")),
        "invalid config: server.roles must list at least one of api, worker"
    );
    assert!(
        error_of(&MINIMAL.replace(r#"roles = ["api"]"#, r#"roles = ["edge"]"#))
            .contains("unknown variant")
    );
}

#[test]
fn duplicate_chain_id_is_rejected() {
    let text = format!(
        "{MINIMAL}\n[chains.arb2]\nchain_id = 42161\nrpc_url = \"x\"\nfinality = {{ mode = \"rpc_tags\" }}\n\
         chunk_blocks = 1\nmax_get_logs_range_blocks = 1\nmax_addresses_per_query = 1\n\
         provider = {{ rps = 1, burst = 1, max_in_flight = 1 }}\n"
    );
    assert_eq!(
        error_of(&text),
        "invalid config: chains.arbitrum: chain_id 42161 is used by another chain"
    );
}

#[test]
fn application_references_must_exist() {
    assert_eq!(
        error_of(&MINIMAL.replace(
            r#"chains = ["arbitrum"]"#,
            r#"chains = ["arbitrum", "base"]"#
        )),
        "invalid config: applications.degov-live: unknown chain \"base\""
    );
    assert!(
        error_of(&MINIMAL.replace(r#"["evm.logs"]"#, r#"["evm.transactions"]"#))
            .contains("unknown variant")
    );
}

#[test]
fn duplicate_application_id_and_token_are_rejected() {
    let app = MINIMAL.split("[[applications]]").nth(1).unwrap();
    assert_eq!(
        error_of(&format!("{MINIMAL}[[applications]]{app}")),
        "invalid config: applications: duplicate id \"degov-live\""
    );
    let second = app.replace("degov-live", "ormp");
    assert_eq!(
        error_of(&format!("{MINIMAL}[[applications]]{second}")),
        "invalid config: applications.ormp: token is shared with another application"
    );
}

#[test]
fn invalid_values_are_rejected() {
    assert!(parse(&MINIMAL.replace("chain_id = 42161", "chain_id = 0")).is_err());
    assert!(parse(&MINIMAL.replace("chunk_blocks = 10000", "chunk_blocks = 0")).is_err());
    assert!(parse(&MINIMAL.replace("[chains.arbitrum]", "[chains.Arbitrum]")).is_err());
    assert_eq!(
        error_of(&MINIMAL.replace("safe_lag_blocks = 64", "safe_lag_blocks = 300")),
        "invalid config: chains.arbitrum.finality: safe_lag_blocks 300 exceeds finalized_lag_blocks 256"
    );
}

fn placeholder_env(name: &str) -> Option<String> {
    Some(format!("value-of-{name}"))
}

#[test]
fn shipped_example_config_parses() {
    let config = Config::from_toml(
        include_str!("../../../config/datalens.example.toml"),
        &placeholder_env,
    )
    .unwrap();
    let chains: BTreeMap<String, i32> = config
        .chains
        .iter()
        .map(|(name, chain)| (name.to_string(), chain.chain_id.get()))
        .collect();
    assert_eq!(
        chains,
        BTreeMap::from([
            ("arbitrum".to_owned(), 42161),
            ("base".to_owned(), 8453),
            ("bsc".to_owned(), 56),
            ("darwinia".to_owned(), 46),
            ("ethereum".to_owned(), 1),
            ("lisk".to_owned(), 1135),
            ("optimism".to_owned(), 10),
            ("polygon".to_owned(), 137),
        ])
    );
    assert_eq!(
        config.chains[&ChainName::new("darwinia").unwrap()]
            .max_get_logs_range_blocks
            .get(),
        1000
    );
    let ids: Vec<&str> = config.applications.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(ids, ["degov-live", "degov-stg", "degov-realtime", "ormp"]);
    assert_eq!(
        config.server.roles,
        BTreeSet::from([Role::Api, Role::Worker])
    );
}

#[test]
fn shipped_dev_config_parses() {
    let config = Config::from_toml(
        include_str!("../../../config/datalens.dev.toml"),
        &placeholder_env,
    )
    .unwrap();
    let names: Vec<String> = config.chains.keys().map(ToString::to_string).collect();
    assert_eq!(names, ["ethereum"]);
    assert_eq!(
        config.chains[&ChainName::new("ethereum").unwrap()]
            .rpc_url
            .expose(),
        "value-of-DATALENS_ETHEREUM_RPC_URL"
    );
}
