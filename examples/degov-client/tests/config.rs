use std::sync::Mutex;
use std::{fs, path::PathBuf};

use alloy_primitives::keccak256;
use datalens_example_degov_client::{
    config::{AppConfig, DEFAULT_EVENT_TOPIC0, DegovFixtureFile},
    datalens::VOTE_CAST_SIGNATURE,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_vote_cast_default_topic_matches_signature() {
    let topic0 = format!("{:#x}", keccak256(VOTE_CAST_SIGNATURE.as_bytes()));

    assert_eq!(DEFAULT_EVENT_TOPIC0, topic0);
}

#[test]
fn test_from_env_requires_live_vote_cast_selector_config() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();

    let error = AppConfig::from_env().expect_err("missing live selector config should fail");

    assert_eq!(
        error.to_string(),
        "DEGOV_CONTRACT_ADDRESS, DEGOV_EVENT_TOPIC0, DEGOV_START_BLOCK, and DEGOV_END_BLOCK are required for live VoteCast indexing"
    );
}

#[test]
fn test_from_env_accepts_vote_cast_topic() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    set_env(
        "DEGOV_CONTRACT_ADDRESS",
        "0x1111111111111111111111111111111111111111",
    );
    set_env("DEGOV_EVENT_TOPIC0", DEFAULT_EVENT_TOPIC0);
    set_env("DEGOV_START_BLOCK", "100");
    set_env("DEGOV_END_BLOCK", "200");

    let config = AppConfig::from_env().expect("vote cast config");

    assert_eq!(config.event_topic0, DEFAULT_EVENT_TOPIC0);
    assert_eq!(config.event_signature, VOTE_CAST_SIGNATURE);
}

#[test]
fn test_from_env_rejects_non_vote_cast_topic() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    set_env(
        "DEGOV_CONTRACT_ADDRESS",
        "0x1111111111111111111111111111111111111111",
    );
    set_env("DEGOV_START_BLOCK", "100");
    set_env("DEGOV_END_BLOCK", "200");
    set_env(
        "DEGOV_EVENT_TOPIC0",
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    );

    let error = AppConfig::from_env().expect_err("transfer topic should fail");

    assert_eq!(
        error.to_string(),
        "DEGOV_EVENT_TOPIC0 must match VoteCast(address,uint256,uint8,uint256,string)"
    );
}

#[test]
fn test_fixture_file_loads_multiple_live_workloads() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    let path = temp_fixture_path("multiple-live-workloads");
    fs::write(
        &path,
        r#"
[[workloads]]
name = "compound-dao"
chain_name = "ethereum"
chain_id = 1
contract_address = "0x309a862bbC1A00e45506cB8A802D1ff10004c8C0"
start_block = 23900000
end_block = 23909999

[[workloads]]
name = "gmx-dao"
chain_name = "arbitrum"
chain_id = 42161
contract_address = "0x03e8f708e9C85EDCEaa6AD7Cd06824CeB82A7E68"
start_block = 435080000
end_block = 435089999
chunk_size = 5000
"#,
    )
    .expect("write fixture config");

    let fixture = DegovFixtureFile::from_path(&path).expect("fixture file");

    assert_eq!(fixture.workloads.len(), 2);
    assert_eq!(fixture.workloads[0].name, "compound-dao");
    assert_eq!(fixture.workloads[1].chain_name, "arbitrum");
    assert_eq!(fixture.workloads[1].chunk_size, Some(5000));

    let _ = fs::remove_file(path);
}

#[test]
fn test_from_fixture_workload_overrides_selector_and_checkpoint_scope() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    set_env("DEGOV_DATABASE_URL", "sqlite:.tmp/degov-fixtures.sqlite");
    set_env("DEGOV_CHUNK_SIZE", "250");

    let fixture = DegovFixtureFile::from_toml_str(
        r#"
[[workloads]]
name = "seamless-dao"
chain_name = "base"
chain_id = 8453
contract_address = "0x8768c789C6df8AF1a92d96dE823b4F80010Db294"
start_block = 21040000
end_block = 21049999
"#,
    )
    .expect("fixture file");

    let config = AppConfig::from_fixture_workload(&fixture.workloads[0]).expect("workload config");

    assert_eq!(config.database_url, "sqlite:.tmp/degov-fixtures.sqlite");
    assert_eq!(config.chain_name, "base");
    assert_eq!(config.chain_id, 8453);
    assert_eq!(
        config.contract_address,
        "0x8768c789C6df8AF1a92d96dE823b4F80010Db294"
    );
    assert_eq!(config.event_topic0, DEFAULT_EVENT_TOPIC0);
    assert_eq!(config.start_block, 21040000);
    assert_eq!(config.end_block, Some(21049999));
    assert_eq!(config.chunk_size, 250);
    assert_eq!(config.consumer_name, "degov-vote-consumer:seamless-dao");
}

#[test]
fn test_sdk_config_uses_configured_timeout_seconds() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    set_env(
        "DEGOV_CONTRACT_ADDRESS",
        "0x1111111111111111111111111111111111111111",
    );
    set_env("DEGOV_EVENT_TOPIC0", DEFAULT_EVENT_TOPIC0);
    set_env("DEGOV_START_BLOCK", "100");
    set_env("DEGOV_END_BLOCK", "200");
    set_env("DATALENS_TIMEOUT_SECONDS", "120");

    let config = AppConfig::from_env().expect("config");

    assert_eq!(
        config.sdk_config().timeout,
        Some(std::time::Duration::from_secs(120))
    );
}

fn clear_env() {
    for name in [
        "DATALENS_ENDPOINT",
        "DATALENS_TOKEN",
        "DATALENS_APPLICATION",
        "DEGOV_DATABASE_URL",
        "DEGOV_CHAIN_NAME",
        "DEGOV_CHAIN_ID",
        "DEGOV_DATASET_FAMILY",
        "DEGOV_DATASET_NAME",
        "DEGOV_CONTRACT_ADDRESS",
        "DEGOV_EVENT_TOPIC0",
        "DEGOV_EVENT_SIGNATURE",
        "DEGOV_START_BLOCK",
        "DEGOV_END_BLOCK",
        "DEGOV_CHUNK_SIZE",
        "DEGOV_PAGE_SIZE",
        "DEGOV_RESET_CHECKPOINT",
        "DEGOV_CONSUMER_NAME",
        "DEGOV_FIXTURES_PATH",
        "DATALENS_TIMEOUT_SECONDS",
    ] {
        unsafe {
            std::env::remove_var(name);
        }
    }
}

fn set_env(name: &str, value: &str) {
    unsafe {
        std::env::set_var(name, value);
    }
}

fn temp_fixture_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "datalens-degov-{name}-{}-{}.toml",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ))
}
