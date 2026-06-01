use std::sync::Mutex;

use alloy_primitives::keccak256;
use datalens_example_degov_client::{
    config::{AppConfig, DEFAULT_EVENT_TOPIC0},
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
