use std::sync::Mutex;
use std::{fs, path::PathBuf};

use alloy_primitives::keccak256;
use datalens_example_ormp_client::{
    config::{AppConfig, DEFAULT_EVENT_TOPIC0, OrmpFixtureFile},
    datalens::MESSAGE_ACCEPTED_SIGNATURE,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_message_accepted_default_topic_matches_signature() {
    let topic0 = format!("{:#x}", keccak256(MESSAGE_ACCEPTED_SIGNATURE.as_bytes()));

    assert_eq!(DEFAULT_EVENT_TOPIC0, topic0);
}

#[test]
fn test_fixture_file_loads_base_live_workload() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    let path = temp_fixture_path("base-live-workload");
    fs::write(
        &path,
        r#"
[[workloads]]
name = "base"
chain_name = "base"
chain_id = 8453
contract_address = "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
start_block = 30519000
end_block = 30520999
chunk_size = 2000
"#,
    )
    .expect("write fixture config");

    let fixture = OrmpFixtureFile::from_path(&path).expect("fixture file");

    assert_eq!(fixture.workloads.len(), 1);
    assert_eq!(fixture.workloads[0].name, "base");
    assert_eq!(fixture.workloads[0].chain_name, "base");
    assert_eq!(fixture.workloads[0].chain_id, 8453);
    assert_eq!(
        fixture.workloads[0].contract_address,
        "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
    );
    assert_eq!(fixture.workloads[0].start_block, 30519000);
    assert_eq!(fixture.workloads[0].end_block, 30520999);
    assert_eq!(fixture.workloads[0].chunk_size, Some(2000));

    let _ = fs::remove_file(path);
}

#[test]
fn test_from_fixture_workload_uses_message_accepted_defaults() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    set_env("ORMP_DATABASE_URL", "sqlite:.tmp/ormp-fixtures.sqlite");

    let fixture = OrmpFixtureFile::from_toml_str(
        r#"
[[workloads]]
name = "base"
chain_name = "base"
chain_id = 8453
contract_address = "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
start_block = 30519000
end_block = 30520999
chunk_size = 2000
"#,
    )
    .expect("fixture file");

    let config = AppConfig::from_fixture_workload(&fixture.workloads[0]).expect("workload config");

    assert_eq!(config.database_url, "sqlite:.tmp/ormp-fixtures.sqlite");
    assert_eq!(config.chain_name, "base");
    assert_eq!(config.chain_id, 8453);
    assert_eq!(
        config.contract_address,
        "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
    );
    assert_eq!(config.event_topic0, DEFAULT_EVENT_TOPIC0);
    assert_eq!(config.event_signature, MESSAGE_ACCEPTED_SIGNATURE);
    assert_eq!(config.start_block, 30519000);
    assert_eq!(config.end_block, Some(30520999));
    assert_eq!(config.chunk_size, 2000);
    assert_eq!(config.consumer_name, "ormp-message-consumer:base");
}

#[test]
fn test_from_env_keeps_event_topic_override() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    clear_env();
    set_env("ORMP_EVENT_TOPIC0", "0x1234");

    let config = AppConfig::from_env().expect("env config");

    assert_eq!(config.event_topic0, "0x1234");
}

fn clear_env() {
    for name in [
        "DATALENS_ENDPOINT",
        "DATALENS_TOKEN",
        "DATALENS_APPLICATION",
        "ORMP_DATABASE_URL",
        "ORMP_CHAIN_NAME",
        "ORMP_CHAIN_ID",
        "ORMP_DATASET_FAMILY",
        "ORMP_DATASET_NAME",
        "ORMP_CONTRACT_ADDRESS",
        "ORMP_EVENT_TOPIC0",
        "ORMP_EVENT_SIGNATURE",
        "ORMP_START_BLOCK",
        "ORMP_END_BLOCK",
        "ORMP_CHUNK_SIZE",
        "ORMP_PAGE_SIZE",
        "ORMP_RESET_CHECKPOINT",
        "ORMP_CONSUMER_NAME",
        "ORMP_FIXTURES_PATH",
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
        "datalens-ormp-{name}-{}-{}.toml",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ))
}
