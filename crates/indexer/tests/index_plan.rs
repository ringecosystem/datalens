use datalens_indexer::{DatalensIndexConfig, IndexPlanBuilder};

fn config_with_sources(chunk_blocks: u64, sources: &str) -> DatalensIndexConfig {
    DatalensIndexConfig::from_toml_str(&format!(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp"
token_env = "PATH"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = {chunk_blocks}

{sources}

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#
    ))
    .expect("valid config")
}

fn build_plan(config: &DatalensIndexConfig) -> datalens_indexer::IndexPlan {
    IndexPlanBuilder::new().build(config).expect("plan builds")
}

#[test]
fn test_plan_single_source_splits_inclusive_ranges() {
    let config = config_with_sources(
        3,
        r#"
[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 16
addresses = ["0x0000000000000000000000000000000000000001"]
topics = ["0x0000000000000000000000000000000000000000000000000000000000000000"]
"#,
    );

    let plan = build_plan(&config);

    assert_eq!(plan.application(), "ormp");
    assert_eq!(plan.tasks().len(), 3);
    assert_eq!(plan.tasks()[0].label, "ormp.000.000000");
    assert_eq!(plan.tasks()[0].source_identity, "evm:ethereum:1:000");
    assert_eq!(plan.tasks()[0].range.start, 10);
    assert_eq!(plan.tasks()[0].range.end, 12);
    assert_eq!(plan.tasks()[1].range.start, 13);
    assert_eq!(plan.tasks()[1].range.end, 15);
    assert_eq!(plan.tasks()[2].range.start, 16);
    assert_eq!(plan.tasks()[2].range.end, 16);
    assert_eq!(plan.tasks()[0].selector.address_count, 1);
    assert_eq!(plan.tasks()[0].selector.topic_count, 1);
}

#[test]
fn test_plan_multi_source_orders_by_source_identity_then_range() {
    let config = config_with_sources(
        2,
        r#"
[[sources]]
chain = "polygon"
family = "evm"
chain_id = 137
from_block = 5
to_block = 6
addresses = []
topics = []

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 1
to_block = 3
addresses = []
topics = []
"#,
    );

    let plan = build_plan(&config);
    let labels = plan
        .tasks()
        .iter()
        .map(|task| {
            (
                task.label.as_str(),
                task.chain.as_str(),
                task.range.start,
                task.range.end,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        labels,
        vec![
            ("ormp.000.000000", "ethereum", 1, 2),
            ("ormp.000.000001", "ethereum", 3, 3),
            ("ormp.001.000000", "polygon", 5, 6),
        ]
    );
}

#[test]
fn test_plan_exact_chunk_boundary_has_no_empty_trailing_task() {
    let config = config_with_sources(
        3,
        r#"
[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 15
addresses = []
topics = []
"#,
    );

    let plan = build_plan(&config);
    let ranges = plan
        .tasks()
        .iter()
        .map(|task| (task.range.start, task.range.end))
        .collect::<Vec<_>>();

    assert_eq!(ranges, vec![(10, 12), (13, 15)]);
}

#[test]
fn test_plan_rejects_missing_upper_bound() {
    let config = config_with_sources(
        3,
        r#"
[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
addresses = []
topics = []
"#,
    );

    let error = IndexPlanBuilder::new()
        .build(&config)
        .expect_err("missing to_block should fail")
        .to_string();

    assert!(error.contains("sources[0].to_block"), "{error}");
    assert!(error.contains("required for index plan"), "{error}");
}

#[test]
fn test_plan_serializes_stable_json_without_selector_values() {
    let config = config_with_sources(
        2,
        r#"
[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 11
addresses = ["0x0000000000000000000000000000000000000001"]
topics = ["0x0000000000000000000000000000000000000000000000000000000000000000"]
"#,
    );

    let plan = build_plan(&config);
    let json = serde_json::to_value(&plan).expect("plan serializes");

    assert_eq!(json["application"], "ormp");
    assert_eq!(json["tasks"][0]["dataset"], "evm.logs");
    assert_eq!(
        json["tasks"][0]["range"],
        serde_json::json!({
            "kind": "block",
            "start": 10,
            "end": 11,
        })
    );
    assert_eq!(
        json["tasks"][0]["selector"],
        serde_json::json!({
            "kind": "evm_logs",
            "address_count": 1,
            "topic_count": 1,
        })
    );
    assert!(
        !json
            .to_string()
            .contains("0000000000000000000000000000000000000001")
    );
}

#[test]
fn test_plan_solana_and_tron_sources_use_stable_family_dataset_and_range_keys() {
    let config = DatalensIndexConfig::from_toml_str(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "multi-chain"
token_env = "PATH"

[index]
name = "activity"
dataset = "solana.transactions"
finality = "durable"
chunk_blocks = 2

[[sources]]
chain = "tron-mainnet"
family = "tron"
chain_id = 728126428
dataset = "tron.events"
from_block = 9
to_block = 10
contracts = ["0x0000000000000000000000000000000000000001"]
events = ["Transfer"]

[[sources]]
chain = "solana-mainnet"
family = "solana"
network_id = "mainnet-beta"
dataset = "solana.transactions"
from_slot = 5
to_slot = 8
selector = { kind = "program", value = "11111111111111111111111111111111" }

[output.jsonl]
path = ".data/indexes/activity/events.jsonl"

[checkpoint]
path = ".data/indexes/activity/checkpoint.json"
"#,
    )
    .expect("valid config");

    let plan = build_plan(&config);
    let json = serde_json::to_value(&plan).expect("plan serializes");

    assert_eq!(plan.tasks().len(), 3);
    assert_eq!(plan.tasks()[0].family, "solana");
    assert_eq!(
        plan.tasks()[0].source_identity,
        "solana:solana-mainnet:mainnet-beta:000"
    );
    assert_eq!(plan.tasks()[0].dataset, "solana.transactions");
    assert_eq!(plan.tasks()[0].range.kind, "slot");
    assert_eq!(plan.tasks()[0].selector.kind, "solana_program");
    assert_eq!(plan.tasks()[2].family, "tron");
    assert_eq!(
        plan.tasks()[2].source_identity,
        "tron:tron-mainnet:728126428:001"
    );
    assert_eq!(plan.tasks()[2].dataset, "tron.events");
    assert_eq!(plan.tasks()[2].range.kind, "block");
    assert_eq!(plan.tasks()[2].selector.kind, "tron_events");
    assert_eq!(
        json["tasks"][0]["selector"],
        serde_json::json!({
            "kind": "solana_program",
            "fingerprint": "solana-program/26da562a2f106128",
            "value_count": 1,
        })
    );
    assert_eq!(
        json["tasks"][2]["selector"],
        serde_json::json!({
            "kind": "tron_events",
            "fingerprint": "tron-events/06719eaec45e49530c8b3b56",
            "value_count": 2,
        })
    );
}
