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
