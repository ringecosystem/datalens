use datalens_indexer::{DatalensIndexConfig, IndexPlanBuilder, IndexRunner, OutputSinkConfig};

#[test]
fn test_index_config_builds_plan_without_executing_tasks() {
    let config = DatalensIndexConfig::from_toml_str(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp-watcher"
token_env = "PATH"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 1000

[[sources]]
chain = "ethereum-mainnet"
family = "evm"
chain_id = 1
from_block = 10
to_block = 20
addresses = []
topics = []

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#,
    )
    .expect("valid config");

    let plan = IndexPlanBuilder::new().build(&config).unwrap();

    assert_eq!(plan.application(), "ormp-watcher");
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].label, "ormp.000.000000");
}

#[test]
fn test_index_runner_is_constructed_from_plan_and_output_sink() {
    let plan = datalens_indexer::IndexPlan::empty("ormp-watcher");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson);

    assert_eq!(runner.plan().application(), "ormp-watcher");
    assert_eq!(runner.output(), &OutputSinkConfig::StdoutJson);
}
