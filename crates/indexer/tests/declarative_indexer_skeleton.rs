use datalens_core::{BlockRange, ChainFamily};
use datalens_indexer::{
    CheckpointPolicy, DatalensIndexConfig, IndexPlanBuilder, IndexRunner, OutputSinkConfig,
};

#[test]
fn test_index_config_builds_plan_with_explicit_responsibilities() {
    let config = DatalensIndexConfig {
        application: "ormp-watcher".to_owned(),
        datalens_endpoint: "http://127.0.0.1:8080".to_owned(),
        chains: vec![datalens_indexer::IndexChainConfig {
            name: "ethereum-mainnet".to_owned(),
            family: ChainFamily::Evm,
            network: "1".to_owned(),
        }],
        queries: vec![datalens_indexer::IndexQueryConfig {
            name: "message-accepted".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            dataset: "evm.logs".to_owned(),
            range: BlockRange::expect_new(10, 20),
        }],
        output: OutputSinkConfig::StdoutJson,
        checkpoint: CheckpointPolicy::Disabled,
    };

    let plan = IndexPlanBuilder::new().build(&config).unwrap();

    assert_eq!(plan.application(), "ormp-watcher");
    assert_eq!(plan.queries().len(), 1);
}

#[test]
fn test_index_runner_is_constructed_from_plan_and_output_sink() {
    let plan = datalens_indexer::IndexPlan::empty("ormp-watcher");
    let runner = IndexRunner::new(plan, OutputSinkConfig::StdoutJson);

    assert_eq!(runner.plan().application(), "ormp-watcher");
    assert_eq!(runner.output(), &OutputSinkConfig::StdoutJson);
}
