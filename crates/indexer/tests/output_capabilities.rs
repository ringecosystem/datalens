use datalens_indexer::{OutputConfig, OutputKind, OutputWriteMode};

#[test]
fn test_jsonl_output_capability_is_write_only_append_only() {
    let output = OutputConfig::Jsonl {
        path: ".data/indexes/ormp/events.jsonl".into(),
    };
    let capability = output.capability();

    assert_eq!(capability.kind, OutputKind::Jsonl);
    assert!(capability.supports_write);
    assert!(!capability.supports_query);
    assert!(!capability.supports_graphql);
    assert_eq!(capability.write_mode, OutputWriteMode::AppendOnly);
}
