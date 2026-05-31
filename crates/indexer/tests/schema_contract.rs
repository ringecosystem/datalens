#[test]
fn test_index_graphql_schema_contract_is_current() {
    let expected = include_str!("../../../schemas/index.graphql");

    assert_eq!(
        expected,
        datalens_indexer::graphql::index_graphql_schema_sdl()
    );
}
