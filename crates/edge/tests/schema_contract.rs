#[test]
fn test_native_graphql_schema_contract_is_current() {
    let expected = include_str!("../../../schemas/native.graphql");

    assert_eq!(expected, datalens_edge::native_graphql_schema_sdl());
}
