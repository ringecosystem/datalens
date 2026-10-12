#[test]
fn committed_graphql_schema_matches_the_export() {
    let committed = include_str!("../../../schemas/native.graphql");
    assert_eq!(
        committed,
        datalens_api::graphql_sdl(),
        "schemas/native.graphql is stale; run `just schema`"
    );
}
