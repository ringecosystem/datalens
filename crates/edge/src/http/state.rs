use crate::{config, graphql::DatalensGraphqlSchema, service::registry::QueryServiceRegistry};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) registry: QueryServiceRegistry,
    pub(crate) native_graphql_schema: Option<DatalensGraphqlSchema>,
    pub(crate) edge: config::EdgeConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRoute {
    pub path: &'static str,
}
