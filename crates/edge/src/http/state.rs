use crate::{graphql::DatalensGraphqlSchema, service::registry::QueryServiceRegistry};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) registry: QueryServiceRegistry,
    pub(crate) graphql_schema: Option<DatalensGraphqlSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRoute {
    pub path: &'static str,
}
