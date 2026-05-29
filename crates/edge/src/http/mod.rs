pub mod handlers;
pub mod router;

use crate::{graphql::DatalensGraphqlSchema, service::registry::QueryServiceRegistry};

pub use crate::service::lifecycle::{serve, serve_lifecycle};
pub use router::{router, router_with_api_config};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) registry: QueryServiceRegistry,
    pub(crate) graphql_schema: Option<DatalensGraphqlSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRoute {
    pub path: &'static str,
}
