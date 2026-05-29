pub mod handlers;
pub mod router;
mod state;

pub use crate::service::lifecycle::{serve, serve_lifecycle};
pub use router::{router, router_with_edge_config};
pub(crate) use state::AppState;
pub use state::HttpRoute;
