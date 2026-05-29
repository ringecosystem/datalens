//! Edge API boundary for datalens.

pub mod auth;
mod chain_family;
pub mod config;
pub mod contract;
pub mod graphql;
pub mod http;
pub mod service;

pub use contract::{
    discovery::{ChainDiscovery, DatasetDiscovery, DiscoveryResponse},
    error::{ApiErrorBody, ApiErrorDetail, api_error_body, api_error_status},
    query::{
        FieldSelectionApi, QueryApiRequest, QueryApiResponse, QueryCacheApi, QueryRangeApi,
        QuerySegmentApi, QuerySelectorApi,
    },
    warmup::{
        WarmupDatasetKeyApi, WarmupRunOnceApiResponse, WarmupSelectorApiRequest,
        WarmupSubmitApiRequest, WarmupSubmitApiResponse, WarmupTaskApiResponse,
        WarmupTaskListApiResponse, WarmupTaskListQuery, WarmupTaskView,
    },
};
pub use http::router::{router, router_with_edge_config};
pub use service::{
    lifecycle::{
        LifecycleShutdown, NoopLifecycleShutdown, ServiceLifecycle, WarmupSchedulerHandle, serve,
        serve_lifecycle,
    },
    query_service::{
        NativeCacheSummary, NativeQueryResponse, QueryService, RegisteredWarmupService,
    },
    registry::QueryServiceRegistry,
};

pub(crate) use chain_family::chain_family;

pub const APPLICATION_IDENTITY_HEADER: &str = "x-datalens-application";
