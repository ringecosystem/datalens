//! Edge API boundary for datalens.

pub mod auth;
pub mod compatibility;
pub mod config;
pub mod contract;
pub mod graphql;
pub mod http;
pub mod native;
pub mod service;
pub mod streaming;

pub use contract::{
    discovery::{ChainDiscovery, DiscoveryResponse},
    error::{ApiErrorBody, ApiErrorDetail, api_error_body, api_error_status},
    query::{
        CacheSummary, FieldSelectionApi, QueryApiRequest, QueryApiResponse, QueryCacheApi,
        QueryRangeApi, QuerySegment, QuerySegmentApi, QuerySelectorApi,
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

pub const APPLICATION_IDENTITY_HEADER: &str = "x-datalens-application";

pub(crate) fn chain_family(
    kind: &str,
) -> Result<datalens_core::ChainFamily, datalens_core::DatalensError> {
    match kind {
        "evm" => Ok(datalens_core::ChainFamily::Evm),
        value => datalens_core::ChainFamily::try_other(value.to_owned()),
    }
}

pub(crate) fn enabled_datasets(chain: &config::ChainConfig) -> Vec<datalens_core::Dataset> {
    let mut datasets = Vec::new();
    if chain.datasets.blocks.enabled {
        datasets.push(datalens_core::Dataset::Blocks);
    }
    if chain.datasets.logs.enabled {
        datasets.push(datalens_core::Dataset::Logs);
    }
    datasets
}
