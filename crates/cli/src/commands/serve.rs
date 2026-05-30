use datalens_chain::ChainAdapter;
use datalens_edge::config::{DatalensConfig, EdgeConfig};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata};
use datalens_indexer::{DatalensIndexConfig, IndexDaemon, QueryProtocol};
use tracing_subscriber::EnvFilter;

use crate::config::{application_index_config, load_config, validate_config};
use crate::runtime::build_service_registry;

use super::parse_bind;

#[derive(Debug, clap::Args)]
pub struct ServeCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,
}

pub fn serve_command(
    command: ServeCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging()?;
    log::info!("starting datalens");
    let config = load_config(&command.config)?;
    validate_config(&config)?;
    let bind = parse_bind(&config.server.bind)?;
    let registry = build_service_registry(&config)?;
    let application_index = application_index_config(&config)?;
    let index_query_router = application_index
        .as_ref()
        .filter(|_| config.query.index.graphql_enabled)
        .map(|index_config| {
            let query_config = service_index_query_config(index_config, &config);
            datalens_indexer::application_query_router_with_playground_path(
                &query_config,
                Some(&config.query.index.playground_path),
            )
        })
        .transpose()?;
    let warmup_scheduler = if config.warmup.enabled {
        Some(
            registry.start_warmup_scheduler(std::time::Duration::from_millis(
                config.warmup.scheduler_interval_ms,
            )),
        )
    } else {
        None
    };

    let adapter = EvmAdapter::new(EvmAdapterMetadata::default());
    let _capabilities = adapter.capabilities();

    log::info!("serving datalens edge on {bind}");
    let edge = serve_edge_config(&config, &command);
    let mut lifecycle = datalens_edge::ServiceLifecycle::new_with_edge_config(registry, edge);
    if let Some(router) = index_query_router {
        lifecycle = lifecycle.with_extra_router(router);
    }
    let runtime = tokio::runtime::Runtime::new()?;
    if let Some(index_config) = application_index {
        let worker_config = service_index_worker_config(index_config, &config);
        let client =
            datalens_client::DatalensClient::new(worker_config.client.to_datalens_client_config())?;
        runtime.spawn(async move {
            let daemon = IndexDaemon::new(worker_config, client);
            if let Err(error) = daemon
                .run_until_shutdown(async {
                    if let Err(error) = tokio::signal::ctrl_c().await {
                        log::error!("failed to listen for index worker shutdown signal: {error}");
                    }
                })
                .await
            {
                log::error!("application index worker stopped with error: {error}");
            }
        });
    }
    if let Some(scheduler) = warmup_scheduler {
        runtime.block_on(datalens_edge::serve_lifecycle(
            bind,
            lifecycle.with_warmup_scheduler(scheduler),
        ))?;
    } else {
        runtime.block_on(datalens_edge::serve_lifecycle(bind, lifecycle))?;
    }
    Ok(())
}

fn service_index_query_config(
    index_config: &DatalensIndexConfig,
    service_config: &DatalensConfig,
) -> DatalensIndexConfig {
    let mut index_config = index_config.clone();
    index_config.query.enabled = true;
    index_config.query.protocol = QueryProtocol::Graphql;
    index_config.query.bind = service_config.server.bind.clone();
    index_config.query.path = service_config.query.index.path.clone();
    index_config.query.playground = service_config.query.index.playground_enabled;
    index_config
}

fn service_index_worker_config(
    index_config: DatalensIndexConfig,
    service_config: &DatalensConfig,
) -> DatalensIndexConfig {
    let mut index_config = service_index_query_config(&index_config, service_config);
    index_config.query.enabled = false;
    index_config
}

pub fn serve_edge_config(config: &DatalensConfig, command: &ServeCommand) -> EdgeConfig {
    let _ = command;
    let mut edge = config.edge.clone();
    edge.query = config.query.clone();
    edge
}

fn init_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_log::LogTracer::init()?;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("datalens=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()?;
    Ok(())
}
