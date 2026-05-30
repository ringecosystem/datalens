use datalens_chain::ChainAdapter;
use datalens_edge::config::{DatalensConfig, EdgeConfig};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata};
use tracing_subscriber::EnvFilter;

use crate::config::{load_config, validate_config};
use crate::runtime::build_service_registry;

use super::parse_bind;

#[derive(Debug, clap::Args)]
pub struct ServeCommand {
    #[arg(long, default_value = "datalens.toml")]
    pub config: String,
    #[arg(long)]
    pub enable_graphql: bool,
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
    let lifecycle = datalens_edge::ServiceLifecycle::new_with_edge_config(registry, edge);
    let runtime = tokio::runtime::Runtime::new()?;
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

pub fn serve_edge_config(config: &DatalensConfig, command: &ServeCommand) -> EdgeConfig {
    let mut edge = config.edge.clone();
    if command.enable_graphql {
        edge.graphql.enabled = true;
    }
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
