use std::error::Error;
use std::future::Future;
use std::time::Duration;

use datalens_api::{ApiConfig, Application, Quota};
use datalens_engine::{Chain, ChainSettings, EngineSettings, QueryEngine};
use datalens_evm::{EvmClient, HeadMode};
use datalens_provider::{Gateway, ProviderMetrics};
use datalens_store::Store;
use prometheus::Registry;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{Config, FinalityConfig, Role};
use crate::http::ops_router;

pub type RunError = Box<dyn Error + Send + Sync>;

const USAGE_FLUSH: Duration = Duration::from_secs(10);
const HEADER_BATCH: usize = 50;

/// Connects, migrates, and runs the configured roles until SIGINT or SIGTERM.
pub async fn serve(config: Config) -> Result<(), RunError> {
    let mut terminate = signal(SignalKind::terminate())?;
    let pool = prepare(&config).await?;
    let listener = TcpListener::bind(config.server.bind).await?;
    let shutdown = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        info!("shutdown signal received");
    };
    run(config, pool.clone(), listener, shutdown).await?;
    pool.close().await;
    info!("stopped");
    Ok(())
}

pub async fn migrate(config: Config) -> Result<(), RunError> {
    let pool = prepare(&config).await?;
    pool.close().await;
    Ok(())
}

/// Registers chains, starts the roles' tasks, and serves HTTP on `listener` until `shutdown`
/// resolves. Background tasks stop and release their fill jobs before this returns.
pub async fn run(
    config: Config,
    pool: PgPool,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), RunError> {
    for (name, chain) in &config.chains {
        datalens_store::ensure_chain(&pool, chain.chain_id, name).await?;
    }
    info!(chains = config.chains.len(), "chains registered");

    let registry = Registry::new();
    let engine = build_engine(&config, Store::new(pool.clone()), &registry)?;
    let api = config.server.roles.contains(&Role::Api);
    let worker = config.server.roles.contains(&Role::Worker);
    let (stop, stopped) = watch::channel(false);
    let mut tasks = JoinSet::new();

    let fill_workers = if worker {
        config.fill.workers.get()
    } else {
        config.fill.inline_workers
    };
    let prefix = worker_prefix();
    for n in 0..fill_workers {
        let engine = engine.clone();
        let stopped = stopped.clone();
        let id = format!("{prefix}-{n}");
        tasks.spawn(async move { engine.run_fill_worker(id, stopped).await });
    }
    if worker {
        for name in engine.chain_names() {
            let engine = engine.clone();
            let stopped = stopped.clone();
            tasks.spawn(async move { engine.run_head_tracker(name, stopped).await });
        }
    }
    tasks.spawn(flush_usage(engine.clone(), stopped.clone()));
    info!(api, worker, fill_workers, "roles started");

    let routes = if api {
        datalens_api::router(engine.clone(), registry, api_config(&config))
    } else {
        datalens_api::metrics_router(
            engine.clone(),
            registry,
            config.metrics.bearer_token.expose().to_owned(),
        )
    };
    info!(bind = %listener.local_addr()?, "listening");
    let served = axum::serve(listener, ops_router(pool).merge(routes))
        .with_graceful_shutdown(shutdown)
        .await;

    let _ = stop.send(true);
    while tasks.join_next().await.is_some() {}
    if let Err(error) = engine.usage().flush(engine.store()).await {
        warn!(%error, "final usage flush failed");
    }
    served?;
    Ok(())
}

fn build_engine(
    config: &Config,
    store: Store,
    registry: &Registry,
) -> Result<QueryEngine, RunError> {
    let provider_metrics = ProviderMetrics::register(registry)?;
    let mut chains = Vec::with_capacity(config.chains.len());
    for (name, chain) in &config.chains {
        let gateway = Gateway::new(
            name.clone(),
            datalens_provider::ProviderConfig {
                url: chain.rpc_url.expose().to_owned(),
                rps: chain.provider.rps,
                burst: chain.provider.burst,
                daily_calls: chain.provider.daily_calls,
                max_in_flight: chain.provider.max_in_flight,
                min_range_blocks: chain.provider.min_range_blocks,
                max_get_logs_range_blocks: chain.max_get_logs_range_blocks,
            },
            provider_metrics.clone(),
        )?;
        let head_mode = match chain.finality {
            FinalityConfig::Lag {
                safe_lag_blocks,
                finalized_lag_blocks,
            } => HeadMode::Lag {
                safe_lag_blocks,
                finalized_lag_blocks,
            },
            FinalityConfig::RpcTags => HeadMode::Tags,
        };
        chains.push(Chain::new(
            ChainSettings {
                name: name.clone(),
                id: chain.chain_id,
                chunk_blocks: chain.chunk_blocks,
                header_chunk_blocks: chain.header_chunk_blocks,
                max_addresses_per_query: chain.max_addresses_per_query,
                head_mode,
                tick: Duration::from_millis(chain.tick_ms.get()),
            },
            EvmClient::new(gateway, chain.max_addresses_per_query, HEADER_BATCH)?,
        ));
    }
    let settings = EngineSettings {
        max_wait: Duration::from_millis(config.query.max_wait_ms),
        page_limit: config.query.page_limit,
        lease: Duration::from_secs(config.fill.lease_s.get()),
        ..EngineSettings::default()
    };
    Ok(QueryEngine::new(store, chains, settings, registry)?)
}

fn api_config(config: &Config) -> ApiConfig {
    ApiConfig {
        applications: config
            .applications
            .iter()
            .map(|app| Application {
                id: app.id.clone(),
                token: app.token.expose().to_owned(),
                chains: app.chains.clone(),
                datasets: app.datasets.clone(),
                quota: Quota {
                    max_range_blocks: app.quota.max_range_blocks.get(),
                    max_requests_per_minute: app.quota.max_requests_per_minute.get(),
                    max_concurrent_requests: app.quota.max_concurrent_requests.get(),
                    max_fill_blocks_per_minute: app
                        .quota
                        .max_fill_blocks_per_minute
                        .map(|limit| limit.get()),
                    allow_unfiltered_logs: app.quota.allow_unfiltered_logs,
                },
                admin: app.admin,
            })
            .collect(),
        metrics_token: config.metrics.bearer_token.expose().to_owned(),
    }
}

async fn flush_usage(engine: QueryEngine, mut stopped: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(USAGE_FLUSH);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = stopped.changed() => return,
        }
        if let Err(error) = engine.usage().flush(engine.store()).await {
            warn!(%error, "usage flush failed");
        }
    }
}

/// Unique per process, so a lease names the replica that holds it.
fn worker_prefix() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "datalens".to_owned());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or_default();
    format!("{host}-{}-{nanos:08x}", std::process::id())
}

async fn prepare(config: &Config) -> Result<PgPool, RunError> {
    let pool = datalens_store::connect(
        config.database.url.expose(),
        config.database.max_connections.get(),
    )
    .await?;
    datalens_store::migrate(&pool).await?;
    info!("migrations applied");
    Ok(pool)
}
