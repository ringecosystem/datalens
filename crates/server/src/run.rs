use std::error::Error;

use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

use crate::config::{Config, Role};
use crate::http::ops_router;

pub type RunError = Box<dyn Error + Send + Sync>;

/// Connects, migrates, registers chains, and serves until SIGINT or SIGTERM.
pub async fn serve(config: Config) -> Result<(), RunError> {
    let mut terminate = signal(SignalKind::terminate())?;
    let pool = prepare(&config).await?;
    for (name, chain) in &config.chains {
        datalens_store::ensure_chain(&pool, chain.chain_id, name).await?;
    }
    info!(chains = config.chains.len(), "chains registered");

    for role in &config.server.roles {
        match role {
            Role::Api => info!("api role enabled"),
            Role::Worker => info!("worker role enabled; no worker tasks yet"),
        }
    }

    let listener = TcpListener::bind(config.server.bind).await?;
    info!(bind = %listener.local_addr()?, "listening");
    axum::serve(listener, ops_router(pool.clone()))
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            info!("shutdown signal received");
        })
        .await?;
    pool.close().await;
    info!("stopped");
    Ok(())
}

pub async fn migrate(config: Config) -> Result<(), RunError> {
    let pool = prepare(&config).await?;
    pool.close().await;
    Ok(())
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
