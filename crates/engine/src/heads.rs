use datalens_core::{ChainName, Error, Heads};
use datalens_provider::{Purpose, TokenWait};
use datalens_store::StoreError;
use sqlx::{Connection, PgConnection};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::chain::Chain;
use crate::engine::{QueryEngine, provider_error};

impl QueryEngine {
    /// One head RPC for `chain`, written to `chain_heads`.
    pub async fn track_heads_once(&self, chain: &ChainName) -> Result<Heads, Error> {
        let chain = self.chain(chain)?;
        self.track(&chain).await
    }

    /// Keeps `chain_heads` fresh for one chain. Replicas elect one leader per chain with
    /// `pg_try_advisory_lock(chain_id)` held on a dedicated connection, so a dead leader's
    /// lock is released with its connection.
    pub async fn run_head_tracker(&self, chain: ChainName, mut shutdown: watch::Receiver<bool>) {
        let chain = match self.chain(&chain) {
            Ok(chain) => chain,
            Err(error) => {
                warn!(%error, "head tracker has no chain");
                return;
            }
        };
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self.lead_heads(&chain, &mut shutdown).await {
                Ok(()) => return,
                Err(error) => warn!(chain = %chain.name(), %error, "head tracker lost its lock"),
            }
            tokio::select! {
                () = tokio::time::sleep(chain.settings.tick) => {}
                _ = shutdown.changed() => return,
            }
        }
    }

    /// Returns `Ok` on shutdown and `Err` when the lock connection fails.
    async fn lead_heads(
        &self,
        chain: &Chain,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), Error> {
        let tick = chain.settings.tick;
        let key = i64::from(chain.id().get());
        let mut conn: PgConnection = self
            .inner
            .store
            .pool()
            .acquire()
            .await
            .map_err(StoreError::from)?
            .detach();
        loop {
            let locked: bool = sqlx::query_scalar("select pg_try_advisory_lock($1)")
                .bind(key)
                .fetch_one(&mut conn)
                .await
                .map_err(StoreError::from)?;
            if locked {
                break;
            }
            tokio::select! {
                () = tokio::time::sleep(tick) => {}
                _ = shutdown.changed() => {
                    let _ = conn.close().await;
                    return Ok(());
                }
            }
        }
        info!(chain = %chain.name(), "head tracker leads");
        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.changed() => break,
            }
            if let Err(error) = self.track(chain).await {
                warn!(chain = %chain.name(), %error, "head refresh failed");
            }
            sqlx::query("select 1")
                .execute(&mut conn)
                .await
                .map_err(StoreError::from)?;
        }
        let _ = sqlx::query("select pg_advisory_unlock($1)")
            .bind(key)
            .execute(&mut conn)
            .await;
        let _ = conn.close().await;
        Ok(())
    }

    async fn track(&self, chain: &Chain) -> Result<Heads, Error> {
        let heads = chain
            .evm
            .heads(
                chain.settings.head_mode,
                Purpose::Head,
                None,
                TokenWait::UntilToken,
            )
            .await
            .map_err(provider_error)?;
        self.inner.store.upsert_heads(chain.id(), &heads).await?;
        Ok(heads)
    }
}
