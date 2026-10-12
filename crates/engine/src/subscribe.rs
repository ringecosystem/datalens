use std::time::{Duration, Instant};

use datalens_core::wire::{ChainHeads, WatchRequest, WatchResponse};
use datalens_core::{B256, ChainName, Dataset, Error};
use datalens_store::{FollowProgress, HeadKind, SelectorId, StoreError, SubscriptionSource};

use crate::admin::follow_info;
use crate::chain::Chain;
use crate::engine::{QueryEngine, lock};
use crate::query::Caller;
use crate::rows::Target;

/// An auto subscription is rewritten at most this often per process.
const RENEW_EVERY: Duration = Duration::from_secs(60);
const TOUCHED_CAP: usize = 10_000;

impl QueryEngine {
    /// Subscribes the caller to the selector and returns its follow progress, created at
    /// `safe + 1` on first sight. `None` when the application is at `max_subscriptions`.
    pub(crate) async fn attach(
        &self,
        caller: &Caller<'_>,
        chain: &Chain,
        dataset: Dataset,
        selector_id: SelectorId,
        safe: u64,
        source: SubscriptionSource,
    ) -> Result<Option<FollowProgress>, Error> {
        let store = &self.inner.store;
        let ttl = self.inner.settings.subscription_ttl;
        let key = (
            chain.id(),
            dataset,
            selector_id,
            caller.application.to_owned(),
        );
        let renew = RENEW_EVERY.min(ttl / 2);
        let fresh = source == SubscriptionSource::Auto
            && lock(&self.inner.touched)
                .get(&key)
                .is_some_and(|at| at.elapsed() < renew);
        if !fresh {
            if let Some(max) = caller.max_subscriptions {
                let live = store.application_subscriptions(caller.application).await?;
                let known = live.iter().any(|sub| {
                    sub.chain_id == chain.id()
                        && sub.dataset == dataset
                        && sub.selector_id == selector_id
                });
                if !known && live.len() >= max as usize {
                    return Ok(None);
                }
            }
            store
                .touch_subscription(
                    chain.id(),
                    dataset,
                    selector_id,
                    caller.application,
                    source,
                    ttl,
                )
                .await?;
            let mut touched = lock(&self.inner.touched);
            if touched.len() >= TOUCHED_CAP {
                touched.retain(|_, at| at.elapsed() < renew);
            }
            touched.insert(key, Instant::now());
        }
        Ok(Some(
            store
                .follow_get_or_init(chain.id(), dataset, selector_id, safe + 1, &B256::ZERO)
                .await?,
        ))
    }

    /// Explicit subscription for `subscription_ttl`. Calling again renews it.
    pub async fn watch(
        &self,
        caller: &Caller<'_>,
        request: WatchRequest,
    ) -> Result<WatchResponse, Error> {
        let chain = self.chain(&request.chain)?;
        let selector = Target::new(request.dataset, request.selector, None)?.selector();
        let heads = self.heads(&chain).await?;
        let selector_id = self
            .inner
            .store
            .upsert_selector(chain.id(), &selector)
            .await?;
        let progress = self
            .attach(
                caller,
                &chain,
                request.dataset,
                selector_id,
                heads.safe,
                SubscriptionSource::Explicit,
            )
            .await?
            .ok_or_else(|| subscription_quota(caller))?;
        Ok(WatchResponse {
            chain: request.chain,
            dataset: request.dataset,
            selector_id: selector_id.get(),
            ttl_ms: u64::try_from(self.inner.settings.subscription_ttl.as_millis())
                .unwrap_or(u64::MAX),
            follow: follow_info(&progress),
        })
    }

    /// Drops this application's subscription. Follow stops once no application holds one.
    pub async fn unwatch(&self, application: &str, request: WatchRequest) -> Result<(), Error> {
        let store = &self.inner.store;
        let chain = self.chain(&request.chain)?;
        let selector = Target::new(request.dataset, request.selector, None)?.selector();
        let selector_id = store.upsert_selector(chain.id(), &selector).await?;
        store
            .remove_subscription(chain.id(), request.dataset, selector_id, application)
            .await?;
        lock(&self.inner.touched).remove(&(
            chain.id(),
            request.dataset,
            selector_id,
            application.to_owned(),
        ));
        Ok(())
    }

    /// Heads once `kind` passes `after`, or the current heads when `timeout` runs out first.
    pub async fn wait_heads(
        &self,
        chain: &ChainName,
        after: u64,
        kind: HeadKind,
        timeout: Duration,
    ) -> Result<ChainHeads, Error> {
        let runtime = self.chain(chain)?;
        match self
            .inner
            .store
            .wait_head_change(runtime.id(), after, kind, timeout)
            .await
        {
            Ok(heads) => Ok(ChainHeads::new(chain.clone(), heads)),
            Err(StoreError::Timeout(_)) => self.chain_heads(chain).await,
            Err(error) => Err(error.into()),
        }
    }
}

pub(crate) fn subscription_quota(caller: &Caller<'_>) -> Error {
    Error::QuotaExceeded(format!(
        "application {} is at max_subscriptions {}",
        caller.application,
        caller.max_subscriptions.unwrap_or_default()
    ))
}
