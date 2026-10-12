use std::time::Instant;

use chrono::Utc;
use datalens_core::{Error, Heads};

use crate::chain::Chain;
use crate::engine::{QueryEngine, lock};

impl QueryEngine {
    /// Expires subscriptions, stops follow for selectors nobody watches, deletes unsafe rows
    /// safe passed more than `unsafe_retention` ago, and prunes old jobs. Runs at most once per
    /// `retention_every`. Every statement is idempotent, so leaders of different chains may
    /// overlap on the global parts.
    pub(crate) async fn retain(&self, chain: &Chain, heads: &Heads) -> Result<(), Error> {
        let settings = &self.inner.settings;
        let store = &self.inner.store;
        let now = Instant::now();
        let passed = {
            let mut state = lock(&chain.follow);
            if state
                .last_retention
                .is_some_and(|at| now.duration_since(at) < settings.retention_every)
            {
                return Ok(());
            }
            state.last_retention = Some(now);
            state.safe_samples.push_back((now, heads.safe));
            let old = |at: &Instant| now.duration_since(*at) >= settings.unsafe_retention;
            while state.safe_samples.get(1).is_some_and(|(at, _)| old(at)) {
                state.safe_samples.pop_front();
            }
            state
                .safe_samples
                .front()
                .filter(|(at, _)| old(at))
                .map(|(_, safe)| *safe)
        };

        store.expire_subscriptions(Utc::now()).await?;
        store.delete_unwatched_follow(chain.id()).await?;
        if let Some(passed) = passed {
            // A live follow still needs its unpromoted rows, however old.
            let through = match store
                .list_follow(chain.id())
                .await?
                .iter()
                .map(|row| row.promoted_to)
                .min()
            {
                None => Some(passed),
                Some(promoted) => u64::try_from(promoted).ok().map(|p| p.min(passed)),
            };
            if let Some(through) = through {
                store.delete_unsafe_through(chain.id(), through).await?;
            }
        }
        store.prune_done(settings.job_retention).await?;
        store.prune_failed(settings.job_retention).await?;
        Ok(())
    }
}
