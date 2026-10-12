use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use datalens_core::wire::ChainHeads;
use datalens_core::{ChainId, ChainName, Error, Heads};
use datalens_provider::ProviderError;
use datalens_store::Store;
use prometheus::Registry;
use tokio::sync::Notify;

use crate::chain::{Chain, EngineSettings};
use crate::metrics::EngineMetrics;
use crate::usage::UsageBook;

/// Query planner, fill worker, and head tracker over one store. Clone shares everything.
#[derive(Clone)]
pub struct QueryEngine {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) store: Store,
    pub(crate) chains: HashMap<ChainName, Arc<Chain>>,
    pub(crate) by_id: HashMap<ChainId, Arc<Chain>>,
    pub(crate) settings: EngineSettings,
    heads: Mutex<HashMap<ChainId, (Instant, Heads)>>,
    pub(crate) usage: UsageBook,
    pub(crate) metrics: EngineMetrics,
    /// Wakes this process's idle fill workers right after a local enqueue.
    pub(crate) enqueued: Notify,
}

impl QueryEngine {
    pub fn new(
        store: Store,
        chains: Vec<Chain>,
        settings: EngineSettings,
        registry: &Registry,
    ) -> Result<Self, prometheus::Error> {
        let metrics = EngineMetrics::register(registry)?;
        let chains: Vec<Arc<Chain>> = chains.into_iter().map(Arc::new).collect();
        let by_id = chains
            .iter()
            .map(|chain| (chain.id(), Arc::clone(chain)))
            .collect();
        let chains = chains
            .into_iter()
            .map(|chain| (chain.name().clone(), chain))
            .collect();
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                chains,
                by_id,
                settings,
                heads: Mutex::new(HashMap::new()),
                usage: UsageBook::default(),
                metrics,
                enqueued: Notify::new(),
            }),
        })
    }

    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    pub fn metrics(&self) -> &EngineMetrics {
        &self.inner.metrics
    }

    pub fn usage(&self) -> &UsageBook {
        &self.inner.usage
    }

    pub fn chain_names(&self) -> Vec<ChainName> {
        let mut names: Vec<ChainName> = self.inner.chains.keys().cloned().collect();
        names.sort();
        names
    }

    pub(crate) fn chain(&self, name: &ChainName) -> Result<Arc<Chain>, Error> {
        self.inner
            .chains
            .get(name)
            .cloned()
            .ok_or_else(|| Error::InvalidInput(format!("chain {name} is not configured")))
    }

    pub(crate) fn chain_by_id(&self, id: ChainId) -> Result<Arc<Chain>, Error> {
        self.inner
            .by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("chain id {id} is not configured")))
    }

    /// `chain_heads`, reused for `heads_ttl`. Queries never ask the provider for heights.
    pub(crate) async fn heads(&self, chain: &Chain) -> Result<Heads, Error> {
        if let Some((at, heads)) = lock(&self.inner.heads).get(&chain.id()).copied()
            && at.elapsed() < self.inner.settings.heads_ttl
        {
            return Ok(heads);
        }
        let heads = self.inner.store.heads(chain.id()).await?.ok_or_else(|| {
            Error::InvalidInput(format!("chain {} has no head yet", chain.name()))
        })?;
        lock(&self.inner.heads).insert(chain.id(), (Instant::now(), heads));
        Ok(heads)
    }

    /// Heads of every configured chain that has one, read from `chain_heads`.
    pub async fn all_heads(&self) -> Result<Vec<ChainHeads>, Error> {
        let mut out = Vec::new();
        for (id, heads) in self.inner.store.all_heads().await? {
            if let Some(chain) = self.inner.by_id.get(&id) {
                out.push(ChainHeads::new(chain.name().clone(), heads));
            }
        }
        out.sort_by(|a, b| a.chain.cmp(&b.chain));
        Ok(out)
    }

    pub async fn chain_heads(&self, name: &ChainName) -> Result<ChainHeads, Error> {
        let chain = self.chain(name)?;
        let heads = self
            .inner
            .store
            .heads(chain.id())
            .await?
            .ok_or_else(|| Error::InvalidInput(format!("chain {name} has no head yet")))?;
        Ok(ChainHeads::new(name.clone(), heads))
    }

    /// Updates the `datalens_fill_jobs` gauge from the database.
    pub async fn refresh_job_gauge(&self) -> Result<(), Error> {
        for (status, count) in self.inner.store.job_counts().await? {
            self.inner.metrics.set_fill_jobs(status.as_str(), count);
        }
        Ok(())
    }
}

pub(crate) fn provider_error(error: ProviderError) -> Error {
    Error::Internal(format!("provider {}: {error}", error.kind()))
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
