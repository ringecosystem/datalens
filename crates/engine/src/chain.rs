use std::collections::VecDeque;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use datalens_core::{ChainId, ChainName, Dataset};
use datalens_evm::{EvmClient, HeadMode};

/// Per-chain settings the engine reads. The server maps its config into this.
#[derive(Debug, Clone)]
pub struct ChainSettings {
    pub name: ChainName,
    pub id: ChainId,
    /// `evm.logs` fill grid. Jobs cover `[k * chunk_blocks, (k + 1) * chunk_blocks - 1]`,
    /// clipped to safe.
    pub chunk_blocks: NonZeroU64,
    /// `evm.blocks` fill grid. Headers cost one RPC item per block, so this grid is finer.
    /// It also caps how many headers the follower fetches in one tick.
    pub header_chunk_blocks: NonZeroU64,
    pub max_addresses_per_query: NonZeroUsize,
    pub head_mode: HeadMode,
    pub tick: Duration,
    /// Deepest fork the follower searches for. A deeper one stops follow on this chain.
    pub reorg_window_blocks: NonZeroU64,
}

impl ChainSettings {
    pub fn chunk(&self, dataset: Dataset) -> NonZeroU64 {
        match dataset {
            Dataset::EvmLogs => self.chunk_blocks,
            Dataset::EvmBlocks => self.header_chunk_blocks,
        }
    }
}

/// A configured chain and the only RPC client that may reach it.
pub struct Chain {
    pub settings: ChainSettings,
    pub evm: EvmClient,
    pub(crate) follow: Mutex<FollowState>,
}

/// What the chain leader keeps between ticks. Lost on restart, which only delays retention.
#[derive(Default)]
pub(crate) struct FollowState {
    /// `(when, safe)` samples. Unsafe rows at or below a sample older than the retention
    /// were passed by safe that long ago, so nothing will promote them any more.
    pub(crate) safe_samples: VecDeque<(Instant, u64)>,
    pub(crate) last_retention: Option<Instant>,
}

impl Chain {
    pub fn new(settings: ChainSettings, evm: EvmClient) -> Self {
        Self {
            settings,
            evm,
            follow: Mutex::default(),
        }
    }

    pub fn id(&self) -> ChainId {
        self.settings.id
    }

    pub fn name(&self) -> &ChainName {
        &self.settings.name
    }
}

#[derive(Debug, Clone)]
pub struct EngineSettings {
    /// Server cap on a query's `wait_ms`. Also the wait when `wait_ms` is omitted.
    pub max_wait: Duration,
    pub page_limit: NonZeroU32,
    /// Fill job lease. A live worker renews it every third of this, so only a dead worker's
    /// chunk is reclaimed.
    pub lease: Duration,
    /// How long a fill or a query lookup waits for a provider token before `throttled`.
    pub token_wait: Duration,
    /// How long `chain_heads` reads are reused in process.
    pub heads_ttl: Duration,
    /// How often an idle fill worker polls for jobs enqueued by other replicas.
    pub idle_poll: Duration,
    /// A `durable_only` query ending this close to safe subscribes its selector.
    pub follow_attach_blocks: u64,
    pub subscription_ttl: Duration,
    /// How long unsafe rows outlive the safe head passing them.
    pub unsafe_retention: Duration,
    /// Done and failed fill jobs older than this are deleted.
    pub job_retention: Duration,
    /// How often a chain leader runs subscription expiry and retention.
    pub retention_every: Duration,
}

impl Default for EngineSettings {
    fn default() -> Self {
        Self {
            max_wait: Duration::from_millis(15_000),
            page_limit: NonZeroU32::new(10_000).unwrap(),
            lease: Duration::from_secs(120),
            token_wait: Duration::from_secs(10),
            heads_ttl: Duration::from_millis(500),
            idle_poll: Duration::from_millis(250),
            follow_attach_blocks: 1_000,
            subscription_ttl: Duration::from_secs(900),
            unsafe_retention: Duration::from_secs(86_400),
            job_retention: Duration::from_secs(86_400),
            retention_every: Duration::from_secs(30),
        }
    }
}
