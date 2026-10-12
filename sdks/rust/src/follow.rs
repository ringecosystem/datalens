use std::collections::VecDeque;
use std::time::Duration;

use datalens_core::wire::{ChainHeads, IncompleteReason, PageRequest, QueryRequest, QueryResponse};
use datalens_core::{BlockRange, ChainName, Dataset, LogSelector, QueryFinality};
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::client::{Client, HeadKind};
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowRequest {
    pub chain: ChainName,
    pub dataset: Dataset,
    pub selector: Option<LogSelector>,
    /// `durable_only` follows the safe head. The other two follow latest.
    pub finality: QueryFinality,
    pub fields: Option<Vec<String>>,
    pub start: u64,
    /// Last block to read. `None` follows the head until the stream is dropped.
    pub end: Option<u64>,
    /// Most blocks per query. Keep it at or below the application's `max_range_blocks`.
    pub chunk_blocks: u64,
    pub page_limit: Option<u32>,
    /// `wait_ms` of every query. `None` takes the server default.
    pub wait_ms: Option<u64>,
    /// Long-poll timeout of `heads/wait` once caught up.
    pub heads_wait: Duration,
    /// Pause after `pending` or `not_followed_yet` before asking again.
    pub poll_interval: Duration,
}

impl FollowRequest {
    pub fn logs(chain: ChainName, selector: LogSelector, start: u64) -> Self {
        Self::new(chain, Dataset::EvmLogs, Some(selector), start)
    }

    pub fn blocks(chain: ChainName, start: u64) -> Self {
        Self::new(chain, Dataset::EvmBlocks, None, start)
    }

    fn new(chain: ChainName, dataset: Dataset, selector: Option<LogSelector>, start: u64) -> Self {
        Self {
            chain,
            dataset,
            selector,
            finality: QueryFinality::DurableOnly,
            fields: None,
            start,
            end: None,
            chunk_blocks: 1_000,
            page_limit: None,
            wait_ms: None,
            heads_wait: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
        }
    }

    fn head_kind(&self) -> HeadKind {
        match self.finality {
            QueryFinality::DurableOnly => HeadKind::Safe,
            QueryFinality::SafeToLatest | QueryFinality::LatestOnly => HeadKind::Latest,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FollowEvent {
    /// One query with every page merged. Its rows cover `served`, a contiguous prefix of
    /// `requested`. The stream resumes at `next_start`, or after `requested.end` when null.
    Batch(QueryResponse),
    /// Everything up to `head` has been read. The stream long-polls until the head passes it.
    CaughtUp { head: u64 },
    /// The server stopped at `next_start`. The stream asks again after `retry_in`.
    Stalled {
        next_start: u64,
        reason: IncompleteReason,
        detail: Option<String>,
        retry_in: Duration,
    },
}

/// Errors come out as items. A retryable one is followed by a backoff and another try. Any
/// other ends the stream.
pub(crate) fn follow(
    client: Client,
    request: FollowRequest,
) -> BoxStream<'static, Result<FollowEvent>> {
    let follower = Follower {
        next: request.start,
        client,
        request,
        heads: None,
        caught_up: false,
        failures: 0,
        pause: None,
        events: VecDeque::new(),
        done: false,
    };
    futures::stream::unfold(follower, |mut follower| async move {
        follower.next_item().await.map(|item| (item, follower))
    })
    .boxed()
}

struct Follower {
    client: Client,
    request: FollowRequest,
    /// First block not yet read.
    next: u64,
    /// Refreshed only by `heads/wait`, since the tip only matters once `next` passes it.
    heads: Option<ChainHeads>,
    caught_up: bool,
    /// Consecutive failures and provider stalls, for the backoff.
    failures: u32,
    pause: Option<Duration>,
    events: VecDeque<FollowEvent>,
    done: bool,
}

impl Follower {
    async fn next_item(&mut self) -> Option<Result<FollowEvent>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Some(Ok(event));
            }
            if self.done {
                return None;
            }
            if let Some(pause) = self.pause.take() {
                tokio::time::sleep(pause).await;
            }
            if let Err(error) = self.step().await {
                self.failures += 1;
                if error.is_retryable() {
                    let retry = self.client.retry_config();
                    self.pause = Some(
                        error
                            .retry_after()
                            .unwrap_or_else(|| retry.backoff(self.failures)),
                    );
                } else {
                    self.done = true;
                }
                return Some(Err(error));
            }
        }
    }

    async fn step(&mut self) -> Result<()> {
        let request = &self.request;
        if request.end.is_some_and(|end| self.next > end) {
            self.done = true;
            return Ok(());
        }
        let heads = match &self.heads {
            Some(heads) => heads.clone(),
            None => {
                let heads = self.client.heads(&request.chain).await?;
                self.heads = Some(heads.clone());
                heads
            }
        };
        if request.finality == QueryFinality::LatestOnly {
            self.next = self.next.max(heads.safe + 1);
        }
        let kind = request.head_kind();
        let tip = kind.of(&heads);
        let last = request.end.map_or(tip, |end| end.min(tip));
        if self.next > last {
            if !self.caught_up {
                self.caught_up = true;
                self.events.push_back(FollowEvent::CaughtUp { head: tip });
                return Ok(());
            }
            let heads = self
                .client
                .wait_heads(&request.chain, self.next - 1, kind, request.heads_wait)
                .await?;
            self.heads = Some(heads);
            return Ok(());
        }
        self.caught_up = false;

        let end = last
            .min(self.next.saturating_add(request.chunk_blocks.max(1) - 1))
            .min(BlockRange::MAX_END);
        let range =
            BlockRange::new(self.next, end).map_err(|error| Error::Decode(error.to_string()))?;
        let response = self
            .client
            .query_all(&QueryRequest {
                chain: request.chain.clone(),
                dataset: request.dataset,
                selector: request.selector.clone(),
                range,
                finality: request.finality,
                fields: request.fields.clone(),
                page: request.page_limit.map(|limit| PageRequest {
                    limit: Some(limit),
                    cursor: None,
                }),
                wait_ms: request.wait_ms,
            })
            .await?;

        let resume = response.next_start.unwrap_or(range.end() + 1);
        if resume > self.next {
            self.failures = 0;
        }
        self.next = resume;
        let stall = response
            .incomplete_reason
            .map(|reason| (reason, response.incomplete_detail.clone()));
        if response.served.is_some() {
            self.events.push_back(FollowEvent::Batch(response));
        }
        if let Some((reason, detail)) = stall {
            let retry_in = match reason {
                IncompleteReason::Pending
                | IncompleteReason::NotFollowedYet
                | IncompleteReason::BelowSafe => self.request.poll_interval,
                IncompleteReason::ProviderError | IncompleteReason::BudgetExhausted => {
                    self.failures += 1;
                    self.client.retry_config().backoff(self.failures)
                }
            };
            self.pause = Some(retry_in);
            self.events.push_back(FollowEvent::Stalled {
                next_start: resume,
                reason,
                detail,
                retry_in,
            });
        }
        Ok(())
    }
}
