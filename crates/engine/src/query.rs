use std::time::{Duration, Instant};

use datalens_core::wire::{
    Anchor, IncompleteReason, PageInfo, QueryRequest, QueryResponse, Segment, SegmentSource,
};
use datalens_core::{B256, BlockRange, Dataset, Error, Finality, Heads, QueryFinality, Selector};
use datalens_store::{
    FollowProgress, HeadKind, JobId, JobPurpose, JobStatus, SelectorId, StoreError,
    SubscriptionSource, longest_covered_prefix,
};
use serde_json::Value;
use tracing::warn;

use crate::budget::FillBudget;
use crate::chain::Chain;
use crate::engine::QueryEngine;
use crate::plan::{Cursor, durable_part, durable_segments, fill_chunks, grid_span};
use crate::rows::{Page, Target};
use crate::subscribe::subscription_quota;
use crate::usage::UsageCounts;

/// Who is asking. `application` attributes jobs, usage, and subscriptions. It is never part
/// of a cache key.
pub struct Caller<'a> {
    pub application: &'a str,
    pub fill_budget: Option<&'a FillBudget>,
    /// `None` means no limit.
    pub max_subscriptions: Option<u32>,
}

type Blocker = (IncompleteReason, Option<String>);

/// How far one part of a query got, and why it stopped short.
struct Progress {
    served: Option<BlockRange>,
    blocker: Option<Blocker>,
}

impl Progress {
    fn reaches(&self, end: u64) -> bool {
        self.served.is_some_and(|served| served.end() == end)
    }
}

/// The followed tail read from unsafe rows.
struct Hot {
    progress: Progress,
    segment: Option<Segment>,
}

impl QueryEngine {
    pub async fn query(
        &self,
        caller: &Caller<'_>,
        request: QueryRequest,
    ) -> Result<QueryResponse, Error> {
        let settings = &self.inner.settings;
        let chain = self.chain(&request.chain)?;
        let target = Target::parse(&request)?;
        let page = request.page.clone().unwrap_or_default();
        let cursor = page.cursor.as_deref().map(Cursor::decode).transpose()?;
        let limit = page
            .limit
            .unwrap_or(settings.page_limit.get())
            .clamp(1, settings.page_limit.get());
        let wait = request
            .wait_ms
            .map(Duration::from_millis)
            .unwrap_or(settings.max_wait)
            .min(settings.max_wait);
        let deadline = Instant::now() + wait;
        let range = request.range;
        let finality = request.finality;

        let mut heads = self.heads(&chain).await?;
        if finality == QueryFinality::DurableOnly && range.end() > heads.safe {
            heads = self.fresh_heads(&chain).await?;
        }
        if finality == QueryFinality::DurableOnly && range.end() > heads.safe {
            return Err(Error::InvalidInput(format!(
                "block range end {} is above the safe head {}",
                range.end(),
                heads.safe
            )));
        }
        let selector = target.selector();
        let dataset = selector.dataset();
        let selector_id = self
            .inner
            .store
            .upsert_selector(chain.id(), &selector)
            .await?;
        let follow = if self.follows(finality, range, &heads) {
            let attached = self
                .attach(
                    caller,
                    &chain,
                    dataset,
                    selector_id,
                    heads.safe,
                    SubscriptionSource::Auto,
                )
                .await?;
            if attached.is_none() && finality != QueryFinality::DurableOnly {
                return Err(subscription_quota(caller));
            }
            attached
        } else {
            None
        };

        let durable = match finality {
            QueryFinality::LatestOnly => None,
            _ => durable_part(range, heads.safe),
        };
        let durable_progress = match durable {
            Some(durable) => Some(
                self.fill_durable(
                    caller,
                    &chain,
                    &selector,
                    selector_id,
                    durable,
                    &heads,
                    deadline,
                    follow.as_ref(),
                )
                .await?,
            ),
            None => None,
        };
        let durable_done = match (durable, &durable_progress) {
            (Some(durable), Some(progress)) => progress.reaches(durable.end()),
            _ => true,
        };
        let hot_range = (finality != QueryFinality::DurableOnly && range.end() > heads.safe)
            .then(|| BlockRange::new(range.start().max(heads.safe + 1), range.end()))
            .transpose()?;
        let hot = match hot_range {
            Some(wanted) if durable_done => {
                Some(self.hot(&chain, dataset, selector_id, wanted).await?)
            }
            _ => None,
        };

        let (served, blocker) = match (&durable_progress, &hot) {
            (None, None) => (None, None),
            (Some(durable), None) => (durable.served, durable.blocker.clone()),
            (None, Some(hot)) => (hot.progress.served, hot.progress.blocker.clone()),
            (Some(durable), Some(hot)) => match (durable.served, hot.progress.served) {
                (Some(low), Some(high)) => (
                    Some(BlockRange::new(low.start(), high.end())?),
                    hot.progress.blocker.clone(),
                ),
                _ => (durable.served, hot.progress.blocker.clone()),
            },
        };
        let scope_start = hot_range
            .filter(|_| finality == QueryFinality::LatestOnly)
            .map_or(range.start(), |hot| hot.start());
        let in_scope = finality != QueryFinality::LatestOnly || hot_range.is_some();
        let next_start = match served {
            None => in_scope.then_some(scope_start),
            Some(served) => (served.end() < range.end()).then(|| served.end() + 1),
        };
        let (incomplete_reason, incomplete_detail) = match (next_start, blocker) {
            (None, _) => (None, None),
            (Some(_), Some((reason, detail))) => (Some(reason), detail),
            (Some(_), None) => (Some(IncompleteReason::NotFollowedYet), None),
        };

        let mut segments = durable_progress
            .as_ref()
            .and_then(|durable| durable.served)
            .map(|served| durable_segments(served, &heads))
            .unwrap_or_default();
        if durable_done {
            segments.extend(hot.as_ref().and_then(|hot| hot.segment));
        }

        let (rows, next_cursor) = match served {
            Some(served) => {
                self.read_rows(
                    caller.application,
                    &chain,
                    &target,
                    Page {
                        served,
                        cursor,
                        limit,
                    },
                    &heads,
                )
                .await?
            }
            None => (Vec::new(), None),
        };

        self.inner.usage.record(
            caller.application,
            chain.id(),
            request.dataset,
            UsageCounts {
                queries: 1,
                rows_returned: rows.len() as i64,
                blocks_served: served.map_or(0, |served| served.len() as i64),
                ..UsageCounts::default()
            },
        );
        Ok(QueryResponse {
            chain: request.chain,
            dataset: request.dataset,
            requested: range,
            served,
            next_start,
            incomplete_reason,
            incomplete_detail,
            segments,
            rows,
            page: PageInfo { next_cursor },
        })
    }

    /// Head-tail queries subscribe their selector, and so does a `durable_only` query ending
    /// within `follow_attach_blocks` of safe, such as an indexer chasing the safe head.
    fn follows(&self, finality: QueryFinality, range: BlockRange, heads: &Heads) -> bool {
        match finality {
            QueryFinality::SafeToLatest | QueryFinality::LatestOnly => true,
            QueryFinality::DurableOnly => {
                range
                    .end()
                    .saturating_add(self.inner.settings.follow_attach_blocks)
                    >= heads.safe
            }
        }
    }

    /// Covered prefix of `durable`. Gaps below the follow region are filled by jobs. Gaps in
    /// it belong to the finalizer, so the query waits for promotion instead of fetching.
    #[allow(clippy::too_many_arguments)]
    async fn fill_durable(
        &self,
        caller: &Caller<'_>,
        chain: &Chain,
        selector: &Selector,
        selector_id: SelectorId,
        durable: BlockRange,
        heads: &Heads,
        deadline: Instant,
        follow: Option<&FollowProgress>,
    ) -> Result<Progress, Error> {
        let store = &self.inner.store;
        let fill_end = match follow {
            Some(follow) => follow
                .from_block
                .checked_sub(1)
                .map(|end| end.min(heads.safe)),
            None => Some(heads.safe),
        };
        let chunk = chain.settings.chunk(selector.dataset());
        let fill_span = fill_end
            .filter(|end| durable.start() <= *end)
            .and_then(|end| grid_span(durable, chunk, end));
        let span = match fill_span {
            Some(fill) => BlockRange::new(fill.start(), fill.end().max(durable.end()))?,
            None => durable,
        };
        let covered = self.covered(chain, selector, selector_id, span).await?;
        let served = longest_covered_prefix(&covered, durable);
        if served.is_some_and(|served| served.end() == durable.end()) {
            return Ok(Progress {
                served,
                blocker: None,
            });
        }

        let mut jobs: Vec<(JobId, BlockRange)> = Vec::new();
        let mut over_budget = false;
        let planned = match (fill_span, fill_end) {
            (Some(fill), Some(end)) => fill_chunks(fill, &covered, chunk, end),
            _ => Vec::new(),
        };
        for chunk in planned {
            // The budget pays for new fills only. A poll of a chunk already in flight joins it,
            // and once a charge is refused, later chunks are only joined, never created.
            if let Some(budget) = caller.fill_budget {
                if let Some(id) = store
                    .active_job(chain.id(), selector.dataset(), selector_id, chunk)
                    .await?
                {
                    jobs.push((id, chunk));
                    continue;
                }
                if over_budget || !budget.try_take(chunk.len()) {
                    over_budget = true;
                    continue;
                }
            }
            let id = store
                .enqueue(
                    chain.id(),
                    selector.dataset(),
                    selector_id,
                    chunk,
                    JobPurpose::Query,
                    caller.application,
                )
                .await?;
            jobs.push((id, chunk));
        }
        self.inner.enqueued.notify_waiters();

        let wait = deadline.saturating_duration_since(Instant::now());
        if !wait.is_zero() && !jobs.is_empty() {
            let ids: Vec<JobId> = jobs.iter().map(|(id, _)| *id).collect();
            match store.wait_done(&ids, wait).await {
                Ok(_) | Err(StoreError::Timeout(_)) => {}
                Err(error) => warn!(%error, "waiting for fill jobs failed"),
            }
        }

        let mut served = self.served(chain, selector, selector_id, durable).await?;
        let first_missing =
            |served: Option<BlockRange>| served.map_or(durable.start(), |served| served.end() + 1);
        if served.is_some_and(|served| served.end() == durable.end()) {
            return Ok(Progress {
                served,
                blocker: None,
            });
        }
        if fill_end.is_none_or(|end| first_missing(served) > end) {
            let mut after = heads.latest;
            loop {
                let wait = deadline.saturating_duration_since(Instant::now());
                if wait.is_zero() {
                    break;
                }
                match store
                    .wait_head_change(chain.id(), after, HeadKind::Latest, wait)
                    .await
                {
                    Ok(next) => after = next.latest,
                    Err(StoreError::Timeout(_)) => {}
                    Err(error) => {
                        warn!(%error, "waiting for promotion failed");
                        break;
                    }
                }
                served = self.served(chain, selector, selector_id, durable).await?;
                if served.is_some_and(|served| served.end() == durable.end()) {
                    return Ok(Progress {
                        served,
                        blocker: None,
                    });
                }
            }
            return Ok(Progress {
                served,
                blocker: Some((IncompleteReason::NotFollowedYet, None)),
            });
        }
        let blocker = self
            .blocker(&jobs, first_missing(served), over_budget)
            .await?;
        Ok(Progress {
            served,
            blocker: Some(blocker),
        })
    }

    async fn served(
        &self,
        chain: &Chain,
        selector: &Selector,
        selector_id: SelectorId,
        durable: BlockRange,
    ) -> Result<Option<BlockRange>, Error> {
        let covered = self.covered(chain, selector, selector_id, durable).await?;
        Ok(longest_covered_prefix(&covered, durable))
    }

    /// `wanted ∩ (safe, head_block]` from unsafe rows, anchored at the follow head.
    async fn hot(
        &self,
        chain: &Chain,
        dataset: Dataset,
        selector_id: SelectorId,
        wanted: BlockRange,
    ) -> Result<Hot, Error> {
        let follow = self
            .inner
            .store
            .follow_get_or_init(
                chain.id(),
                dataset,
                selector_id,
                wanted.start(),
                &B256::ZERO,
            )
            .await?;
        let head = u64::try_from(follow.head_block).ok();
        let Some(head) =
            head.filter(|head| *head >= wanted.start() && follow.from_block <= wanted.start())
        else {
            return Ok(Hot {
                progress: Progress {
                    served: None,
                    blocker: Some((IncompleteReason::NotFollowedYet, None)),
                },
                segment: None,
            });
        };
        let served = BlockRange::new(wanted.start(), wanted.end().min(head))?;
        Ok(Hot {
            progress: Progress {
                served: Some(served),
                blocker: (served.end() < wanted.end()).then_some((IncompleteReason::Pending, None)),
            },
            segment: Some(Segment {
                start: served.start(),
                end: served.end(),
                source: SegmentSource::Hot,
                finality: Finality::Unsafe,
                anchor: Some(Anchor {
                    number: head,
                    hash: follow.head_hash,
                }),
            }),
        })
    }

    pub(crate) async fn covered(
        &self,
        chain: &Chain,
        selector: &Selector,
        selector_id: SelectorId,
        range: BlockRange,
    ) -> Result<Vec<BlockRange>, Error> {
        let store = &self.inner.store;
        Ok(match selector {
            Selector::Logs(logs) => store.covered_for_query(chain.id(), logs, range).await?,
            Selector::Blocks => {
                store
                    .covered(chain.id(), Dataset::EvmBlocks, &[selector_id], range)
                    .await?
            }
        })
    }

    /// Why `block` is not covered yet, read from the job that owns its chunk.
    async fn blocker(
        &self,
        jobs: &[(JobId, BlockRange)],
        block: u64,
        over_budget: bool,
    ) -> Result<Blocker, Error> {
        let Some((id, _)) = jobs.iter().find(|(_, chunk)| chunk.contains(block)) else {
            let reason = if over_budget {
                IncompleteReason::BudgetExhausted
            } else {
                IncompleteReason::Pending
            };
            return Ok((reason, None));
        };
        let state = self
            .inner
            .store
            .job_status(&[*id])
            .await?
            .into_iter()
            .next();
        let kind = state
            .as_ref()
            .and_then(|state| state.last_error.as_ref())
            .and_then(|error| error.get("kind"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let reason = match (state.map(|state| state.status), kind.as_deref()) {
            (Some(JobStatus::Failed), _) => IncompleteReason::ProviderError,
            (_, Some("budget_exhausted" | "throttled")) => IncompleteReason::BudgetExhausted,
            _ => IncompleteReason::Pending,
        };
        Ok((reason, kind))
    }
}
