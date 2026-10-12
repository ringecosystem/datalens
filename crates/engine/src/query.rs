use std::time::Duration;

use datalens_core::wire::{IncompleteReason, PageInfo, QueryRequest, QueryResponse};
use datalens_core::{BlockRange, Dataset, Error, Heads, QueryFinality, Selector};
use datalens_store::{
    JobId, JobPurpose, JobStatus, SelectorId, StoreError, longest_covered_prefix,
};
use serde_json::Value;
use tracing::warn;

use crate::budget::FillBudget;
use crate::chain::Chain;
use crate::engine::QueryEngine;
use crate::plan::{Cursor, durable_part, durable_segments, fill_chunks, grid_span};
use crate::rows::{Page, Target};
use crate::usage::UsageCounts;

/// Who is asking. `application` attributes jobs and usage. It is never part of a cache key.
pub struct Caller<'a> {
    pub application: &'a str,
    pub fill_budget: Option<&'a FillBudget>,
}

/// How far the durable path got, and why it stopped short.
struct Progress {
    served: Option<BlockRange>,
    blocker: Option<(IncompleteReason, Option<String>)>,
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
        let range = request.range;

        let heads = self.heads(&chain).await?;
        if request.finality == QueryFinality::DurableOnly && range.end() > heads.safe {
            return Err(Error::InvalidInput(format!(
                "block range end {} is above the safe head {}",
                range.end(),
                heads.safe
            )));
        }
        let selector = target.selector();
        let selector_id = self
            .inner
            .store
            .upsert_selector(chain.id(), &selector)
            .await?;
        let progress = match durable_part(range, heads.safe) {
            Some(durable) => {
                self.fill_durable(
                    caller,
                    &chain,
                    &selector,
                    selector_id,
                    durable,
                    &heads,
                    wait,
                )
                .await?
            }
            None => Progress {
                served: None,
                blocker: None,
            },
        };

        let next_start = match progress.served {
            None => Some(range.start()),
            Some(served) => (served.end() < range.end()).then(|| served.end() + 1),
        };
        let (incomplete_reason, incomplete_detail) = match (next_start, progress.blocker) {
            (None, _) => (None, None),
            (Some(_), Some((reason, detail))) => (Some(reason), detail),
            (Some(_), None) => (Some(IncompleteReason::NotFollowedYet), None),
        };
        let (rows, next_cursor) = match progress.served {
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
                blocks_served: progress.served.map_or(0, |served| served.len() as i64),
                ..UsageCounts::default()
            },
        );
        Ok(QueryResponse {
            chain: request.chain,
            dataset: request.dataset,
            requested: range,
            served: progress.served,
            next_start,
            incomplete_reason,
            incomplete_detail,
            segments: progress
                .served
                .map(|served| durable_segments(served, &heads))
                .unwrap_or_default(),
            rows,
            page: PageInfo { next_cursor },
        })
    }

    /// Covered prefix of `durable`, after enqueuing and waiting for fills of its gaps.
    #[allow(clippy::too_many_arguments)]
    async fn fill_durable(
        &self,
        caller: &Caller<'_>,
        chain: &Chain,
        selector: &Selector,
        selector_id: SelectorId,
        durable: BlockRange,
        heads: &Heads,
        wait: Duration,
    ) -> Result<Progress, Error> {
        let store = &self.inner.store;
        let chunk = chain.settings.chunk(selector.dataset());
        let span = grid_span(durable, chunk, heads.safe).unwrap_or(durable);
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
        for chunk in fill_chunks(span, &covered, chunk, heads.safe) {
            if caller
                .fill_budget
                .is_some_and(|budget| !budget.try_take(chunk.len()))
            {
                over_budget = true;
                break;
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

        if !wait.is_zero() && !jobs.is_empty() {
            let ids: Vec<JobId> = jobs.iter().map(|(id, _)| *id).collect();
            match store.wait_done(&ids, wait).await {
                Ok(_) | Err(StoreError::Timeout(_)) => {}
                Err(error) => warn!(%error, "waiting for fill jobs failed"),
            }
        }

        let covered = self.covered(chain, selector, selector_id, durable).await?;
        let served = longest_covered_prefix(&covered, durable);
        if served.is_some_and(|served| served.end() == durable.end()) {
            return Ok(Progress {
                served,
                blocker: None,
            });
        }
        let first_missing = served.map_or(durable.start(), |served| served.end() + 1);
        let blocker = self.blocker(&jobs, first_missing, over_budget).await?;
        Ok(Progress {
            served,
            blocker: Some(blocker),
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
    ) -> Result<(IncompleteReason, Option<String>), Error> {
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
