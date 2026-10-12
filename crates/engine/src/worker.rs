use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use datalens_core::{
    BlockRange, BlockRow, Dataset, Error, Finality, LogRow, LogSelector, Selector,
};
use datalens_provider::{ProviderError, Purpose, TokenWait};
use datalens_store::{Job, JobId, JobPurpose, SelectorId, StoreError};
use serde_json::json;
use tokio::sync::watch;
use tracing::warn;

use crate::chain::Chain;
use crate::engine::QueryEngine;
use crate::plan::{fill_finality, uncovered};
use crate::usage::UsageCounts;

const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// An internal error that repeats this often is deterministic, so the job fails and its
/// waiters see `provider_error` instead of `pending` forever.
const INTERNAL_ATTEMPTS: i32 = 3;

struct Failure {
    label: &'static str,
    body: serde_json::Value,
    retryable: bool,
    retry_after: Duration,
}

enum FillError {
    Provider(ProviderError),
    /// Stored rows at `block` carry another hash. Nothing is committed, and a later attempt
    /// runs after the reorg path has replaced them.
    Reorg {
        block: u64,
    },
    Internal(Error),
}

impl From<StoreError> for FillError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Reorg { block } => Self::Reorg { block },
            other => Self::Internal(other.into()),
        }
    }
}

impl From<Error> for FillError {
    fn from(error: Error) -> Self {
        Self::Internal(error)
    }
}

/// Batch members that lack the same parts of the cell.
struct Gaps {
    missing: Vec<BlockRange>,
    members: Vec<(SelectorId, Selector)>,
}

/// One provider fetch for a group of members over one gap.
struct Fetch {
    members: Vec<SelectorId>,
    /// The fetched prefix of the gap. `None` when nothing was fetched.
    blocks: Option<BlockRange>,
    calls: i64,
}

impl Fetch {
    fn held_members(&self, held: &HashSet<SelectorId>) -> Vec<SelectorId> {
        self.members
            .iter()
            .filter(|id| held.contains(id))
            .copied()
            .collect()
    }
}

#[derive(Default)]
struct Fetched {
    logs: Vec<LogRow>,
    blocks: Vec<BlockRow>,
    fetches: Vec<Fetch>,
    /// Why fetching stopped early. The prefixes in `fetches` are committed regardless.
    error: Option<FillError>,
}

impl QueryEngine {
    /// Claims one job plus its pending siblings in the same chunk and fills them.
    /// Returns `false` when nothing was ready.
    pub async fn fill_once(&self, worker_id: &str) -> Result<bool, Error> {
        let Some(jobs) = self.claim_batch(worker_id).await? else {
            return Ok(false);
        };
        self.execute(worker_id, jobs).await;
        Ok(true)
    }

    /// Fills jobs until `shutdown` turns true. A batch cut short by shutdown is released
    /// for another worker instead of waiting out its lease.
    pub async fn run_fill_worker(&self, worker_id: String, mut shutdown: watch::Receiver<bool>) {
        let idle = self.inner.settings.idle_poll;
        loop {
            if *shutdown.borrow() {
                return;
            }
            let notified = self.inner.enqueued.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.claim_batch(&worker_id).await {
                Ok(Some(jobs)) => {
                    let ids: Vec<JobId> = jobs.iter().map(|job| job.id).collect();
                    tokio::select! {
                        () = self.execute(&worker_id, jobs) => {}
                        _ = shutdown.changed() => {
                            self.release(&worker_id, &ids).await;
                            return;
                        }
                    }
                }
                Ok(None) => {
                    tokio::select! {
                        () = &mut notified => {}
                        () = tokio::time::sleep(idle) => {}
                        _ = shutdown.changed() => return,
                    }
                }
                Err(error) => {
                    warn!(%error, "claiming fill jobs failed");
                    tokio::select! {
                        () = tokio::time::sleep(idle) => {}
                        _ = shutdown.changed() => return,
                    }
                }
            }
        }
    }

    async fn claim_batch(&self, worker_id: &str) -> Result<Option<Vec<Job>>, Error> {
        let store = &self.inner.store;
        let lease = self.inner.settings.lease;
        let Some(leader) = store.claim(worker_id, lease, 1, None).await?.pop() else {
            return Ok(None);
        };
        // An unconfigured chain claims no siblings. Its leader still runs, so the failure is
        // recorded on the job instead of leaving it running until the lease expires.
        let siblings = match (leader.dataset, self.chain_by_id(leader.chain_id)) {
            (Dataset::EvmLogs, Ok(chain)) => {
                let max = chain.settings.max_addresses_per_query.get();
                let limit = u32::try_from(max.saturating_sub(1)).unwrap_or(u32::MAX);
                store.claim_batch_siblings(&leader, limit).await?
            }
            _ => Vec::new(),
        };
        let mut jobs = vec![leader];
        jobs.extend(siblings);
        Ok(Some(jobs))
    }

    async fn execute(&self, worker_id: &str, jobs: Vec<Job>) {
        let started = Instant::now();
        let leader = jobs[0].clone();
        let ids: Vec<JobId> = jobs.iter().map(|job| job.id).collect();
        let outcome = tokio::select! {
            outcome = self.fill(worker_id, &jobs) => outcome,
            () = self.renew_leases(worker_id, &ids) => {
                warn!(worker_id, jobs = ?ids, "fill lost every lease and stopped");
                return;
            }
        };
        let chain = self
            .chain_by_id(leader.chain_id)
            .map(|chain| chain.name().to_string())
            .unwrap_or_else(|_| leader.chain_id.to_string());
        let failure = match &outcome {
            Ok(()) => None,
            Err(FillError::Provider(error)) => Some(Failure {
                label: error.kind(),
                body: json!({"kind": error.kind(), "message": error.to_string()}),
                retryable: matches!(
                    error,
                    ProviderError::NotReady
                        | ProviderError::RateLimited { .. }
                        | ProviderError::Transient { .. }
                        | ProviderError::BudgetExhausted
                        | ProviderError::Throttled
                ),
                retry_after: match error {
                    ProviderError::RateLimited { retry_after } => *retry_after,
                    _ => Duration::ZERO,
                },
            }),
            Err(FillError::Reorg { block }) => Some(Failure {
                label: "reorg",
                body: json!({"kind": "reorg", "message": format!("block hash mismatch at block {block}")}),
                retryable: true,
                retry_after: Duration::ZERO,
            }),
            Err(FillError::Internal(error)) => Some(Failure {
                label: "internal",
                body: json!({"kind": "internal", "message": error.to_string()}),
                retryable: leader.attempts < INTERNAL_ATTEMPTS,
                retry_after: Duration::ZERO,
            }),
        };
        self.inner.metrics.observe_fill(
            &chain,
            leader.dataset.as_str(),
            failure.as_ref().map_or("ok", |failure| failure.label),
            started.elapsed().as_secs_f64(),
        );
        let Some(failure) = failure else {
            return;
        };
        warn!(chain, error = %failure.body, retryable = failure.retryable, "fill failed");
        for job in &jobs {
            let backoff = backoff(job.attempts)
                .max(failure.retry_after)
                .min(MAX_BACKOFF);
            if let Err(error) = self
                .inner
                .store
                .fail_job(job.id, worker_id, &failure.body, failure.retryable, backoff)
                .await
            {
                warn!(%error, job = %job.id, "recording fill failure failed");
            }
        }
    }

    /// Extends the batch's leases every third of a lease. Returns only once no job is held.
    async fn renew_leases(&self, worker_id: &str, ids: &[JobId]) {
        let lease = self.inner.settings.lease;
        let mut held = ids.to_vec();
        loop {
            tokio::time::sleep(lease / 3).await;
            match self.inner.store.extend_lease(worker_id, &held, lease).await {
                Ok(kept) if kept.is_empty() => return,
                Ok(kept) => held = kept,
                Err(error) => warn!(%error, worker_id, "renewing fill leases failed"),
            }
        }
    }

    /// Fetches what each member's coverage lacks inside the cell, then commits rows, coverage
    /// of every fetched prefix, and (when nothing failed) completion, for still-held jobs only.
    async fn fill(&self, worker_id: &str, jobs: &[Job]) -> Result<(), FillError> {
        let store = &self.inner.store;
        let leader = &jobs[0];
        let chain = self.chain_by_id(leader.chain_id)?;
        let heads = self.heads(&chain).await?;
        let finality = fill_finality(leader.blocks, &heads);
        let ids: Vec<_> = jobs.iter().map(|job| job.selector_id).collect();
        let mut members = store.selectors_by_id(&ids).await?;
        members.sort_by_key(|(id, _)| *id);
        // Members with the same uncovered parts share fetches. A retry after a partial failure
        // only asks for what the committed prefix left, and a job whose cell another fill
        // already covered completes without spending RPC.
        let mut gaps: Vec<Gaps> = Vec::new();
        for (selector_id, selector) in members {
            let covered = self
                .covered(&chain, &selector, selector_id, leader.blocks)
                .await?;
            let missing = uncovered(leader.blocks, &covered);
            if missing.is_empty() {
                continue;
            }
            match gaps.iter_mut().find(|gaps| gaps.missing == missing) {
                Some(gaps) => gaps.members.push((selector_id, selector)),
                None => gaps.push(Gaps {
                    missing,
                    members: vec![(selector_id, selector)],
                }),
            }
        }

        let fetched = match leader.dataset {
            Dataset::EvmLogs => self.fetch_logs(&chain, leader, gaps, finality).await,
            Dataset::EvmBlocks => self.fetch_blocks(&chain, leader, gaps, finality).await,
        };

        // A fresh lease outlasts the short write transaction, so a job held here cannot be
        // reclaimed before the commit.
        let job_ids: Vec<JobId> = jobs.iter().map(|job| job.id).collect();
        let held = store
            .extend_lease(worker_id, &job_ids, self.inner.settings.lease)
            .await?;
        if held.len() < job_ids.len() {
            warn!(
                worker_id,
                held = held.len(),
                claimed = job_ids.len(),
                "another worker reclaimed part of the batch"
            );
        }
        let held_selectors: HashSet<SelectorId> = jobs
            .iter()
            .filter(|job| held.contains(&job.id))
            .map(|job| job.selector_id)
            .collect();

        let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
        store
            .insert_logs(&mut tx, chain.id(), &fetched.logs)
            .await?;
        store
            .insert_blocks(&mut tx, chain.id(), &fetched.blocks)
            .await?;
        for fetch in &fetched.fetches {
            let Some(blocks) = fetch.blocks else {
                continue;
            };
            let ids = fetch.held_members(&held_selectors);
            if ids.is_empty() {
                continue;
            }
            store
                .add_coverage_many(&mut tx, chain.id(), leader.dataset, &ids, blocks, finality)
                .await?;
        }
        if fetched.error.is_none() {
            store.complete_jobs(&mut tx, worker_id, &held).await?;
        }
        tx.commit().await.map_err(StoreError::from)?;

        self.record_fill_usage(&chain, jobs, &fetched.fetches, &held_selectors);
        fetched.error.map_or(Ok(()), Err)
    }

    /// Each fetch's calls are split across its held members. Blocks count what was fetched.
    fn record_fill_usage(
        &self,
        chain: &Chain,
        jobs: &[Job],
        fetches: &[Fetch],
        held: &HashSet<SelectorId>,
    ) {
        let mut counts: HashMap<SelectorId, UsageCounts> = HashMap::new();
        for fetch in fetches {
            let members = fetch.held_members(held);
            let n = members.len() as i64;
            for (index, id) in members.into_iter().enumerate() {
                let entry = counts.entry(id).or_default();
                entry.provider_calls +=
                    fetch.calls / n + i64::from((index as i64) < fetch.calls % n);
                entry.blocks_filled += fetch.blocks.map_or(0, |blocks| blocks.len() as i64);
            }
        }
        for job in jobs {
            if let Some(add) = counts.remove(&job.selector_id) {
                self.inner
                    .usage
                    .record(&job.application_id, chain.id(), job.dataset, add);
            }
        }
    }

    /// One `get_logs` per batch group and gap, re-filtered so each row matches some member.
    /// Stops at the first failure, keeping the prefix that call fetched.
    async fn fetch_logs(
        &self,
        chain: &Chain,
        leader: &Job,
        gaps: Vec<Gaps>,
        finality: Finality,
    ) -> Fetched {
        let max = chain.settings.max_addresses_per_query.get();
        let mut rows: BTreeMap<(u64, u32), LogRow> = BTreeMap::new();
        let mut fetched = Fetched::default();
        'gaps: for Gaps { missing, members } in gaps {
            let members: Vec<(SelectorId, LogSelector)> = members
                .into_iter()
                .filter_map(|(id, selector)| match selector {
                    Selector::Logs(logs) => Some((id, logs)),
                    Selector::Blocks => None,
                })
                .collect();
            for group in batch_groups(&members, max) {
                let selectors: Vec<LogSelector> =
                    group.iter().map(|(_, selector)| selector.clone()).collect();
                let query =
                    LogSelector::batch(&selectors, max).unwrap_or_else(|| selectors[0].clone());
                for gap in &missing {
                    let (page, error) = match chain
                        .evm
                        .get_logs(
                            &query,
                            *gap,
                            purpose(leader),
                            Some(leader.application_id.as_str()),
                            TokenWait::For(self.inner.settings.token_wait),
                            finality,
                        )
                        .await
                    {
                        Ok(page) => (page, None),
                        Err(failure) => (failure.page, Some(failure.error)),
                    };
                    for row in page.rows {
                        if selectors.iter().any(|member| member.matches(&row)) {
                            rows.insert((row.block_number, row.log_index), row);
                        }
                    }
                    fetched.fetches.push(Fetch {
                        members: group.iter().map(|(id, _)| *id).collect(),
                        blocks: page
                            .fetched_to
                            .and_then(|end| BlockRange::new(gap.start(), end).ok()),
                        calls: 1,
                    });
                    if let Some(error) = error {
                        fetched.error = Some(FillError::Provider(error));
                        break 'gaps;
                    }
                }
            }
        }
        fetched.logs = rows.into_values().collect();
        fetched
    }

    /// `evm.blocks` has one selector, so `gaps` holds at most one entry.
    async fn fetch_blocks(
        &self,
        chain: &Chain,
        leader: &Job,
        gaps: Vec<Gaps>,
        finality: Finality,
    ) -> Fetched {
        let mut fetched = Fetched::default();
        for Gaps { missing, members } in gaps {
            for gap in missing {
                let result = chain
                    .evm
                    .block_headers(
                        gap,
                        purpose(leader),
                        Some(leader.application_id.as_str()),
                        TokenWait::For(self.inner.settings.token_wait),
                        finality,
                    )
                    .await;
                let blocks = match result {
                    Ok(blocks) => {
                        fetched.blocks.extend(blocks);
                        Some(gap)
                    }
                    Err(error) => {
                        fetched.error = Some(FillError::Provider(error));
                        None
                    }
                };
                fetched.fetches.push(Fetch {
                    members: members.iter().map(|(id, _)| *id).collect(),
                    blocks,
                    calls: 1,
                });
                if fetched.error.is_some() {
                    return fetched;
                }
            }
        }
        fetched
    }

    async fn release(&self, worker_id: &str, ids: &[JobId]) {
        let body = json!({"kind": "shutdown", "message": "worker stopped"});
        for id in ids {
            if let Err(error) = self
                .inner
                .store
                .fail_job(*id, worker_id, &body, true, Duration::ZERO)
                .await
            {
                warn!(%error, job = %id, "releasing fill job failed");
            }
        }
    }
}

fn purpose(job: &Job) -> Purpose {
    match job.purpose {
        JobPurpose::Query => Purpose::QueryFill,
        JobPurpose::Backfill => Purpose::Backfill,
    }
}

/// Greedy groups that each fit one batched `eth_getLogs`.
fn batch_groups(
    members: &[(SelectorId, LogSelector)],
    max_addresses: usize,
) -> Vec<Vec<(SelectorId, LogSelector)>> {
    let mut groups: Vec<Vec<(SelectorId, LogSelector)>> = Vec::new();
    for member in members {
        if let Some(group) = groups.last_mut() {
            group.push(member.clone());
            let selectors: Vec<LogSelector> =
                group.iter().map(|(_, selector)| selector.clone()).collect();
            if LogSelector::batch(&selectors, max_addresses).is_some() {
                continue;
            }
            group.pop();
        }
        groups.push(vec![member.clone()]);
    }
    groups
}

fn backoff(attempts: i32) -> Duration {
    let exponent = attempts.saturating_sub(1).clamp(0, 9) as u32;
    Duration::from_secs(1 << exponent).min(MAX_BACKOFF)
}
