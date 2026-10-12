use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use datalens_core::{BlockRow, Dataset, Error, Finality, LogRow, LogSelector, Selector};
use datalens_provider::{ProviderError, Purpose, TokenWait};
use datalens_store::{Job, JobId, JobPurpose, SelectorId, StoreError};
use serde_json::json;
use tokio::sync::watch;
use tracing::warn;

use crate::chain::Chain;
use crate::engine::QueryEngine;
use crate::plan::fill_finality;
use crate::usage::UsageCounts;

const MAX_BACKOFF: Duration = Duration::from_secs(300);

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

enum Fetched {
    Logs(Vec<LogRow>),
    Blocks(Vec<BlockRow>),
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
        let siblings = match leader.dataset {
            Dataset::EvmLogs => {
                let max = self
                    .chain_by_id(leader.chain_id)?
                    .settings
                    .max_addresses_per_query
                    .get();
                let limit = u32::try_from(max.saturating_sub(1)).unwrap_or(u32::MAX);
                store.claim_batch_siblings(&leader, limit).await?
            }
            Dataset::EvmBlocks => Vec::new(),
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
                retryable: true,
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

    async fn fill(&self, worker_id: &str, jobs: &[Job]) -> Result<(), FillError> {
        let store = &self.inner.store;
        let leader = &jobs[0];
        let chain = self.chain_by_id(leader.chain_id)?;
        let heads = self.heads(&chain).await?;
        let finality = fill_finality(leader.blocks, &heads);
        let ids: Vec<_> = jobs.iter().map(|job| job.selector_id).collect();
        let mut members = store.selectors_by_id(&ids).await?;
        members.sort_by_key(|(id, _)| *id);
        // A job enqueued from a coverage read that raced another fill's commit finds its
        // chunk already covered. It completes without spending RPC.
        let mut uncovered = Vec::with_capacity(members.len());
        for (selector_id, selector) in members {
            let covered = self
                .covered(&chain, &selector, selector_id, leader.blocks)
                .await?;
            if covered.first() != Some(&leader.blocks) {
                uncovered.push((selector_id, selector));
            }
        }
        let members = uncovered;

        let (fetched, calls) = match leader.dataset {
            _ if members.is_empty() => (None, 0),
            Dataset::EvmLogs => {
                let selectors: Vec<LogSelector> = members
                    .iter()
                    .filter_map(|(_, selector)| match selector {
                        Selector::Logs(logs) => Some(logs.clone()),
                        Selector::Blocks => None,
                    })
                    .collect();
                let (rows, calls) = self
                    .fetch_logs(&chain, leader, &selectors, finality)
                    .await?;
                (Some(Fetched::Logs(rows)), calls)
            }
            Dataset::EvmBlocks => {
                let blocks = chain
                    .evm
                    .block_headers(
                        leader.blocks,
                        purpose(leader),
                        Some(leader.application_id.as_str()),
                        TokenWait::For(self.inner.settings.token_wait),
                        finality,
                    )
                    .await
                    .map_err(FillError::Provider)?;
                (Some(Fetched::Blocks(blocks)), 1)
            }
        };

        let mut tx = store.pool().begin().await.map_err(StoreError::from)?;
        match &fetched {
            None => {}
            Some(Fetched::Logs(rows)) => store.insert_logs(&mut tx, chain.id(), rows).await?,
            Some(Fetched::Blocks(blocks)) => {
                store.insert_blocks(&mut tx, chain.id(), blocks).await?
            }
        }
        let selector_ids: Vec<SelectorId> = members.iter().map(|(id, _)| *id).collect();
        store
            .add_coverage_many(
                &mut tx,
                chain.id(),
                leader.dataset,
                &selector_ids,
                leader.blocks,
                finality,
            )
            .await?;
        let job_ids: Vec<JobId> = jobs.iter().map(|job| job.id).collect();
        let done = store.complete_jobs(&mut tx, worker_id, &job_ids).await?;
        tx.commit().await.map_err(StoreError::from)?;
        if done.len() < job_ids.len() {
            warn!(
                worker_id,
                held = done.len(),
                claimed = job_ids.len(),
                "another worker reclaimed part of the batch"
            );
        }

        for job in jobs {
            self.inner.usage.record(
                &job.application_id,
                chain.id(),
                job.dataset,
                UsageCounts {
                    blocks_filled: job.blocks.len() as i64,
                    provider_calls: if job.id == leader.id { calls } else { 0 },
                    ..UsageCounts::default()
                },
            );
        }
        Ok(())
    }

    /// One `eth_getLogs` per batch group, re-filtered so each row matches some member.
    async fn fetch_logs(
        &self,
        chain: &Chain,
        leader: &Job,
        selectors: &[LogSelector],
        finality: Finality,
    ) -> Result<(Vec<LogRow>, i64), FillError> {
        let max = chain.settings.max_addresses_per_query.get();
        let mut rows: BTreeMap<(u64, u32), LogRow> = BTreeMap::new();
        let mut calls = 0_i64;
        for group in batch_groups(selectors, max) {
            let query = LogSelector::batch(&group, max).unwrap_or_else(|| group[0].clone());
            let page = chain
                .evm
                .get_logs(
                    &query,
                    leader.blocks,
                    purpose(leader),
                    Some(leader.application_id.as_str()),
                    TokenWait::For(self.inner.settings.token_wait),
                    finality,
                )
                .await
                .map_err(|failure| FillError::Provider(failure.error))?;
            calls += 1;
            for row in page.rows {
                if group.iter().any(|member| member.matches(&row)) {
                    rows.insert((row.block_number, row.log_index), row);
                }
            }
        }
        Ok((rows.into_values().collect(), calls))
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
fn batch_groups(selectors: &[LogSelector], max_addresses: usize) -> Vec<Vec<LogSelector>> {
    let mut groups: Vec<Vec<LogSelector>> = Vec::new();
    for selector in selectors {
        if let Some(group) = groups.last_mut() {
            group.push(selector.clone());
            if LogSelector::batch(group, max_addresses).is_some() {
                continue;
            }
            group.pop();
        }
        groups.push(vec![selector.clone()]);
    }
    groups
}

fn backoff(attempts: i32) -> Duration {
    let exponent = attempts.saturating_sub(1).clamp(0, 9) as u32;
    Duration::from_secs(1 << exponent).min(MAX_BACKOFF)
}
