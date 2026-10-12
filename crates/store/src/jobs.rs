use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use datalens_core::{BlockRange, ChainId, Dataset};
use sqlx::postgres::types::PgRange;
use sqlx::postgres::{PgListener, PgNotification};

use crate::codec::{
    block_range_from_pg, block_range_to_pg, chain_from_db, dataset_from_db, duration_millis,
};
use crate::ids::{JobId, SelectorId};
use crate::{PgTx, Store, StoreError};

const FILL_DONE: &str = "fill_done";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobPurpose {
    Query,
    Backfill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    pub id: JobId,
    pub chain_id: ChainId,
    pub dataset: Dataset,
    pub selector_id: SelectorId,
    pub blocks: BlockRange,
    pub purpose: JobPurpose,
    pub application_id: String,
    pub status: JobStatus,
    pub attempts: i32,
    pub worker_id: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobState {
    pub id: JobId,
    pub status: JobStatus,
    pub last_error: Option<serde_json::Value>,
}

impl JobPurpose {
    fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Backfill => "backfill",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "query" => Ok(Self::Query),
            "backfill" => Ok(Self::Backfill),
            other => Err(StoreError::data(format!("unknown job purpose {other}"))),
        }
    }
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "done" => Ok(Self::Done),
            "failed" => Ok(Self::Failed),
            other => Err(StoreError::data(format!("unknown job status {other}"))),
        }
    }
}

impl Store {
    /// Inserts a pending job. An active job with the same chunk returns that job's id.
    /// The first creator keeps `application_id`. A `done` or `failed` row is not active.
    pub async fn enqueue(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        blocks: BlockRange,
        purpose: JobPurpose,
        application_id: &str,
    ) -> Result<JobId, StoreError> {
        let range = block_range_to_pg(blocks);
        let inserted: Option<i64> = sqlx::query_scalar(
            "insert into fill_jobs (
                chain_id, dataset, selector_id, blocks, purpose, application_id, status
             )
             values ($1, $2, $3, $4, $5, $6, 'pending')
             on conflict (chain_id, dataset, selector_id, blocks)
                where status in ('pending', 'running')
             do nothing
             returning id",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(range)
        .bind(purpose.as_str())
        .bind(application_id)
        .fetch_optional(&self.pool)
        .await?;
        let id = match inserted {
            Some(id) => id,
            None => {
                sqlx::query_scalar(
                    "select id from fill_jobs
                     where chain_id = $1 and dataset = $2 and selector_id = $3 and blocks = $4
                       and status in ('pending', 'running')",
                )
                .bind(chain.get())
                .bind(dataset.as_str())
                .bind(selector_id.get())
                .bind(range)
                .fetch_one(&self.pool)
                .await?
            }
        };
        Ok(JobId::from_raw(id))
    }

    /// Claims pending jobs whose `next_run_at` has arrived, and running jobs whose lease expired.
    pub async fn claim(
        &self,
        worker_id: &str,
        lease: Duration,
        limit: u32,
        chain: Option<ChainId>,
    ) -> Result<Vec<Job>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "update fill_jobs as job
             set status = 'running',
                 worker_id = $1,
                 lease_until = now() + ($2::bigint * interval '1 millisecond'),
                 attempts = job.attempts + 1,
                 updated_at = now()
             where job.id in (
                select id from fill_jobs
                where (
                    (status = 'pending' and next_run_at <= now())
                    or (status = 'running' and lease_until <= now())
                )
                  and ($3::int is null or chain_id = $3)
                order by next_run_at, id
                limit $4
                for update skip locked
             )
             returning id, chain_id, dataset, selector_id, blocks, purpose, application_id,
                       status, attempts, worker_id, lease_until",
        )
        .bind(worker_id)
        .bind(duration_millis(lease)?)
        .bind(chain.map(|id| id.get()))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in &rows {
            jobs.push(job_from_row(row)?);
        }
        jobs.sort_by_key(|job| job.id);
        Ok(jobs)
    }

    /// Claims other pending jobs in the same chunk so one fetch can fill several selectors.
    pub async fn claim_batch_siblings(&self, job: &Job) -> Result<Vec<Job>, StoreError> {
        let Some(worker_id) = job.worker_id.as_deref() else {
            return Err(StoreError::invalid(
                "batch siblings are claimed by a running job",
            ));
        };
        let Some(lease_until) = job.lease_until else {
            return Err(StoreError::invalid("running job is missing a lease"));
        };
        let rows = sqlx::query(
            "update fill_jobs
             set status = 'running',
                 worker_id = $1,
                 lease_until = $2,
                 attempts = attempts + 1,
                 updated_at = now()
             where id in (
                select id from fill_jobs
                where chain_id = $3 and dataset = $4 and blocks = $5
                  and status = 'pending' and next_run_at <= now() and id <> $6
                for update skip locked
             )
             returning id, chain_id, dataset, selector_id, blocks, purpose, application_id,
                       status, attempts, worker_id, lease_until",
        )
        .bind(worker_id)
        .bind(lease_until)
        .bind(job.chain_id.get())
        .bind(job.dataset.as_str())
        .bind(block_range_to_pg(job.blocks))
        .bind(job.id.get())
        .fetch_all(&self.pool)
        .await?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in &rows {
            jobs.push(job_from_row(row)?);
        }
        jobs.sort_by_key(|job| (job.selector_id, job.id));
        Ok(jobs)
    }

    pub async fn complete_jobs(
        &self,
        tx: &mut PgTx<'_>,
        job_ids: &[JobId],
    ) -> Result<(), StoreError> {
        if job_ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<i64> = job_ids.iter().map(|id| id.get()).collect();
        let done: Vec<i64> = sqlx::query_scalar(
            "update fill_jobs
             set status = 'done', worker_id = null, lease_until = null, updated_at = now()
             where id = any($1) and status <> 'done'
             returning id",
        )
        .bind(&ids)
        .fetch_all(&mut **tx)
        .await?;
        notify_ids(tx, &done).await
    }

    pub async fn fail_job(
        &self,
        job_id: JobId,
        error_json: &serde_json::Value,
        retryable: bool,
        backoff: Duration,
    ) -> Result<(), StoreError> {
        let updated: Option<i64> = sqlx::query_scalar(
            "update fill_jobs
             set status = case when $2 then 'pending' else 'failed' end,
                 last_error = $3,
                 worker_id = null,
                 lease_until = null,
                 next_run_at = case
                    when $2 then now() + ($4::bigint * interval '1 millisecond')
                    else next_run_at
                 end,
                 updated_at = now()
             where id = $1 and status in ('pending', 'running')
             returning id",
        )
        .bind(job_id.get())
        .bind(retryable)
        .bind(error_json)
        .bind(duration_millis(backoff)?)
        .fetch_optional(&self.pool)
        .await?;
        if updated.is_none() {
            return Err(StoreError::invalid(format!(
                "job {job_id} is not pending or running"
            )));
        }
        if !retryable {
            sqlx::query("select pg_notify($1, $2)")
                .bind(FILL_DONE)
                .bind(job_id.get().to_string())
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn job_status(&self, job_ids: &[JobId]) -> Result<Vec<JobState>, StoreError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = job_ids.iter().map(|id| id.get()).collect();
        let rows: Vec<(i64, String, Option<serde_json::Value>)> =
            sqlx::query_as("select id, status, last_error from fill_jobs where id = any($1)")
                .bind(&ids)
                .fetch_all(&self.pool)
                .await?;
        let by_id: HashMap<i64, (String, Option<serde_json::Value>)> = rows
            .into_iter()
            .map(|(id, status, error)| (id, (status, error)))
            .collect();
        let mut states = Vec::with_capacity(job_ids.len());
        for id in job_ids {
            let Some((status, last_error)) = by_id.get(&id.get()) else {
                continue;
            };
            states.push(JobState {
                id: *id,
                status: JobStatus::parse(status)?,
                last_error: last_error.clone(),
            });
        }
        Ok(states)
    }

    /// Listens for `fill_done`, then reads status again so a commit that landed before `LISTEN` is not missed.
    pub async fn wait_done(
        &self,
        job_ids: &[JobId],
        timeout: Duration,
    ) -> Result<Vec<JobState>, StoreError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(FILL_DONE).await?;
        let deadline = Instant::now() + timeout;
        loop {
            let states = self.job_status(job_ids).await?;
            if states.len() != job_ids.len() {
                return Err(StoreError::not_found("fill job not found"));
            }
            if states.iter().all(|state| state.status.is_terminal()) {
                return Ok(states);
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(StoreError::Timeout(format!(
                    "fill jobs {job_ids:?} not done"
                )));
            };
            match tokio::time::timeout(remaining, listener.recv()).await {
                Ok(Ok(note)) => ignore_unrelated(note),
                Ok(Err(err)) => return Err(err.into()),
                Err(_) => {
                    return Err(StoreError::Timeout(format!(
                        "fill jobs {job_ids:?} not done"
                    )));
                }
            }
        }
    }

    pub async fn prune_done(&self, older_than: Duration) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "delete from fill_jobs
             where status = 'done'
               and updated_at <= now() - ($1::bigint * interval '1 millisecond')",
        )
        .bind(duration_millis(older_than)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

fn ignore_unrelated(_note: PgNotification) {}

async fn notify_ids(tx: &mut PgTx<'_>, ids: &[i64]) -> Result<(), StoreError> {
    for id in ids {
        sqlx::query("select pg_notify($1, $2)")
            .bind(FILL_DONE)
            .bind(id.to_string())
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

fn job_from_row(row: &sqlx::postgres::PgRow) -> Result<Job, StoreError> {
    use sqlx::Row;
    let blocks: PgRange<i64> = row.try_get("blocks")?;
    Ok(Job {
        id: JobId::from_raw(row.try_get("id")?),
        chain_id: chain_from_db(row.try_get("chain_id")?)?,
        dataset: dataset_from_db(row.try_get("dataset")?)?,
        selector_id: SelectorId::from_raw(row.try_get("selector_id")?),
        blocks: block_range_from_pg(blocks)?,
        purpose: JobPurpose::parse(row.try_get("purpose")?)?,
        application_id: row.try_get("application_id")?,
        status: JobStatus::parse(row.try_get("status")?)?,
        attempts: row.try_get("attempts")?,
        worker_id: row.try_get("worker_id")?,
        lease_until: row.try_get("lease_until")?,
    })
}
