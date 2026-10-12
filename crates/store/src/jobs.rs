use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalens_core::{BlockRange, ChainId, Dataset};
use sqlx::postgres::types::PgRange;

use crate::codec::{
    block_range_from_pg, block_range_to_pg, chain_from_db, dataset_from_db, duration_millis,
};
use crate::ids::{JobId, SelectorId};
use crate::notify::FILL_DONE;
use crate::{PgTx, Store, StoreError};

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
    pub fn as_str(self) -> &'static str {
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
    pub const ALL: [JobStatus; 4] = [Self::Pending, Self::Running, Self::Done, Self::Failed];

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
        }
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
    /// Inserts a pending job. An active job with the same chunk returns that job's id, and
    /// the first creator keeps `application_id`.
    ///
    /// A job that finished while this call ran is returned as is, so the caller sees `done`
    /// and recomputes what is missing. A new job whose chunk the selector's coverage already
    /// holds is stored as `done`, so no worker fetches it again.
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
        for _ in 0..3 {
            let mut tx = self.pool.begin().await?;
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
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(id) = inserted {
                // Runs as its own statement so it sees coverage committed after the insert began.
                let covered: Option<i64> = sqlx::query_scalar(
                    "update fill_jobs as job
                     set status = 'done', updated_at = now()
                     where job.id = $1
                       and int8multirange(job.blocks) <@ (
                            select range_agg(c.blocks) from coverage c
                            where c.chain_id = job.chain_id and c.dataset = job.dataset
                              and c.selector_id = job.selector_id and c.blocks && job.blocks
                       )
                     returning id",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
                if let Some(id) = covered {
                    notify_ids(&mut tx, &[id]).await?;
                }
                tx.commit().await?;
                return Ok(JobId::from_raw(id));
            }
            let latest: Option<i64> = sqlx::query_scalar(
                "select id from fill_jobs
                 where chain_id = $1 and dataset = $2 and selector_id = $3 and blocks = $4
                 order by id desc
                 limit 1",
            )
            .bind(chain.get())
            .bind(dataset.as_str())
            .bind(selector_id.get())
            .bind(range)
            .fetch_optional(&mut *tx)
            .await?;
            tx.commit().await?;
            if let Some(id) = latest {
                return Ok(JobId::from_raw(id));
            }
        }
        Err(StoreError::not_found(format!(
            "fill job for {blocks:?} vanished while enqueueing"
        )))
    }

    /// The pending or running job for exactly this chunk, if any.
    pub async fn active_job(
        &self,
        chain: ChainId,
        dataset: Dataset,
        selector_id: SelectorId,
        blocks: BlockRange,
    ) -> Result<Option<JobId>, StoreError> {
        let id: Option<i64> = sqlx::query_scalar(
            "select id from fill_jobs
             where chain_id = $1 and dataset = $2 and selector_id = $3 and blocks = $4
               and status in ('pending', 'running')",
        )
        .bind(chain.get())
        .bind(dataset.as_str())
        .bind(selector_id.get())
        .bind(block_range_to_pg(blocks))
        .fetch_optional(&self.pool)
        .await?;
        Ok(id.map(JobId::from_raw))
    }

    /// Claims pending jobs whose `next_run_at` has arrived, and running jobs whose lease expired.
    /// `query` jobs go first, because a caller is waiting on them.
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
                order by (purpose = 'query') desc, next_run_at, id
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

    /// Claims up to `limit` other pending jobs in the same chunk, lowest id first, so one
    /// fetch can fill several selectors. The caller sizes `limit` to its address cap.
    pub async fn claim_batch_siblings(
        &self,
        job: &Job,
        limit: u32,
    ) -> Result<Vec<Job>, StoreError> {
        let Some(worker_id) = job.worker_id.as_deref() else {
            return Err(StoreError::invalid(
                "batch siblings are claimed by a running job",
            ));
        };
        let Some(lease_until) = job.lease_until else {
            return Err(StoreError::invalid("running job is missing a lease"));
        };
        if limit == 0 {
            return Ok(Vec::new());
        }
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
                order by id
                limit $7
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
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in &rows {
            jobs.push(job_from_row(row)?);
        }
        jobs.sort_by_key(|job| (job.selector_id, job.id));
        Ok(jobs)
    }

    /// Marks the jobs `done` that are still `running` under `worker_id` and returns them in
    /// id order. A job another worker reclaimed after this lease expired is left alone.
    pub async fn complete_jobs(
        &self,
        tx: &mut PgTx<'_>,
        worker_id: &str,
        job_ids: &[JobId],
    ) -> Result<Vec<JobId>, StoreError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = job_ids.iter().map(|id| id.get()).collect();
        let mut done: Vec<i64> = sqlx::query_scalar(
            "update fill_jobs
             set status = 'done', worker_id = null, lease_until = null, updated_at = now()
             where id = any($1) and status = 'running' and worker_id = $2
             returning id",
        )
        .bind(&ids)
        .bind(worker_id)
        .fetch_all(&mut **tx)
        .await?;
        notify_ids(tx, &done).await?;
        done.sort_unstable();
        Ok(done.into_iter().map(JobId::from_raw).collect())
    }

    /// Records a failure for a job still `running` under `worker_id`. Returns `false` when this
    /// worker no longer holds it. A retryable failure goes back to `pending` after `backoff`.
    /// A final one becomes `failed` and wakes its waiters.
    pub async fn fail_job(
        &self,
        job_id: JobId,
        worker_id: &str,
        error_json: &serde_json::Value,
        retryable: bool,
        backoff: Duration,
    ) -> Result<bool, StoreError> {
        let mut tx = self.pool.begin().await?;
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
             where id = $1 and status = 'running' and worker_id = $5
             returning id",
        )
        .bind(job_id.get())
        .bind(retryable)
        .bind(error_json)
        .bind(duration_millis(backoff)?)
        .bind(worker_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(id) = updated else {
            return Ok(false);
        };
        if !retryable {
            notify_ids(&mut tx, &[id]).await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    /// Moves `lease_until` to now plus `lease` for jobs still `running` under `worker_id`, and
    /// returns them in id order. A fetch slower than its lease calls this to keep the chunk.
    pub async fn extend_lease(
        &self,
        worker_id: &str,
        job_ids: &[JobId],
        lease: Duration,
    ) -> Result<Vec<JobId>, StoreError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = job_ids.iter().map(|id| id.get()).collect();
        let mut kept: Vec<i64> = sqlx::query_scalar(
            "update fill_jobs
             set lease_until = now() + ($3::bigint * interval '1 millisecond'), updated_at = now()
             where id = any($1) and status = 'running' and worker_id = $2
             returning id",
        )
        .bind(&ids)
        .bind(worker_id)
        .bind(duration_millis(lease)?)
        .fetch_all(&self.pool)
        .await?;
        kept.sort_unstable();
        Ok(kept.into_iter().map(JobId::from_raw).collect())
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

    /// Waits until every job is `done` or `failed`. Status is reread on each matching
    /// `fill_done` and at least once a second, so a dropped notification only delays the wake.
    pub async fn wait_done(
        &self,
        job_ids: &[JobId],
        timeout: Duration,
    ) -> Result<Vec<JobState>, StoreError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: HashSet<String> = job_ids.iter().map(|id| id.to_string()).collect();
        let store = self;
        let done = self
            .wait_until(
                timeout,
                |channel, payload| channel == FILL_DONE && wanted.contains(payload),
                move || async move {
                    let states = store.job_status(job_ids).await?;
                    if states.len() != job_ids.len() {
                        return Err(StoreError::not_found("fill job not found"));
                    }
                    Ok(states
                        .iter()
                        .all(|state| state.status.is_terminal())
                        .then_some(states))
                },
            )
            .await?;
        done.ok_or_else(|| StoreError::Timeout(format!("fill jobs {job_ids:?} not done")))
    }

    /// Most recently updated first, with each job's `last_error`.
    pub async fn list_jobs(
        &self,
        status: Option<JobStatus>,
        limit: u32,
    ) -> Result<Vec<(Job, Option<serde_json::Value>)>, StoreError> {
        let rows = sqlx::query(
            "select id, chain_id, dataset, selector_id, blocks, purpose, application_id,
                    status, attempts, worker_id, lease_until, last_error
             from fill_jobs
             where $1::text is null or status = $1
             order by updated_at desc, id desc
             limit $2",
        )
        .bind(status.map(JobStatus::as_str))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in &rows {
            use sqlx::Row;
            jobs.push((job_from_row(row)?, row.try_get("last_error")?));
        }
        Ok(jobs)
    }

    /// Job count per status. Statuses with no jobs are reported as zero.
    pub async fn job_counts(&self) -> Result<Vec<(JobStatus, i64)>, StoreError> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("select status, count(*) from fill_jobs group by status")
                .fetch_all(&self.pool)
                .await?;
        let mut counts: Vec<(JobStatus, i64)> =
            JobStatus::ALL.iter().map(|status| (*status, 0)).collect();
        for (status, count) in rows {
            let status = JobStatus::parse(&status)?;
            if let Some(slot) = counts.iter_mut().find(|(known, _)| *known == status) {
                slot.1 = count;
            }
        }
        Ok(counts)
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

    /// Failed jobs keep `last_error` for inspection, so they usually get a longer retention.
    pub async fn prune_failed(&self, older_than: Duration) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "delete from fill_jobs
             where status = 'failed'
               and updated_at <= now() - ($1::bigint * interval '1 millisecond')",
        )
        .bind(duration_millis(older_than)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

async fn notify_ids(tx: &mut PgTx<'_>, ids: &[i64]) -> Result<(), StoreError> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("select pg_notify($1, id::text) from unnest($2::bigint[]) as id")
        .bind(FILL_DONE)
        .bind(ids)
        .execute(&mut **tx)
        .await?;
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
