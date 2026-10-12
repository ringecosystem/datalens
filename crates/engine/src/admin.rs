use datalens_core::wire::{
    BackfillRequest, BackfillResponse, CoverageResponse, CoverageSpan, JobInfo, JobsResponse,
};
use datalens_core::{BlockRange, ChainName, Dataset, Error, LogSelector};
use datalens_store::{JobPurpose, JobStatus};

use crate::engine::QueryEngine;
use crate::plan::{fill_chunks, grid_span, uncovered};
use crate::rows::Target;

impl QueryEngine {
    /// Enqueues backfill jobs for every uncovered grid chunk of the range.
    pub async fn backfill(
        &self,
        application: &str,
        request: BackfillRequest,
    ) -> Result<BackfillResponse, Error> {
        let store = &self.inner.store;
        let chain = self.chain(&request.chain)?;
        let selector = Target::new(request.dataset, request.selector, None)?.selector();
        let heads = self.heads(&chain).await?;
        if request.range.end() > heads.safe {
            return Err(Error::InvalidInput(format!(
                "block range end {} is above the safe head {}",
                request.range.end(),
                heads.safe
            )));
        }
        let selector_id = store.upsert_selector(chain.id(), &selector).await?;
        let chunk = chain.settings.chunk(selector.dataset());
        let span = grid_span(request.range, chunk, heads.safe).unwrap_or(request.range);
        let covered = self.covered(&chain, &selector, selector_id, span).await?;
        let mut jobs = Vec::new();
        for chunk in fill_chunks(span, &covered, chunk, heads.safe) {
            let id = store
                .enqueue(
                    chain.id(),
                    request.dataset,
                    selector_id,
                    chunk,
                    JobPurpose::Backfill,
                    application,
                )
                .await?;
            jobs.push(id.get());
        }
        self.inner.enqueued.notify_waiters();
        Ok(BackfillResponse {
            selector_id: selector_id.get(),
            jobs,
        })
    }

    pub async fn coverage(
        &self,
        chain: &ChainName,
        dataset: Dataset,
        selector: Option<LogSelector>,
        range: Option<BlockRange>,
    ) -> Result<CoverageResponse, Error> {
        let store = &self.inner.store;
        let runtime = self.chain(chain)?;
        let selector = Target::new(dataset, selector, None)?.selector();
        let selector_id = store.upsert_selector(runtime.id(), &selector).await?;
        let spans = store
            .coverage_spans(runtime.id(), dataset, selector_id)
            .await?
            .into_iter()
            .map(|(blocks, finality)| CoverageSpan {
                start: blocks.start(),
                end: blocks.end(),
                finality,
            })
            .collect();
        let (covered, missing) = match range {
            Some(range) => {
                let covered = self
                    .covered(&runtime, &selector, selector_id, range)
                    .await?;
                let missing = uncovered(range, &covered);
                (covered, missing)
            }
            None => (Vec::new(), Vec::new()),
        };
        Ok(CoverageResponse {
            chain: chain.clone(),
            dataset,
            selector_id: selector_id.get(),
            spans,
            covered,
            missing,
        })
    }

    pub async fn jobs(&self, status: Option<JobStatus>, limit: u32) -> Result<JobsResponse, Error> {
        let jobs = self
            .inner
            .store
            .list_jobs(status, limit)
            .await?
            .into_iter()
            .map(|(job, last_error)| JobInfo {
                id: job.id.get(),
                chain_id: job.chain_id.get(),
                dataset: job.dataset,
                selector_id: job.selector_id.get(),
                range: job.blocks,
                purpose: job.purpose.as_str().to_owned(),
                application_id: job.application_id,
                status: job.status.as_str().to_owned(),
                attempts: job.attempts,
                last_error,
            })
            .collect();
        Ok(JobsResponse { jobs })
    }
}
