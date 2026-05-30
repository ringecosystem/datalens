use std::{
    error::Error,
    io::Write,
    time::{Duration, Instant},
};

use datalens_client::{DatalensClient, HttpTransport, QueryResponse};
use serde::Serialize;

use crate::{
    OrmpExampleError, OrmpPlan, OrmpPlanJob, RangeSummary, build_job_query_request,
    summarize_response,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LongRunJobRecord {
    Ok(LongRunJobSummary),
    Error(LongRunJobError),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LongRunJobSummary {
    pub label: String,
    pub chain: String,
    pub chain_id: u64,
    pub range: RangeSummary,
    pub elapsed_ms: u128,
    pub row_count: usize,
    pub hit_ranges: Vec<RangeSummary>,
    pub missing_ranges: Vec<RangeSummary>,
    pub durable_hit_ranges: Vec<RangeSummary>,
    pub provider_fill_ranges: Vec<RangeSummary>,
    pub full_durable_cache_hit: bool,
    pub first_log_block: Option<u64>,
    pub last_log_block: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LongRunJobError {
    pub label: String,
    pub chain: String,
    pub chain_id: u64,
    pub range: RangeSummary,
    pub elapsed_ms: u128,
    pub error: String,
}

pub fn run_plan_with_client<T, W>(
    client: &DatalensClient<T>,
    plan: &OrmpPlan,
    writer: &mut W,
) -> Result<(), Box<dyn Error>>
where
    T: HttpTransport,
    W: Write,
{
    for job in &plan.jobs {
        let started = Instant::now();
        let record = match run_job(client, job, started) {
            Ok(summary) => LongRunJobRecord::Ok(summary),
            Err(error) => LongRunJobRecord::Error(LongRunJobError {
                label: job.label.clone(),
                chain: job.chain.clone(),
                chain_id: job.chain_id,
                range: job.range_summary(),
                elapsed_ms: elapsed_ms(started.elapsed()),
                error: error.to_string(),
            }),
        };
        serde_json::to_writer(&mut *writer, &record)?;
        writeln!(writer)?;
    }
    Ok(())
}

fn run_job<T>(
    client: &DatalensClient<T>,
    job: &OrmpPlanJob,
    started: Instant,
) -> Result<LongRunJobSummary, Box<dyn Error>>
where
    T: HttpTransport,
{
    let request = build_job_query_request(job)?;
    let response = client.query(request)?;
    Ok(summarize_job_result(
        job,
        &response,
        elapsed_ms(started.elapsed()),
    )?)
}

pub fn summarize_job_result(
    job: &OrmpPlanJob,
    response: &QueryResponse,
    elapsed_ms: u128,
) -> Result<LongRunJobSummary, OrmpExampleError> {
    let summary = summarize_response(response)?;

    Ok(LongRunJobSummary {
        label: job.label.clone(),
        chain: job.chain.clone(),
        chain_id: job.chain_id,
        range: summary.requested_range,
        elapsed_ms,
        row_count: summary.row_count,
        hit_ranges: summary.hit_ranges,
        missing_ranges: summary.missing_ranges,
        durable_hit_ranges: summary.durable_hit_ranges,
        provider_fill_ranges: summary.provider_fill_ranges,
        full_durable_cache_hit: summary.full_durable_cache_hit,
        first_log_block: summary.first_log_block,
        last_log_block: summary.last_log_block,
    })
}

fn elapsed_ms(duration: Duration) -> u128 {
    duration.as_millis()
}
