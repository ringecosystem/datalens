use datalens_client::{
    ApiErrorKind, ClientError, DatalensClient, HttpTransport, QueryRequest, QueryResponse,
};
use datalens_core::LedgerRange;

use crate::{IndexRetryConfig, IndexerError, PlannedIndexTask};

pub(super) fn query_with_retry<T>(
    client: &DatalensClient<T>,
    request: QueryRequest,
    task: &PlannedIndexTask,
    range: &LedgerRange,
    retry: &IndexRetryConfig,
) -> Result<QueryResponse, IndexerError>
where
    T: HttpTransport,
{
    let max_attempts = retry.max_attempts.max(1);
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.query(request.clone()) {
            Ok(response) => {
                if attempts > 1 {
                    log::info!(
                        "task {} retry succeeded attempt={} range={}-{}",
                        task.label,
                        attempts,
                        range.start(),
                        range.end()
                    );
                }
                return Ok(response);
            }
            Err(error) if is_retryable_client_error(&error) && attempts < max_attempts => {
                let backoff_ms = retry_backoff_ms(retry, attempts);
                log::warn!(
                    "task {} transient query failure attempt={}/{} backoff_ms={} range={}-{} kind={} error={}",
                    task.label,
                    attempts,
                    max_attempts,
                    backoff_ms,
                    range.start(),
                    range.end(),
                    client_error_kind_name(&error),
                    error
                );
                sleep_retry_backoff(backoff_ms);
            }
            Err(error) if is_retryable_client_error(&error) => {
                log::error!(
                    "task {} transient query failure attempts exhausted attempts={} range={}-{} kind={} error={}",
                    task.label,
                    attempts,
                    range.start(),
                    range.end(),
                    client_error_kind_name(&error),
                    error
                );
                return Err(IndexerError::Runner(format!(
                    "task {} failed after transient query attempts exhausted attempts={} kind={}: {error}",
                    task.label,
                    attempts,
                    client_error_kind_name(&error)
                )));
            }
            Err(error) => {
                return Err(IndexerError::Runner(format!(
                    "task {} failed: {error}",
                    task.label
                )));
            }
        }
    }
}

fn is_retryable_client_error(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Api {
            kind: ApiErrorKind::ProviderFailure
                | ApiErrorKind::ProviderTimeout
                | ApiErrorKind::RateLimited
                | ApiErrorKind::StorageReadFailure
                | ApiErrorKind::StorageWriteFailure
                | ApiErrorKind::ManifestUpdateFailure,
            ..
        }
    )
}

fn client_error_kind_name(error: &ClientError) -> &'static str {
    match error {
        ClientError::Api {
            kind: ApiErrorKind::ProviderFailure,
            ..
        } => "provider_failure",
        ClientError::Api {
            kind: ApiErrorKind::ProviderTimeout,
            ..
        } => "provider_timeout",
        ClientError::Api {
            kind: ApiErrorKind::RateLimited,
            ..
        } => "rate_limited",
        ClientError::Api {
            kind: ApiErrorKind::StorageReadFailure,
            ..
        } => "storage_read_failure",
        ClientError::Api {
            kind: ApiErrorKind::StorageWriteFailure,
            ..
        } => "storage_write_failure",
        ClientError::Api {
            kind: ApiErrorKind::ManifestUpdateFailure,
            ..
        } => "manifest_update_failure",
        _ => "non_retryable",
    }
}

fn retry_backoff_ms(retry: &IndexRetryConfig, attempts: u32) -> u64 {
    retry
        .initial_backoff_ms
        .saturating_mul(2u64.saturating_pow(attempts.saturating_sub(1)))
        .min(retry.max_backoff_ms)
}

fn sleep_retry_backoff(backoff_ms: u64) {
    if backoff_ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
    }
}
