use std::{io, thread::sleep, time::Duration};

use reqwest::{
    StatusCode,
    blocking::Client,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde::Serialize;

use super::{
    IndexedRecord, OutputWriteReceipt, OutputWriteResult, WebhookOutputConfig,
    event::{NormalizedIndexedEvent, max_position},
};

pub(super) fn write_records_webhook(
    config: &WebhookOutputConfig,
    records: &[IndexedRecord],
) -> io::Result<OutputWriteResult> {
    let client = Client::builder()
        .timeout(Duration::from_millis(config.timeout_ms))
        .build()
        .map_err(io::Error::other)?;
    let headers = request_headers(config)?;
    let secret_values = config
        .headers
        .iter()
        .filter(|header| header.secret)
        .map(|header| header.value.as_str())
        .collect::<Vec<_>>();
    let batches = build_batches(config, records)?;
    let mut batches_attempted = 0;
    let mut batches_delivered = 0;
    let mut highest_position = None;

    for batch in &batches {
        let attempts = deliver_batch(
            &client,
            config,
            &headers,
            &secret_values,
            batch,
            &mut batches_attempted,
        )?;
        if attempts > 0 {
            batches_delivered += 1;
        }
        highest_position = max_position(highest_position, batch.highest_position.clone());
    }

    let highest_position = highest_position.map(|position| position.receipt_key);
    Ok(OutputWriteResult {
        written_rows: records.len(),
        receipt: Some(OutputWriteReceipt {
            accepted_rows: records.len(),
            flushed_rows: records.len(),
            inserted_rows: records.len(),
            skipped_or_replaced_rows: 0,
            files_written: 0,
            batches_attempted,
            batches_delivered,
            highest_position: highest_position.clone(),
            last_record: highest_position,
        }),
    })
}

fn request_headers(config: &WebhookOutputConfig) -> io::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for header in &config.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(io::Error::other)?;
        let value = HeaderValue::from_str(&header.value).map_err(io::Error::other)?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn build_batches(
    config: &WebhookOutputConfig,
    records: &[IndexedRecord],
) -> io::Result<Vec<WebhookBatch>> {
    let max_rows = config.max_rows_per_request.max(1);
    let max_bytes = config.max_bytes_per_request.max(1);
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes: usize = 0;

    for record in records {
        let row = WebhookRow::from_record(record);
        let row_bytes = serde_json::to_vec(&row).map_err(io::Error::other)?.len();
        if !current.is_empty()
            && (current.len() >= max_rows || current_bytes.saturating_add(row_bytes) > max_bytes)
        {
            batches.push(WebhookBatch::new(
                batches.len(),
                std::mem::take(&mut current),
            ));
            current_bytes = 0;
        }
        current.push(row);
        current_bytes = current_bytes.saturating_add(row_bytes);
    }

    if !current.is_empty() {
        batches.push(WebhookBatch::new(batches.len(), current));
    }

    Ok(batches)
}

fn deliver_batch(
    client: &Client,
    config: &WebhookOutputConfig,
    headers: &HeaderMap,
    secret_values: &[&str],
    batch: &WebhookBatch,
    batches_attempted: &mut usize,
) -> io::Result<usize> {
    let payload = batch.payload();
    let mut attempts = 0;
    loop {
        attempts += 1;
        *batches_attempted += 1;
        let mut request = client
            .post(&config.url)
            .headers(headers.clone())
            .json(&payload);
        if let Some(header) = &config.idempotency_key_header {
            request = request.header(header, &batch.id);
        }

        match request.send() {
            Ok(response) if response.status().is_success() => return Ok(attempts),
            Ok(response) => {
                let status = response.status();
                let body = response.text().unwrap_or_default();
                if !is_retryable_status(status, config) || attempts >= max_attempts(config) {
                    return Err(io::Error::other(format!(
                        "webhook delivery failed with status {}: {}",
                        status.as_u16(),
                        sanitize(&body, secret_values)
                    )));
                }
            }
            Err(error) => {
                if !is_retryable_error(&error) || attempts >= max_attempts(config) {
                    return Err(io::Error::other(format!(
                        "webhook delivery failed: {}",
                        sanitize(&error.to_string(), secret_values)
                    )));
                }
            }
        }

        sleep(backoff(config, attempts));
    }
}

fn max_attempts(config: &WebhookOutputConfig) -> usize {
    config.retry.max_attempts.max(1)
}

fn is_retryable_status(status: StatusCode, config: &WebhookOutputConfig) -> bool {
    status.is_server_error() || (status == StatusCode::TOO_MANY_REQUESTS && config.retry.retry_429)
}

fn is_retryable_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect()
}

fn backoff(config: &WebhookOutputConfig, attempts: usize) -> Duration {
    let shift = attempts.saturating_sub(1).min(16);
    let multiplier = 1_u64 << shift;
    let millis = config
        .retry
        .initial_backoff_ms
        .saturating_mul(multiplier)
        .min(config.retry.max_backoff_ms);
    Duration::from_millis(millis)
}

fn sanitize(value: &str, secret_values: &[&str]) -> String {
    let mut sanitized = value.to_owned();
    for secret in secret_values {
        if !secret.is_empty() {
            sanitized = sanitized.replace(secret, "<redacted>");
            if let Some(token) = secret.strip_prefix("Bearer ") {
                sanitized = sanitized.replace(token, "<redacted>");
            }
        }
    }
    redact_bearer_tokens(&sanitized)
}

fn redact_bearer_tokens(value: &str) -> String {
    let mut output = String::new();
    let mut rest = value;
    loop {
        let lower = rest.to_ascii_lowercase();
        let Some(index) = lower.find("bearer ") else {
            output.push_str(rest);
            return output;
        };
        output.push_str(&rest[..index]);
        output.push_str("bearer <redacted>");
        let token_start = index + "bearer ".len();
        let token_end = rest[token_start..]
            .find(char::is_whitespace)
            .map(|offset| token_start + offset)
            .unwrap_or(rest.len());
        rest = &rest[token_end..];
    }
}

#[derive(Clone)]
struct WebhookBatch {
    id: String,
    sequence: usize,
    rows: Vec<WebhookRow>,
    chain_family: String,
    chain_name: String,
    chain_id: u64,
    chain_identity: String,
    index: String,
    dataset: String,
    highest_position: Option<super::event::EventPosition>,
    first_position_key: Option<String>,
    highest_position_key: Option<String>,
}

impl WebhookBatch {
    fn new(sequence: usize, rows: Vec<WebhookRow>) -> Self {
        let first = rows.first().expect("nonempty webhook batch");
        let index = first.index.clone();
        let chain_name = first.chain.clone();
        let chain_id = first.chain_id;
        let dataset = first.dataset.clone();
        let chain_family = chain_family(&first.dataset);
        let chain_identity = format!("{}:{}:{}", chain_family, first.chain, first.chain_id);
        let mut highest_position = None;
        for row in &rows {
            highest_position = max_position(highest_position, row.position.clone());
        }
        let first_position_key = rows
            .first()
            .and_then(|row| row.position.clone())
            .map(|position| position.receipt_key);
        let highest_position_key = highest_position
            .clone()
            .map(|position| position.receipt_key);
        let id = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            index,
            chain_family,
            chain_name,
            chain_id,
            dataset,
            highest_position_key.as_deref().unwrap_or("none"),
            sequence
        );

        Self {
            id,
            sequence,
            rows,
            chain_family,
            chain_name,
            chain_id,
            chain_identity,
            index,
            dataset,
            highest_position,
            first_position_key,
            highest_position_key,
        }
    }

    fn payload(&self) -> WebhookPayload<'_> {
        WebhookPayload {
            index: &self.index,
            chain: WebhookChain {
                family: &self.chain_family,
                name: &self.chain_name,
                id: self.chain_id,
                identity: &self.chain_identity,
            },
            dataset: &self.dataset,
            batch: WebhookBatchMetadata {
                id: &self.id,
                sequence: self.sequence,
                row_count: self.rows.len(),
                first_position: self.first_position_key.as_deref(),
                highest_position: self.highest_position_key.as_deref(),
            },
            rows: &self.rows,
        }
    }
}

#[derive(Clone, Serialize)]
struct WebhookRow {
    index: String,
    chain: String,
    chain_id: u64,
    dataset: String,
    payload: serde_json::Value,
    #[serde(skip)]
    position: Option<super::event::EventPosition>,
}

impl WebhookRow {
    fn from_record(record: &IndexedRecord) -> Self {
        let event = NormalizedIndexedEvent::from_record(record);
        Self {
            index: record.index.clone(),
            chain: record.chain.clone(),
            chain_id: record.chain_id,
            dataset: record.dataset.clone(),
            payload: record.payload.clone(),
            position: event.position,
        }
    }
}

#[derive(Serialize)]
struct WebhookPayload<'a> {
    index: &'a str,
    chain: WebhookChain<'a>,
    dataset: &'a str,
    batch: WebhookBatchMetadata<'a>,
    rows: &'a [WebhookRow],
}

#[derive(Serialize)]
struct WebhookChain<'a> {
    family: &'a str,
    name: &'a str,
    id: u64,
    identity: &'a str,
}

#[derive(Serialize)]
struct WebhookBatchMetadata<'a> {
    id: &'a str,
    sequence: usize,
    row_count: usize,
    first_position: Option<&'a str>,
    highest_position: Option<&'a str>,
}

fn chain_family(dataset: &str) -> String {
    dataset
        .split_once('.')
        .map(|(family, _)| family.to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}
