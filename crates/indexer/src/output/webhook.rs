use std::{io, thread::sleep, time::Duration};

use reqwest::{
    StatusCode,
    blocking::Client,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde::Serialize;

use super::{
    IndexedRecord, OutputWriteReceipt, OutputWriteResult, OutputWriteSink, WebhookOutputConfig,
    event::{NormalizedIndexedEvent, max_position},
    webhook_outbox::{WebhookOutboxBatch, WebhookOutboxRecord, WebhookOutboxStore},
};

pub struct WebhookOutputSink {
    config: WebhookOutputConfig,
    client: Client,
    headers: HeaderMap,
    secret_values: Vec<String>,
    outbox: Option<WebhookOutboxStore>,
}

impl WebhookOutputSink {
    pub fn connect(config: WebhookOutputConfig) -> io::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(io::Error::other)?;
        let headers = request_headers(&config)?;
        let secret_values = config
            .headers
            .iter()
            .filter(|header| header.secret)
            .map(|header| header.value.clone())
            .collect::<Vec<_>>();
        let outbox = if config.outbox.enabled {
            Some(WebhookOutboxStore::connect(
                config
                    .outbox
                    .path
                    .as_deref()
                    .ok_or_else(|| io::Error::other("webhook outbox path is required"))?,
            )?)
        } else {
            None
        };

        Ok(Self {
            config,
            client,
            headers,
            secret_values,
            outbox,
        })
    }

    fn write_without_outbox(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        let batches = build_batches(&self.config, records)?;
        let mut batches_attempted = 0;
        let mut batches_delivered = 0;
        let mut highest_position = None;

        for batch in &batches {
            let attempts = deliver_batch(
                &self.client,
                &self.config,
                &self.headers,
                &self.secret_values,
                batch,
                &mut batches_attempted,
            )?;
            if attempts > 0 {
                batches_delivered += 1;
            }
            highest_position = max_position(highest_position, batch.highest_position.clone());
        }

        Ok(write_result(
            records.len(),
            batches_attempted,
            batches_delivered,
            highest_position,
        ))
    }

    fn write_with_outbox(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        let replay = self.flush_outbox()?;
        let batches = build_batches(&self.config, records)?;
        let outbox = self.outbox.as_ref().expect("outbox enabled");
        let mut batches_attempted = replay
            .receipt
            .as_ref()
            .map(|receipt| receipt.batches_attempted)
            .unwrap_or_default();
        let mut batches_delivered = replay
            .receipt
            .as_ref()
            .map(|receipt| receipt.batches_delivered)
            .unwrap_or_default();
        let mut highest_position = None;

        for batch in &batches {
            outbox.upsert_pending(self.outbox_batch(batch)?)?;
            let delivery = self.deliver_outbox_batch(batch)?;
            batches_attempted += delivery.attempts;
            if delivery.delivered {
                batches_delivered += 1;
            }
            highest_position = max_position(highest_position, batch.highest_position.clone());
        }

        Ok(write_result(
            records.len(),
            batches_attempted,
            batches_delivered,
            highest_position,
        ))
    }

    fn flush_outbox(&self) -> io::Result<OutputWriteResult> {
        let Some(outbox) = &self.outbox else {
            return Ok(OutputWriteResult {
                written_rows: 0,
                receipt: None,
            });
        };
        let records = outbox.pending_records()?;
        let mut batches_attempted = 0;
        let mut batches_delivered = 0;

        for record in &records {
            let delivery = self.deliver_outbox_record(record)?;
            batches_attempted += delivery.attempts;
            if delivery.delivered {
                batches_delivered += 1;
            }
        }

        Ok(write_result(0, batches_attempted, batches_delivered, None))
    }

    fn deliver_outbox_batch(&self, batch: &WebhookBatch) -> io::Result<OutboxDelivery> {
        let payload = serde_json::to_value(batch.payload()).map_err(io::Error::other)?;
        self.deliver_outbox_payload(&batch.id, payload)
    }

    fn deliver_outbox_record(&self, record: &WebhookOutboxRecord) -> io::Result<OutboxDelivery> {
        self.deliver_outbox_payload(&record.batch_id, record.payload.clone())
    }

    fn deliver_outbox_payload(
        &self,
        batch_id: &str,
        payload: serde_json::Value,
    ) -> io::Result<OutboxDelivery> {
        let outbox = self.outbox.as_ref().expect("outbox enabled");
        let mut attempts = 0;
        let mut delivered = false;

        loop {
            let current_attempts = outbox.attempt_count(batch_id)?;
            if current_attempts >= self.config.outbox.max_attempts {
                outbox.mark_dead_letter(batch_id, "webhook delivery attempts exhausted")?;
                break;
            }

            attempts += 1;
            let outcome = send_payload(
                &self.client,
                &self.config,
                &self.headers,
                batch_id,
                &payload,
                &self.secret_values,
            );
            outbox.record_attempt(batch_id, outcome.error.as_deref().unwrap_or_default())?;
            if outcome.delivered {
                outbox.delete(batch_id)?;
                delivered = true;
                break;
            }
            if !outcome.retryable {
                outbox.mark_dead_letter(batch_id, outcome.error.as_deref().unwrap_or_default())?;
                break;
            }
            if attempts >= max_attempts(&self.config) {
                break;
            }
            sleep(backoff(&self.config, attempts));
        }

        Ok(OutboxDelivery {
            attempts,
            delivered,
        })
    }

    fn outbox_batch(&self, batch: &WebhookBatch) -> io::Result<WebhookOutboxBatch> {
        let payload_json = serde_json::to_string(&batch.payload()).map_err(io::Error::other)?;
        let header_names = self
            .config
            .headers
            .iter()
            .map(|header| HeaderReference {
                name: header.name.clone(),
                secret: header.secret,
            })
            .collect::<Vec<_>>();
        let header_names_json = serde_json::to_string(&header_names).map_err(io::Error::other)?;
        Ok(WebhookOutboxBatch {
            batch_id: batch.id.clone(),
            idempotency_key: batch.id.clone(),
            endpoint_url: redact_url(&self.config.url),
            header_names_json,
            payload_json,
        })
    }
}

impl OutputWriteSink for WebhookOutputSink {
    fn write_records(&self, records: &[IndexedRecord]) -> io::Result<OutputWriteResult> {
        if self.outbox.is_some() {
            self.write_with_outbox(records)
        } else {
            self.write_without_outbox(records)
        }
    }

    fn flush(&self) -> io::Result<OutputWriteResult> {
        self.flush_outbox()
    }
}

fn write_result(
    written_rows: usize,
    batches_attempted: usize,
    batches_delivered: usize,
    highest_position: Option<super::event::EventPosition>,
) -> OutputWriteResult {
    let highest_position = highest_position.map(|position| position.receipt_key);
    OutputWriteResult {
        written_rows,
        receipt: Some(OutputWriteReceipt {
            accepted_rows: written_rows,
            flushed_rows: written_rows,
            inserted_rows: written_rows,
            skipped_or_replaced_rows: 0,
            files_written: 0,
            batches_attempted,
            batches_delivered,
            highest_position: highest_position.clone(),
            last_record: highest_position,
        }),
    }
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
    secret_values: &[String],
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

fn sanitize(value: &str, secret_values: &[String]) -> String {
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

#[derive(Serialize)]
struct HeaderReference {
    name: String,
    secret: bool,
}

struct OutboxDelivery {
    attempts: usize,
    delivered: bool,
}

struct DeliveryOutcome {
    delivered: bool,
    retryable: bool,
    error: Option<String>,
}

fn send_payload(
    client: &Client,
    config: &WebhookOutputConfig,
    headers: &HeaderMap,
    batch_id: &str,
    payload: &serde_json::Value,
    secret_values: &[String],
) -> DeliveryOutcome {
    let mut request = client
        .post(&config.url)
        .headers(headers.clone())
        .json(payload);
    if let Some(header) = &config.idempotency_key_header {
        request = request.header(header, batch_id);
    }

    match request.send() {
        Ok(response) if response.status().is_success() => DeliveryOutcome {
            delivered: true,
            retryable: false,
            error: None,
        },
        Ok(response) => {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            DeliveryOutcome {
                delivered: false,
                retryable: is_retryable_status(status, config),
                error: Some(format!(
                    "webhook delivery failed with status {}: {}",
                    status.as_u16(),
                    sanitize(&body, secret_values)
                )),
            }
        }
        Err(error) => DeliveryOutcome {
            delivered: false,
            retryable: is_retryable_error(&error),
            error: Some(format!(
                "webhook delivery failed: {}",
                sanitize(&error.to_string(), secret_values)
            )),
        },
    }
}

fn redact_url(url: &str) -> String {
    let Some((base, _)) = url.split_once('?') else {
        return url.to_owned();
    };
    format!("{base}?<redacted>")
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
