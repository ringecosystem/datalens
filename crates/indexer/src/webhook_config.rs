use std::env;

use serde::Deserialize;

use crate::{
    OutputConfig, WebhookHeaderConfig, WebhookOutboxConfig, WebhookOutputConfig, WebhookRetryConfig,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawWebhookOutputConfig {
    url: Option<String>,
    method: Option<String>,
    #[serde(default)]
    headers: Vec<RawWebhookHeaderConfig>,
    timeout_ms: Option<u64>,
    max_rows_per_request: Option<usize>,
    max_bytes_per_request: Option<usize>,
    retry: Option<RawWebhookRetryConfig>,
    outbox: Option<RawWebhookOutboxConfig>,
    idempotency_key_header: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWebhookHeaderConfig {
    name: Option<String>,
    value: Option<String>,
    env: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWebhookRetryConfig {
    max_attempts: Option<usize>,
    initial_backoff_ms: Option<u64>,
    max_backoff_ms: Option<u64>,
    retry_429: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWebhookOutboxConfig {
    enabled: Option<bool>,
    path: Option<std::path::PathBuf>,
    max_attempts: Option<usize>,
}

pub(crate) fn parse_webhook_output(
    raw: Option<RawWebhookOutputConfig>,
    errors: &mut Vec<String>,
) -> Option<OutputConfig> {
    let Some(webhook) = raw else {
        errors.push("output.webhook: missing required table".to_owned());
        return None;
    };
    if let Some(method) = webhook.method.as_deref()
        && method != "POST"
    {
        errors.push("output.webhook.method: supported value is POST".to_owned());
    }
    let url = required_non_empty("output.webhook.url", webhook.url, errors);
    let headers = parse_webhook_headers(webhook.headers, errors);
    let retry = parse_webhook_retry(webhook.retry, errors);
    let outbox = parse_webhook_outbox(webhook.outbox, errors);
    let timeout_ms = optional_positive_u64(
        "output.webhook.timeout_ms",
        webhook.timeout_ms,
        5_000,
        errors,
    );
    let max_rows_per_request = optional_positive_usize(
        "output.webhook.max_rows_per_request",
        webhook.max_rows_per_request,
        500,
        errors,
    );
    let max_bytes_per_request = optional_positive_usize(
        "output.webhook.max_bytes_per_request",
        webhook.max_bytes_per_request,
        1_000_000,
        errors,
    );
    let idempotency_key_header = optional_non_empty(
        "output.webhook.idempotency_key_header",
        webhook.idempotency_key_header,
        errors,
    );

    Some(OutputConfig::Webhook {
        webhook: WebhookOutputConfig {
            url: url?,
            headers,
            timeout_ms,
            max_rows_per_request,
            max_bytes_per_request,
            retry,
            idempotency_key_header,
            outbox,
        },
    })
}

fn parse_webhook_headers(
    raw_headers: Vec<RawWebhookHeaderConfig>,
    errors: &mut Vec<String>,
) -> Vec<WebhookHeaderConfig> {
    raw_headers
        .into_iter()
        .enumerate()
        .filter_map(|(index, raw)| {
            let prefix = format!("output.webhook.headers[{index}]");
            let name = required_non_empty(&format!("{prefix}.name"), raw.name, errors)?;
            let env_provided = raw.env.is_some();
            let value = match (raw.value, raw.env) {
                (Some(value), None) => {
                    required_non_empty(&format!("{prefix}.value"), Some(value), errors)
                }
                (None, Some(env_name)) => {
                    let env_name =
                        required_non_empty(&format!("{prefix}.env"), Some(env_name), errors)?;
                    env::var(&env_name)
                        .map_err(|_| {
                            errors.push(format!(
                                "{prefix}.env: environment variable {env_name} is not set"
                            ));
                        })
                        .ok()
                }
                (Some(_), Some(_)) => {
                    errors.push(format!("{prefix}: specify value or env, not both"));
                    None
                }
                (None, None) => {
                    errors.push(format!("{prefix}: missing required value or env"));
                    None
                }
            }?;
            Some(WebhookHeaderConfig {
                secret: is_secret_header(&name) || env_provided,
                name,
                value,
            })
        })
        .collect()
}

fn parse_webhook_retry(
    raw: Option<RawWebhookRetryConfig>,
    errors: &mut Vec<String>,
) -> WebhookRetryConfig {
    let default = WebhookRetryConfig::default();
    let Some(raw) = raw else {
        return default;
    };
    WebhookRetryConfig {
        max_attempts: optional_positive_usize(
            "output.webhook.retry.max_attempts",
            raw.max_attempts,
            default.max_attempts,
            errors,
        ),
        initial_backoff_ms: optional_positive_u64(
            "output.webhook.retry.initial_backoff_ms",
            raw.initial_backoff_ms,
            default.initial_backoff_ms,
            errors,
        ),
        max_backoff_ms: optional_positive_u64(
            "output.webhook.retry.max_backoff_ms",
            raw.max_backoff_ms,
            default.max_backoff_ms,
            errors,
        ),
        retry_429: raw.retry_429.unwrap_or(default.retry_429),
    }
}

fn parse_webhook_outbox(
    raw: Option<RawWebhookOutboxConfig>,
    errors: &mut Vec<String>,
) -> WebhookOutboxConfig {
    let default = WebhookOutboxConfig::default();
    let Some(raw) = raw else {
        return default;
    };
    let enabled = raw.enabled.unwrap_or(default.enabled);
    if enabled && raw.path.is_none() {
        errors.push("output.webhook.outbox.path: missing required field".to_owned());
    }
    WebhookOutboxConfig {
        enabled,
        path: raw.path,
        max_attempts: optional_positive_usize(
            "output.webhook.outbox.max_attempts",
            raw.max_attempts,
            default.max_attempts,
            errors,
        ),
    }
}

fn required_non_empty(
    field: &str,
    value: Option<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    match value {
        Some(value) => {
            let value = value.trim();
            if value.is_empty() {
                errors.push(format!("{field}: must not be empty"));
                None
            } else {
                Some(value.to_owned())
            }
        }
        None => {
            errors.push(format!("{field}: missing required field"));
            None
        }
    }
}

fn optional_non_empty(
    field: &str,
    value: Option<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            errors.push(format!("{field}: must not be empty"));
            None
        } else {
            Some(value.to_owned())
        }
    })
}

fn optional_positive_u64(
    field: &str,
    value: Option<u64>,
    default: u64,
    errors: &mut Vec<String>,
) -> u64 {
    match value {
        Some(0) => {
            errors.push(format!("{field}: must be greater than 0"));
            default
        }
        Some(value) => value,
        None => default,
    }
}

fn optional_positive_usize(
    field: &str,
    value: Option<usize>,
    default: usize,
    errors: &mut Vec<String>,
) -> usize {
    match value {
        Some(0) => {
            errors.push(format!("{field}: must be greater than 0"));
            default
        }
        Some(value) => value,
        None => default,
    }
}

fn is_secret_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("x-api-key")
        || name.eq_ignore_ascii_case("api-key")
}
