use std::time::Duration;

/// A phrase has to name an oversized query. A bare "range" or "limit" is not enough:
/// that classification used to split every not-ready head down to one block.
const LIMIT_PHRASES: &[&str] = &[
    "query returned more than",
    "block range too large",
    "exceed maximum block range",
    "range is too large",
    "block range is too wide",
    "eth_getlogs is limited to",
    "maximum block range",
    "response size exceeded",
    "log size exceeded",
    "exceeds max results",
    "not supported on free plan",
];

const BATCH_PHRASES: &[&str] = &["batch limit", "batch size"];

const QUOTA_PHRASES: &[&str] = &[
    "capacity",
    "quota",
    "daily request",
    "compute units",
    "credits",
];

const NOT_READY_PHRASES: &[&str] = &["header not found", "block not available", "unknown block"];

const RATE_PHRASES: &[&str] = &[
    "rate limit",
    "rate-limit",
    "too many requests",
    "requests per second",
];

const BACKOFF_MS: u64 = 100;

/// In-call sleep cap. A longer `Retry-After` is returned to the caller and
/// still pauses every other caller on the chain.
pub(crate) const RETRY_AFTER_CAP: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProviderError {
    #[error("provider result is too large")]
    Limit,
    #[error("provider block is not ready")]
    NotReady,
    #[error("provider rate limited")]
    RateLimited { retry_after: Duration },
    #[error("transient provider failure")]
    Transient { status: Option<u16> },
    #[error("fatal provider failure: {message}")]
    Fatal { message: String },
    #[error("provider daily budget exhausted")]
    BudgetExhausted,
    #[error("provider token was not available before the deadline")]
    Throttled,
    #[error("provider batch is too large")]
    BatchTooLarge,
}

impl ProviderError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::NotReady => "not_ready",
            Self::RateLimited { .. } => "rate_limited",
            Self::Transient { .. } => "transient",
            Self::Fatal { .. } => "fatal",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Throttled => "throttled",
            Self::BatchTooLarge => "batch_too_large",
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        let message = message.into();
        let message = if message.chars().count() > 180 {
            let mut shortened: String = message.chars().take(180).collect();
            shortened.push_str("...");
            shortened
        } else {
            message
        };
        Self::Fatal { message }
    }

    pub(crate) fn retryable(&self) -> bool {
        match self {
            Self::RateLimited { retry_after } => *retry_after <= RETRY_AFTER_CAP,
            Self::Transient { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn retry_delay(&self, failed_attempt: u32) -> Duration {
        if let Self::RateLimited { retry_after } = self
            && !retry_after.is_zero()
        {
            return *retry_after;
        }
        Duration::from_millis(BACKOFF_MS.saturating_mul(1_u64 << failed_attempt.saturating_sub(1)))
    }
}

/// Classifies a JSON-RPC error object. Rate phrases win over size phrases,
/// because "rate limit exceeded" is not an oversized `eth_getLogs`.
pub fn classify_rpc_message(code: i64, message: &str) -> ProviderError {
    let lower = message.to_ascii_lowercase();
    if NOT_READY_PHRASES
        .iter()
        .any(|phrase| lower.contains(phrase))
    {
        return ProviderError::NotReady;
    }
    if RATE_PHRASES.iter().any(|phrase| lower.contains(phrase)) || code == 429 {
        return ProviderError::RateLimited {
            retry_after: Duration::ZERO,
        };
    }
    if BATCH_PHRASES.iter().any(|phrase| lower.contains(phrase)) {
        return ProviderError::BatchTooLarge;
    }
    if QUOTA_PHRASES.iter().any(|phrase| lower.contains(phrase)) {
        return ProviderError::RateLimited {
            retry_after: Duration::ZERO,
        };
    }
    if LIMIT_PHRASES.iter().any(|phrase| lower.contains(phrase)) {
        return ProviderError::Limit;
    }
    if code == -32_005 {
        return ProviderError::RateLimited {
            retry_after: Duration::ZERO,
        };
    }
    ProviderError::fatal(message)
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("provider url must be http or https")]
    BadUrl,
    #[error("min_range_blocks {min} exceeds max_get_logs_range_blocks {max}")]
    Range { min: u64, max: u64 },
    #[error("http client: {0}")]
    Client(String),
}

pub(crate) fn parse_retry_after(value: &str) -> Duration {
    let text = value.trim();
    if let Ok(seconds) = text.parse::<u64>() {
        return Duration::from_secs(seconds);
    }
    http_date_delay(text).unwrap_or(RETRY_AFTER_CAP)
}

fn http_date_delay(value: &str) -> Option<Duration> {
    let parsed = chrono::NaiveDateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S GMT").ok()?;
    let target = parsed.and_utc();
    let delta = target.signed_duration_since(chrono::Utc::now());
    let millis = delta.num_milliseconds();
    if millis <= 0 {
        return Some(Duration::ZERO);
    }
    u64::try_from(millis).ok().map(Duration::from_millis)
}
