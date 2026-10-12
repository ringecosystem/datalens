use std::time::Duration;

use datalens_provider::{ProviderError, classify_rpc_message};

#[test]
fn size_phrases_are_limit_and_bare_words_are_not() {
    let limits = [
        "query returned more than 10000 results",
        "Block range too large",
        "exceed maximum block range of 1000",
        "the range is too large",
        "block range is too wide",
        "eth_getLogs is limited to a 10,000 range",
        "maximum block range is 10000",
        "response size exceeded",
        "log size exceeded",
        "query exceeds max results 20000, retry with a smaller range",
    ];
    for message in limits {
        assert_eq!(
            classify_rpc_message(-32_000, message),
            ProviderError::Limit,
            "{message}"
        );
        assert_eq!(
            classify_rpc_message(-32_005, message),
            ProviderError::Limit,
            "{message} with -32005"
        );
    }
    for message in [
        "range",
        "limit",
        "limit exceeded",
        "more than",
        "too many logs in the response",
    ] {
        assert!(
            matches!(
                classify_rpc_message(-32_000, message),
                ProviderError::Fatal { .. }
            ),
            "{message}"
        );
    }
}

#[test]
fn not_ready_and_rate_messages_stay_out_of_limit() {
    for message in ["header not found", "block not available", "unknown block"] {
        assert_eq!(
            classify_rpc_message(-32_000, message),
            ProviderError::NotReady,
            "{message}"
        );
    }
    assert_eq!(
        classify_rpc_message(-32_005, "too many requests"),
        ProviderError::RateLimited {
            retry_after: Duration::ZERO
        }
    );
    assert_eq!(
        classify_rpc_message(-32_005, "rate limit exceeded"),
        ProviderError::RateLimited {
            retry_after: Duration::ZERO
        }
    );
    assert_eq!(
        classify_rpc_message(-32_005, "upstream busy"),
        ProviderError::RateLimited {
            retry_after: Duration::ZERO
        }
    );
    assert_eq!(
        classify_rpc_message(429, "slow down"),
        ProviderError::RateLimited {
            retry_after: Duration::ZERO
        }
    );
}

#[test]
fn quota_and_batch_phrases_do_not_split_a_range() {
    for message in [
        "Monthly capacity limit exceeded.",
        "daily request limit exceeded",
        "compute units limit exceeded",
        "credits exhausted",
        "quota exceeded",
    ] {
        assert_eq!(
            classify_rpc_message(-32_000, message),
            ProviderError::RateLimited {
                retry_after: Duration::ZERO
            },
            "{message}"
        );
    }
    for message in ["batch limit exceeded", "batch size is too large"] {
        assert_eq!(
            classify_rpc_message(-32_000, message),
            ProviderError::BatchTooLarge,
            "{message}"
        );
    }
}
