use std::time::Duration;

use datalens_provider::{ProviderError, classify_rpc_message};

#[test]
fn size_phrases_are_limit_and_bare_words_are_not() {
    let limits = [
        "query returned more than 10000 results",
        "Block range too large",
        "exceed maximum block range of 1000",
        "the range is too large",
        "limit exceeded",
        "response size exceeded",
        "log size exceeded",
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
