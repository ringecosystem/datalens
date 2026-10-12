mod fake;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use datalens_sdk::{ApiErrorKind, Error};

use fake::{client, ethereum, fake, http};

const HEADS: &str = r#"{"chain":"ethereum","latest":10,"latest_hash":"0x00000000000000000000000000000000000000000000000000000000000000aa","safe":8,"finalized":4}"#;

#[tokio::test]
async fn server_errors_and_rate_limits_are_retried_until_success() {
    let (base, count) = fake(|n| match n {
        0 => http("503 Service Unavailable", "", "upstream down"),
        1 => http(
            "429 Too Many Requests",
            "retry-after: 1\r\n",
            r#"{"error":{"kind":"quota_exceeded","message":"more than 1 requests per minute"}}"#,
        ),
        _ => http("200 OK", "", HEADS),
    })
    .await;
    let started = Instant::now();
    let heads = client(&base).heads(&ethereum()).await.unwrap();
    assert_eq!((heads.latest, heads.safe, heads.finalized), (10, 8, 4));
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "Retry-After ignored"
    );
}

#[tokio::test]
async fn retries_stop_at_max_attempts_with_the_last_error() {
    let (base, count) = fake(|_| http("502 Bad Gateway", "", "<html>bad gateway</html>")).await;
    let error = client(&base).heads(&ethereum()).await.unwrap_err();
    assert!(
        matches!(
            &error,
            Error::Api { kind: ApiErrorKind::Other(kind), status: 502, message, retry_after: None }
                if kind == "http" && message == "<html>bad gateway</html>"
        ),
        "{error:?}"
    );
    assert!(error.is_retryable());
    assert_eq!(count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn client_errors_are_not_retried() {
    let (base, count) = fake(|_| {
        http(
            "403 Forbidden",
            "",
            r#"{"error":{"kind":"forbidden","message":"application app may not read base"}}"#,
        )
    })
    .await;
    let error = client(&base).heads(&ethereum()).await.unwrap_err();
    assert_eq!(error.kind(), Some(&ApiErrorKind::Forbidden));
    assert_eq!(
        error.to_string(),
        "datalens 403 forbidden: application app may not read base"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_success_body_that_is_not_json_is_a_decode_error() {
    let (base, _) = fake(|_| http("200 OK", "", "not json")).await;
    let error = client(&base).heads(&ethereum()).await.unwrap_err();
    assert!(matches!(error, Error::Decode(_)), "{error:?}");
    assert!(!error.is_retryable());
}
