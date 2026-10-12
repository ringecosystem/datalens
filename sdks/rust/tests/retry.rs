use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use datalens_sdk::{ApiErrorKind, ChainName, Client, ClientConfig, Error, RetryConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const HEADS: &str = r#"{"chain":"ethereum","latest":10,"latest_hash":"0x00000000000000000000000000000000000000000000000000000000000000aa","safe":8,"finalized":4}"#;

/// Answers request `n` (from 0) with `reply(n)`, one request per connection.
async fn fake(reply: fn(usize) -> String) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let n = seen.fetch_add(1, Ordering::SeqCst);
            let mut request = Vec::new();
            let mut buf = [0_u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let read = socket.read(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
            }
            socket.write_all(reply(n).as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
    });
    (base, count)
}

fn http(status: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}

fn client(base: &str) -> Client {
    let mut config = ClientConfig::new(base, "app", "token");
    config.retry = RetryConfig {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
    };
    Client::new(config).unwrap()
}

fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

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
