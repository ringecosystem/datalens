//! A raw HTTP server that answers request `n` with a scripted reply.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datalens_sdk::{ChainName, Client, ClientConfig, RetryConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Answers request `n` (from 0) with `reply(n)`, one request per connection.
pub async fn fake(reply: fn(usize) -> String) -> (String, Arc<AtomicUsize>) {
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

pub fn http(status: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}

pub fn client(base: &str) -> Client {
    let mut config = ClientConfig::new(base, "app", "token");
    config.retry = RetryConfig {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
    };
    Client::new(config).unwrap()
}

pub fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}
