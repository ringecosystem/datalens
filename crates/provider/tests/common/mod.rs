#![allow(dead_code)]

use std::collections::VecDeque;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use datalens_core::ChainName;
use datalens_provider::{Gateway, ProviderConfig, ProviderMetrics};
use prometheus::Registry;
use serde_json::Value;
use tokio::net::TcpListener;

pub struct Reply {
    pub status: u16,
    pub retry_after: Option<String>,
    pub body: Value,
}

pub struct Script {
    replies: Mutex<VecDeque<Reply>>,
    pub calls: Mutex<Vec<Value>>,
}

pub async fn spawn(replies: Vec<Reply>) -> (String, Arc<Script>) {
    let state = Arc::new(Script {
        replies: Mutex::new(VecDeque::from(replies)),
        calls: Mutex::new(Vec::new()),
    });
    let app = Router::new()
        .route("/", post(handle))
        .with_state(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://127.0.0.1:{port}"), state)
}

async fn handle(State(state): State<Arc<Script>>, body: Bytes) -> impl IntoResponse {
    let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state
        .calls
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(value.clone());
    let reply = state
        .replies
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .pop_front();
    let Some(reply) = reply else {
        if let Some(items) = value.as_array() {
            let results = items
                .iter()
                .map(|item| {
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": item.get("id").cloned().unwrap_or(serde_json::Value::Null),
                        "result": "0x1"
                    })
                })
                .collect::<Vec<_>>();
            return (StatusCode::OK, Json(serde_json::Value::Array(results))).into_response();
        }
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": "0x1"
            })),
        )
            .into_response();
    };
    let mut response = (
        StatusCode::from_u16(reply.status).unwrap(),
        Json(reply.body),
    )
        .into_response();
    if let Some(retry_after) = reply.retry_after {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_str(&retry_after).unwrap());
    }
    response
}

pub fn gateway(
    url: String,
    rps: u32,
    burst: u32,
    daily_calls: Option<u64>,
    max_range: u64,
    min_range: u64,
) -> (Gateway, Registry) {
    let registry = Registry::new();
    let metrics = ProviderMetrics::register(&registry).unwrap();
    let gateway = Gateway::new(
        ChainName::new("arbitrum").unwrap(),
        ProviderConfig {
            url,
            rps: NonZeroU32::new(rps).unwrap(),
            burst: NonZeroU32::new(burst).unwrap(),
            daily_calls: daily_calls.and_then(NonZeroU64::new),
            max_in_flight: NonZeroU32::new(4).unwrap(),
            min_range_blocks: NonZeroU64::new(min_range).unwrap(),
            max_get_logs_range_blocks: NonZeroU64::new(max_range).unwrap(),
        },
        metrics,
    )
    .unwrap();
    (gateway, registry)
}

pub fn rpc_ok(result: Value) -> Reply {
    Reply {
        status: 200,
        retry_after: None,
        body: serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result}),
    }
}

pub fn rpc_error(code: i64, message: &str) -> Reply {
    Reply {
        status: 200,
        retry_after: None,
        body: serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": code, "message": message}
        }),
    }
}
