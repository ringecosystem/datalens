use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// One scripted JSON-RPC item. The server wraps it with `id` and `jsonrpc`.
#[derive(Debug, Clone)]
pub enum ItemOutcome {
    Result(Value),
    Error { code: i64, message: String },
}

struct Fault {
    status: u16,
    retry_after: Option<String>,
}

type Handler = Arc<dyn Fn(&Value) -> ItemOutcome + Send + Sync>;

struct MockState {
    calls: Mutex<Vec<Value>>,
    handler: Mutex<Handler>,
    faults: Mutex<VecDeque<Fault>>,
}

/// Local JSON-RPC server. Drop aborts the listener.
pub struct MockRpc {
    url: String,
    state: Arc<MockState>,
    task: tokio::task::JoinHandle<()>,
}

impl MockRpc {
    pub async fn spawn() -> Self {
        let state = Arc::new(MockState {
            calls: Mutex::new(Vec::new()),
            handler: Mutex::new(Arc::new(default_handler)),
            faults: Mutex::new(VecDeque::new()),
        });
        let app = Router::new()
            .route("/", post(handle))
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock rpc");
        let port = listener.local_addr().expect("mock rpc port").port();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            url: format!("http://127.0.0.1:{port}"),
            state,
            task,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn calls(&self) -> Vec<Value> {
        lock(&self.state.calls).clone()
    }

    pub fn set_handler<F>(&self, handler: F)
    where
        F: Fn(&Value) -> ItemOutcome + Send + Sync + 'static,
    {
        *lock(&self.state.handler) = Arc::new(handler);
    }

    /// The next `count` HTTP requests return `status` and do not run the handler.
    pub fn fail_next_http(&self, count: usize, status: u16, retry_after: Option<String>) {
        let mut faults = lock(&self.state.faults);
        for _ in 0..count {
            faults.push_back(Fault {
                status,
                retry_after: retry_after.clone(),
            });
        }
    }
}

impl Drop for MockRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn default_handler(item: &Value) -> ItemOutcome {
    match item.get("method").and_then(Value::as_str) {
        Some("eth_getLogs") => ItemOutcome::Result(json!([])),
        _ => ItemOutcome::Result(Value::Null),
    }
}

async fn handle(State(state): State<Arc<MockState>>, Json(body): Json<Value>) -> impl IntoResponse {
    lock(&state.calls).push(body.clone());
    if let Some(fault) = lock(&state.faults).pop_front() {
        let mut response = (
            StatusCode::from_u16(fault.status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(json!({"error": "injected"})),
        )
            .into_response();
        if let Some(retry_after) = fault.retry_after
            && let Ok(value) = HeaderValue::from_str(&retry_after)
        {
            response.headers_mut().insert("retry-after", value);
        }
        return response;
    }
    let handler = Arc::clone(&lock(&state.handler));
    if let Some(items) = body.as_array() {
        let results: Vec<Value> = items
            .iter()
            .map(|item| reply_item(item, handler.as_ref()))
            .collect();
        (StatusCode::OK, Json(Value::Array(results))).into_response()
    } else {
        (StatusCode::OK, Json(reply_item(&body, handler.as_ref()))).into_response()
    }
}

fn reply_item(item: &Value, handler: &dyn Fn(&Value) -> ItemOutcome) -> Value {
    let id = item.get("id").cloned().unwrap_or(Value::from(1));
    match handler(item) {
        ItemOutcome::Result(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        ItemOutcome::Error { code, message } => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message}
        }),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
