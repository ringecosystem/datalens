use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use sqlx::PgPool;

const READY_TIMEOUT: Duration = Duration::from_secs(2);

/// Operational routes served by every role.
pub fn ops_router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(pool)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn ready(State(pool): State<PgPool>) -> (StatusCode, Json<Value>) {
    let probe = sqlx::query_scalar::<_, i32>("select 1").fetch_one(&pool);
    match tokio::time::timeout(READY_TIMEOUT, probe).await {
        Ok(Ok(_)) => (StatusCode::OK, Json(json!({ "status": "ready" }))),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable" })),
        ),
    }
}
