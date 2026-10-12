use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use datalens_server::http::ops_router;
use sqlx::PgPool;
use tower::ServiceExt;

async fn get(pool: &PgPool, path: &str) -> (StatusCode, String) {
    let response = ops_router(pool.clone())
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[sqlx::test(migrations = false)]
async fn health_and_ready_when_database_is_up(pool: PgPool) {
    assert_eq!(
        get(&pool, "/health").await,
        (StatusCode::OK, r#"{"status":"ok"}"#.to_owned())
    );
    assert_eq!(
        get(&pool, "/ready").await,
        (StatusCode::OK, r#"{"status":"ready"}"#.to_owned())
    );
}

#[sqlx::test(migrations = false)]
async fn ready_fails_and_health_stays_ok_when_database_is_gone(pool: PgPool) {
    pool.close().await;
    assert_eq!(
        get(&pool, "/ready").await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"status":"unavailable"}"#.to_owned()
        )
    );
    assert_eq!(
        get(&pool, "/health").await,
        (StatusCode::OK, r#"{"status":"ok"}"#.to_owned())
    );
}
