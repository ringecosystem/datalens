use datalens_provider::Purpose;
use prometheus::{Encoder, TextEncoder};
use serde_json::json;

mod common;

use common::{gateway, spawn};

#[tokio::test]
async fn batch_items_increment_calls_total_individually() {
    let (url, calls) = spawn(vec![]).await;
    let (gateway, registry) = gateway(url, 20, 20, None, 64, 16);
    let results = gateway
        .batch(
            vec![
                ("eth_getBlockByNumber".to_owned(), json!(["0x1", false])),
                ("eth_getBlockByNumber".to_owned(), json!(["0x2", false])),
                ("eth_getBlockByNumber".to_owned(), json!(["0x3", false])),
            ],
            Purpose::Follow,
            Some("ormp"),
        )
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![Ok(json!("0x1")), Ok(json!("0x1")), Ok(json!("0x1"))]
    );
    assert_eq!(calls.calls.lock().unwrap().len(), 1);
    let body = calls.calls.lock().unwrap()[0].clone();
    assert_eq!(body.as_array().unwrap().len(), 3);

    let mut encoded = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut encoded)
        .unwrap();
    let text = String::from_utf8(encoded).unwrap();
    assert!(
        text.contains(
            "datalens_provider_calls_total{application=\"ormp\",chain=\"arbitrum\",method=\"eth_getBlockByNumber\",outcome=\"ok\",purpose=\"follow\"} 3"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "datalens_provider_call_duration_seconds_count{chain=\"arbitrum\",method=\"eth_getBlockByNumber\"} 1"
        ),
        "{text}"
    );
    assert!(
        text.contains("datalens_provider_budget_tokens{chain=\"arbitrum\"} 19"),
        "{text}"
    );
}
