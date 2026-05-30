use std::sync::{Arc, Mutex};

use datalens_client::{
    APPLICATION_IDENTITY_HEADER, AUTHORIZATION_HEADER, DatalensClient, DatalensClientConfig,
    HttpRequest, HttpResponse, HttpTransport, QuerySelector,
};
use datalens_core::{
    ChainFamily, DatasetKey, DatasetRows, LedgerRange, LogRecord, NetworkId,
    QueryFinalityRequirement, QueryRows,
};
use datalens_example_ormp::{
    MSGPORT_ADDRESS, ORMP_ADDRESS, OrmpConfig, build_query_request, query_with_client,
    summarize_response,
};

#[test]
fn test_build_query_request_uses_native_evm_logs_contract() {
    let request = build_query_request(20009590, 20010589).expect("request");

    assert_eq!(request.dataset_key, DatasetKey::evm_logs());
    assert_eq!(request.chain.family(), ChainFamily::Evm);
    assert_eq!(request.chain.configured_name(), "ethereum");
    assert_eq!(request.chain.network_id(), Some(&NetworkId::numeric(1)));
    assert_eq!(
        request.range,
        LedgerRange::blocks(20009590, 20010589).expect("range")
    );
    assert_eq!(request.finality, QueryFinalityRequirement::DurableOnly);
    assert_eq!(
        request.selector,
        QuerySelector::EvmLogs(datalens_core::LogFilter {
            addresses: vec![MSGPORT_ADDRESS.to_owned(), ORMP_ADDRESS.to_owned()],
            topics: Vec::new(),
        })
    );
}

#[test]
fn test_query_with_client_passes_application_auth_headers() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        response_json(
            serde_json::json!({
                "hit_ranges": [],
                "missing_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
                "durable_hit_ranges": [],
                "provider_fill_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }]
            }),
            Vec::new(),
        ),
    ));
    let client = DatalensClient::with_transport(
        DatalensClientConfig {
            endpoint: "http://datalens.invalid/".to_owned(),
            application: Some("public".to_owned()),
            bearer_token: Some(" public-token ".to_owned()),
        },
        transport.clone(),
    )
    .expect("client");
    let config = OrmpConfig {
        endpoint: "http://datalens.invalid".to_owned(),
        application: "public".to_owned(),
        bearer_token: Some("public-token".to_owned()),
        from_block: 20009590,
        to_block: 20009591,
    };

    query_with_client(&client, &config).expect("query");

    let request = transport.only_request();
    assert_eq!(request.header(APPLICATION_IDENTITY_HEADER), Some("public"));
    assert_eq!(
        request.header(AUTHORIZATION_HEADER),
        Some("Bearer public-token")
    );
    assert_eq!(request.path, "/v1/query");
}

#[test]
fn test_summarize_response_reports_miss_fill_ranges_and_log_bounds() {
    let response = serde_json::from_value(response_json(
        serde_json::json!({
            "hit_ranges": [],
            "missing_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
            "durable_hit_ranges": [],
            "provider_fill_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
            "segments": [{
                "range": { "kind": "block", "start": 20009590, "end": 20009591 },
                "source": "provider",
                "finality": "safe"
            }]
        }),
        vec![log(20009591, ORMP_ADDRESS), log(20009590, MSGPORT_ADDRESS)],
    ))
    .expect("response");

    let summary = summarize_response(&response).expect("summary");

    assert_eq!(
        serde_json::to_value(summary).expect("summary json"),
        serde_json::json!({
            "requested_range": { "kind": "block", "start": 20009590, "end": 20009591 },
            "row_count": 2,
            "hit_ranges": [],
            "missing_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
            "durable_hit_ranges": [],
            "provider_fill_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
            "first_log_block": 20009590,
            "last_log_block": 20009591,
            "contract_addresses": [MSGPORT_ADDRESS, ORMP_ADDRESS],
            "full_durable_cache_hit": false
        })
    );
}

#[test]
fn test_summarize_response_reports_full_durable_cache_hit() {
    let response = serde_json::from_value(response_json(
        serde_json::json!({
            "hit_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
            "missing_ranges": [],
            "durable_hit_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
            "provider_fill_ranges": [],
            "segments": [{
                "range": { "kind": "block", "start": 20009590, "end": 20009591 },
                "source": "durable",
                "finality": "finalized"
            }]
        }),
        vec![log(20009590, MSGPORT_ADDRESS)],
    ))
    .expect("response");

    let summary = summarize_response(&response).expect("summary");

    assert!(summary.full_durable_cache_hit);
}

fn response_json(cache: serde_json::Value, rows: Vec<LogRecord>) -> serde_json::Value {
    serde_json::json!({
        "chain": {
            "family": "Evm",
            "configured_name": "ethereum",
            "network_id": { "kind": "numeric", "value": 1 }
        },
        "dataset_key": "evm.logs",
        "range": { "kind": "block", "start": 20009590, "end": 20009591 },
        "cache": cache,
        "rows": serde_json::to_value(DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(rows),
        ).expect("dataset rows")).expect("dataset rows json")
    })
}

fn log(block_number: u64, address: &str) -> LogRecord {
    LogRecord::try_new(
        block_number,
        format!("0x{block_number:064x}"),
        format!("0x{:064x}", block_number + 1),
        0,
        block_number - 20009590,
        address,
        Vec::new(),
        "0x".to_owned(),
        false,
    )
    .expect("log")
}

#[derive(Clone)]
struct RecordingTransport {
    response: HttpResponse,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl RecordingTransport {
    fn new(response: HttpResponse) -> Self {
        Self {
            response,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn only_request(&self) -> HttpRequest {
        let requests = self.requests.lock().expect("requests lock");
        assert_eq!(requests.len(), 1);
        requests[0].clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, datalens_client::ClientError> {
        self.requests.lock().expect("requests lock").push(request);
        Ok(self.response.clone())
    }
}
