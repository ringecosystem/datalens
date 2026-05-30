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
    LongRunJobSummary, MSGPORT_ADDRESS, ORMP_ADDRESS, ORMP_START_BLOCK, OrmpConfig,
    build_job_query_request, build_query_request, parse_plan, query_with_client,
    run_plan_with_client, summarize_job_result, summarize_response,
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
fn test_parse_plan_reads_multi_chain_jobs() {
    let plan = parse_plan(
        br#"{
            "jobs": [{
                "label": "polygon-ormp-window",
                "chain": "polygon",
                "chain_id": 137,
                "from_block": 58700000,
                "to_block": 58700100,
                "addresses": [
                    "0x0000000000000000000000000000000000000001",
                    "0x0000000000000000000000000000000000000002"
                ]
            }]
        }"#,
    )
    .expect("plan");

    assert_eq!(plan.jobs.len(), 1);
    let job = &plan.jobs[0];
    assert_eq!(job.label, "polygon-ormp-window");
    assert_eq!(job.chain, "polygon");
    assert_eq!(job.chain_id, 137);
    assert_eq!(job.from_block, 58700000);
    assert_eq!(job.to_block, 58700100);
    assert_eq!(
        job.addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x0000000000000000000000000000000000000002".to_owned()
        ]
    );
}

#[test]
fn test_build_job_query_request_uses_non_ethereum_chain_identity() {
    let plan = parse_plan(
        br#"{
            "jobs": [{
                "label": "polygon-ormp-window",
                "chain": "polygon",
                "chain_id": 137,
                "from_block": 58700000,
                "to_block": 58700100,
                "addresses": ["0x0000000000000000000000000000000000000001"]
            }]
        }"#,
    )
    .expect("plan");

    let request = build_job_query_request(&plan.jobs[0]).expect("request");

    assert_eq!(request.dataset_key, DatasetKey::evm_logs());
    assert_eq!(request.chain.family(), ChainFamily::Evm);
    assert_eq!(request.chain.configured_name(), "polygon");
    assert_eq!(request.chain.network_id(), Some(&NetworkId::numeric(137)));
    assert_eq!(
        request.range,
        LedgerRange::blocks(58700000, 58700100).expect("range")
    );
    assert_eq!(request.finality, QueryFinalityRequirement::DurableOnly);
    assert_eq!(
        request.selector,
        QuerySelector::EvmLogs(datalens_core::LogFilter {
            addresses: vec!["0x0000000000000000000000000000000000000001".to_owned()],
            topics: Vec::new(),
        })
    );
}

#[test]
fn test_summarize_job_result_reports_jsonl_fields() {
    let plan = parse_plan(
        br#"{
            "jobs": [{
                "label": "polygon-ormp-window",
                "chain": "polygon",
                "chain_id": 137,
                "from_block": 58700000,
                "to_block": 58700100,
                "addresses": ["0x0000000000000000000000000000000000000001"]
            }]
        }"#,
    )
    .expect("plan");
    let response = serde_json::from_value(response_json_for_chain(
        "polygon",
        137,
        58700000,
        58700100,
        serde_json::json!({
            "hit_ranges": [{ "kind": "block", "start": 58700000, "end": 58700049 }],
            "missing_ranges": [{ "kind": "block", "start": 58700050, "end": 58700100 }],
            "durable_hit_ranges": [{ "kind": "block", "start": 58700000, "end": 58700049 }],
            "provider_fill_ranges": [{ "kind": "block", "start": 58700050, "end": 58700100 }]
        }),
        vec![
            log_at(58700007, "0x0000000000000000000000000000000000000001"),
            log_at(58700099, "0x0000000000000000000000000000000000000001"),
        ],
    ))
    .expect("response");

    let summary = summarize_job_result(&plan.jobs[0], &response, 42).expect("summary");

    assert_eq!(
        summary,
        LongRunJobSummary {
            label: "polygon-ormp-window".to_owned(),
            chain: "polygon".to_owned(),
            chain_id: 137,
            range: datalens_example_ormp::RangeSummary::Block {
                start: 58700000,
                end: 58700100
            },
            elapsed_ms: 42,
            row_count: 2,
            hit_ranges: vec![datalens_example_ormp::RangeSummary::Block {
                start: 58700000,
                end: 58700049
            }],
            missing_ranges: vec![datalens_example_ormp::RangeSummary::Block {
                start: 58700050,
                end: 58700100
            }],
            durable_hit_ranges: vec![datalens_example_ormp::RangeSummary::Block {
                start: 58700000,
                end: 58700049
            }],
            provider_fill_ranges: vec![datalens_example_ormp::RangeSummary::Block {
                start: 58700050,
                end: 58700100
            }],
            full_durable_cache_hit: false,
            first_log_block: Some(58700007),
            last_log_block: Some(58700099),
        }
    );
    assert_eq!(
        serde_json::to_string(&summary).expect("json"),
        r#"{"label":"polygon-ormp-window","chain":"polygon","chain_id":137,"range":{"kind":"block","start":58700000,"end":58700100},"elapsed_ms":42,"row_count":2,"hit_ranges":[{"kind":"block","start":58700000,"end":58700049}],"missing_ranges":[{"kind":"block","start":58700050,"end":58700100}],"durable_hit_ranges":[{"kind":"block","start":58700000,"end":58700049}],"provider_fill_ranges":[{"kind":"block","start":58700050,"end":58700100}],"full_durable_cache_hit":false,"first_log_block":58700007,"last_log_block":58700099}"#
    );
}

#[test]
fn test_run_plan_with_client_emits_error_record_and_continues() {
    let plan = parse_plan(
        br#"{
            "jobs": [
                {
                    "label": "provider-limit",
                    "chain": "ethereum",
                    "chain_id": 1,
                    "from_block": 20009590,
                    "to_block": 20019589,
                    "addresses": ["0x0000000000000000000000000000000000000001"]
                },
                {
                    "label": "cache-hit",
                    "chain": "ethereum",
                    "chain_id": 1,
                    "from_block": 20009590,
                    "to_block": 20009591,
                    "addresses": ["0x0000000000000000000000000000000000000001"]
                }
            ]
        }"#,
    )
    .expect("plan");
    let transport = RecordingTransport::with_responses(vec![
        HttpResponse::json(
            429,
            serde_json::json!({
                "error": {
                    "kind": "ProviderLimit",
                    "message": "query block range exceeds server limit, narrow your filter: 1000"
                }
            }),
        ),
        HttpResponse::json(
            200,
            response_json(
                serde_json::json!({
                    "hit_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
                    "missing_ranges": [],
                    "durable_hit_ranges": [{ "kind": "block", "start": 20009590, "end": 20009591 }],
                    "provider_fill_ranges": []
                }),
                vec![log(20009590, MSGPORT_ADDRESS)],
            ),
        ),
    ]);
    let client = DatalensClient::with_transport(
        DatalensClientConfig {
            endpoint: "http://datalens.invalid".to_owned(),
            application: Some("public".to_owned()),
            bearer_token: Some("public-token".to_owned()),
        },
        transport,
    )
    .expect("client");

    let mut output = Vec::new();
    run_plan_with_client(&client, &plan, &mut output).expect("plan run");
    let records = String::from_utf8(output).expect("utf8");
    let mut lines = records.lines();
    let first: serde_json::Value =
        serde_json::from_str(lines.next().expect("first")).expect("json");
    let second: serde_json::Value =
        serde_json::from_str(lines.next().expect("second")).expect("json");

    assert_eq!(first["status"], "error");
    assert_eq!(first["label"], "provider-limit");
    assert!(
        first["error"]
            .as_str()
            .expect("error")
            .contains("ProviderLimit")
    );

    assert_eq!(second["status"], "ok");
    assert_eq!(second["label"], "cache-hit");
    assert_eq!(second["full_durable_cache_hit"], true);
    assert!(lines.next().is_none());
}

#[test]
fn test_run_plan_with_client_reports_invalid_range_without_panic() {
    let plan = parse_plan(
        br#"{
            "jobs": [{
                "label": "invalid-range",
                "chain": "ethereum",
                "chain_id": 1,
                "from_block": 20009591,
                "to_block": 20009590,
                "addresses": ["0x0000000000000000000000000000000000000001"]
            }]
        }"#,
    )
    .expect("plan");
    let transport = RecordingTransport::new(HttpResponse::json(200, serde_json::json!({})));
    let client = DatalensClient::with_transport(
        DatalensClientConfig {
            endpoint: "http://datalens.invalid".to_owned(),
            application: Some("public".to_owned()),
            bearer_token: Some("public-token".to_owned()),
        },
        transport.clone(),
    )
    .expect("client");

    let mut output = Vec::new();
    run_plan_with_client(&client, &plan, &mut output).expect("plan run");
    let record: serde_json::Value = serde_json::from_slice(&output).expect("invalid range record");

    assert_eq!(record["status"], "error");
    assert_eq!(record["label"], "invalid-range");
    assert_eq!(
        record["range"],
        serde_json::json!({
            "kind": "block",
            "start": 20009591,
            "end": 20009590,
        })
    );
    assert!(
        record["error"]
            .as_str()
            .expect("error")
            .contains("from_block must be less than or equal to to_block")
    );
    assert!(transport.requests.lock().expect("requests lock").is_empty());
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
fn test_config_from_pairs_uses_smoke_defaults_and_optional_token() {
    let config = OrmpConfig::from_pairs([
        ("ORMP_TO_BLOCK", "20009600"),
        ("DATALENS_PUBLIC_APP_TOKEN", " public-token "),
    ])
    .expect("config");

    assert_eq!(config.endpoint, "http://127.0.0.1:3000");
    assert_eq!(config.application, "public");
    assert_eq!(config.bearer_token, Some("public-token".to_owned()));
    assert_eq!(config.from_block, ORMP_START_BLOCK);
    assert_eq!(config.to_block, 20009600);
}

#[test]
fn test_config_from_pairs_allows_explicit_values() {
    let config = OrmpConfig::from_pairs([
        ("DATALENS_ENDPOINT", " http://datalens.invalid "),
        ("DATALENS_APPLICATION", " smoke "),
        ("ORMP_FROM_BLOCK", "20009595"),
        ("ORMP_TO_BLOCK", "20009600"),
    ])
    .expect("config");

    assert_eq!(config.endpoint, "http://datalens.invalid");
    assert_eq!(config.application, "smoke");
    assert_eq!(config.bearer_token, None);
    assert_eq!(config.from_block, 20009595);
    assert_eq!(config.to_block, 20009600);
}

#[test]
fn test_config_from_pairs_requires_to_block() {
    let error = OrmpConfig::from_pairs([]).expect_err("missing to block");

    assert_eq!(
        error.to_string(),
        "missing required environment variable ORMP_TO_BLOCK"
    );
}

#[test]
fn test_cli_help_exits_successfully_without_env() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_datalens-example-ormp"))
        .arg("--help")
        .env_clear()
        .output()
        .expect("help command");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout");
    assert!(stdout.contains("ORMP_TO_BLOCK"));
    assert!(stdout.contains("http://127.0.0.1:3000"));
    assert!(stdout.contains("20009590"));
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
    response_json_for_chain("ethereum", 1, 20009590, 20009591, cache, rows)
}

fn response_json_for_chain(
    configured_name: &str,
    chain_id: u64,
    start: u64,
    end: u64,
    cache: serde_json::Value,
    rows: Vec<LogRecord>,
) -> serde_json::Value {
    serde_json::json!({
        "chain": {
            "family": "Evm",
            "configured_name": configured_name,
            "network_id": { "kind": "numeric", "value": chain_id }
        },
        "dataset_key": "evm.logs",
        "range": { "kind": "block", "start": start, "end": end },
        "cache": cache,
        "rows": serde_json::to_value(DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(rows),
        ).expect("dataset rows")).expect("dataset rows json")
    })
}

fn log(block_number: u64, address: &str) -> LogRecord {
    log_at(block_number, address)
}

fn log_at(block_number: u64, address: &str) -> LogRecord {
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
    responses: Arc<Mutex<Vec<HttpResponse>>>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl RecordingTransport {
    fn new(response: HttpResponse) -> Self {
        Self::with_responses(vec![response])
    }

    fn with_responses(responses: Vec<HttpResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses)),
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
        let mut responses = self.responses.lock().expect("responses lock");
        assert!(!responses.is_empty());
        Ok(responses.remove(0))
    }
}
