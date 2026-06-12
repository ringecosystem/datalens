use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    thread,
};

use datalens_core::{DatalensErrorKind, LedgerRange};
use datalens_tron::{
    TronContractEventRequest, TronHttpProvider, TronProvider, normalize_tron_contract_address,
};

#[test]
fn test_trongrid_contract_events_request_uses_path_query_auth_and_pagination() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(
        seen.clone(),
        r#"{"data":[],"meta":{"fingerprint":"next-page"}}"#,
        "HTTP/1.1 200 OK",
    );
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid(server.url(), Some("secret-key".to_owned()));

    let page = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: normalize_tron_contract_address(
                "41a614f803b6fd780986a42c78ec9c7f77e6ded13c",
            )
            .expect("address"),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(83_200_000, 83_200_000).expect("range"),
            only_confirmed: true,
            limit: 50,
            fingerprint: Some("previous-page".to_owned()),
        })
        .expect("contract events");

    assert_eq!(page.next_fingerprint.as_deref(), Some("next-page"));
    assert_eq!(page.provider_calls, 1);
    let request = seen.lock().expect("seen").join("\n");
    assert!(request.starts_with("GET /v1/contracts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/events?"));
    assert!(request.contains("event_name=Transfer"));
    assert!(request.contains("block_number=83200000"));
    assert!(!request.contains("min_block_number="));
    assert!(!request.contains("max_block_number="));
    assert!(request.contains("only_confirmed=true"));
    assert!(request.contains("limit=50"));
    assert!(request.contains("fingerprint=previous-page"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("tron-pro-api-key: secret-key")
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("accept-encoding: identity")
    );
}

#[test]
fn test_trongrid_contract_events_rejects_direct_multi_block_range() {
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid("http://trongrid.invalid", Some("secret-key".to_owned()));

    let error = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: normalize_tron_contract_address(
                "41a614f803b6fd780986a42c78ec9c7f77e6ded13c",
            )
            .expect("address"),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(83_200_000, 83_200_002).expect("range"),
            only_confirmed: true,
            limit: 50,
            fingerprint: None,
        })
        .expect_err("provider should not issue unbounded TronGrid scans");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert!(error.message.contains("single block_number"));
}

#[test]
fn test_trongrid_missing_api_key_disables_contract_events() {
    let provider =
        TronHttpProvider::new("http://unused").with_trongrid("http://trongrid.invalid", None);

    assert!(!provider.supports_contract_event_query());
}

#[test]
fn test_tron_transport_error_redacts_rpc_url_credentials() {
    let address = unused_local_address();
    let url = format!("http://user:password@{address}/tron?token=query-token&secret=query-secret");

    let error = TronHttpProvider::new(url)
        .latest_block(datalens_tron::TronFinality::Latest)
        .expect_err("connect failure");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("http://<redacted>@"));
    assert!(error.message.contains("token=<redacted>"));
    assert!(error.message.contains("secret=<redacted>"));
    assert!(!error.message.contains("user:password"));
    assert!(!error.message.contains("query-token"));
    assert!(!error.message.contains("query-secret"));
}

#[test]
fn test_trongrid_transport_error_redacts_rpc_url_credentials() {
    let address = unused_local_address();
    let url = format!(
        "http://user:password@{address}/trongrid?apikey=query-key&signature=query-signature"
    );
    let provider =
        TronHttpProvider::new("http://unused").with_trongrid(url, Some("secret-key".to_owned()));

    let error = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_owned(),
            event_name: Some("MessageAccepted".to_owned()),
            range: LedgerRange::blocks(123, 123).expect("range"),
            only_confirmed: true,
            limit: 20,
            fingerprint: None,
        })
        .expect_err("connect failure");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("http://<redacted>@"));
    assert!(error.message.contains("apikey=<redacted>"));
    assert!(error.message.contains("signature=<redacted>"));
    assert!(!error.message.contains("user:password"));
    assert!(!error.message.contains("query-key"));
    assert!(!error.message.contains("query-signature"));
}

#[test]
fn test_trongrid_contract_events_response_is_normalized() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(
        seen,
        r#"{
            "data":[{
                "contract_address":"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
                "event_name":"MessageAccepted",
                "event":"MessageAccepted(address,uint256)",
                "transaction_id":"tx-1",
                "block_number":123,
                "block_hash":"block-hash",
                "transaction_index":2,
                "event_index":3,
                "result":{"sender":"alice"},
                "result_type":{"sender":"address"},
                "confirmed":true
            }],
            "meta":{}
        }"#,
        "HTTP/1.1 200 OK",
    );
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid(server.url(), Some("secret-key".to_owned()));

    let page = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_owned(),
            event_name: Some("MessageAccepted".to_owned()),
            range: LedgerRange::blocks(123, 123).expect("range"),
            only_confirmed: true,
            limit: 20,
            fingerprint: None,
        })
        .expect("contract events");

    assert_eq!(page.events.len(), 1);
    assert_eq!(
        page.events[0].contract_address,
        "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
    );
    assert_eq!(
        page.events[0].event_name.as_deref(),
        Some("MessageAccepted")
    );
    assert_eq!(
        page.events[0].event_signature.as_deref(),
        Some("MessageAccepted(address,uint256)")
    );
    assert_eq!(page.events[0].transaction_id.as_deref(), Some("tx-1"));
    assert_eq!(page.events[0].block_number, 123);
    assert!(page.events[0].confirmed);
}

#[test]
fn test_trongrid_contract_events_malformed_response_includes_status_and_body_prefix() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(seen, "not-json", "HTTP/1.1 200 OK");
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid(server.url(), Some("secret-key".to_owned()));

    let error = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: normalize_tron_contract_address(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            )
            .expect("address"),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(10, 10).expect("range"),
            only_confirmed: true,
            limit: 50,
            fingerprint: None,
        })
        .expect_err("malformed response should stay visible");

    assert_eq!(error.kind, DatalensErrorKind::ProviderFailure);
    assert!(error.message.contains("status=200"));
    assert!(error.message.contains("body_prefix=not-json"));
    assert!(!error.message.contains("secret-key"));
}

#[test]
fn test_trongrid_contract_events_invalid_address_response_is_invalid_input() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(
        seen,
        r#"{"success":false,"error":"A valid contract address is required.","statusCode":400}"#,
        "HTTP/1.1 400 Bad Request",
    );
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid(server.url(), Some("secret-key".to_owned()));

    let error = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_owned(),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(123, 123).expect("range"),
            only_confirmed: true,
            limit: 20,
            fingerprint: None,
        })
        .expect_err("invalid address response should be distinguished");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("valid contract address"));
    assert!(!error.message.contains("secret-key"));
}

#[test]
fn test_trongrid_contract_events_page_limit_response_is_provider_limit() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(
        seen,
        r#"{"success":false,"error":"TronGrid contract event page limit 100 reached","statusCode":400}"#,
        "HTTP/1.1 400 Bad Request",
    );
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid(server.url(), Some("secret-key".to_owned()));

    let error = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_owned(),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(123, 123).expect("range"),
            only_confirmed: true,
            limit: 20,
            fingerprint: None,
        })
        .expect_err("page-limit response should be distinguished");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(error.message.contains("page limit"));
    assert!(!error.message.contains("secret-key"));
}

struct TestServer {
    address: String,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn spawn(seen: Arc<Mutex<Vec<String>>>, body: &'static str, status: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind server");
        let address = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut buffer = [0; 4096];
            let read = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            seen.lock().expect("seen").push(request);
            let response = format!(
                "{status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });
        Self {
            address,
            handle: Some(handle),
        }
    }

    fn url(&self) -> String {
        self.address.clone()
    }
}

fn unused_local_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused address");
    listener.local_addr().expect("unused address")
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().expect("server thread");
        }
    }
}
