use std::{
    io::{Read, Write},
    net::TcpListener,
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
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            )
            .expect("address"),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(10, 12).expect("range"),
            only_confirmed: true,
            limit: 50,
            fingerprint: Some("previous-page".to_owned()),
        })
        .expect("contract events");

    assert_eq!(page.next_fingerprint.as_deref(), Some("next-page"));
    assert_eq!(page.provider_calls, 1);
    let request = seen.lock().expect("seen").join("\n");
    assert!(
        request.starts_with("GET /v1/contracts/41abcdefabcdefabcdefabcdefabcdefabcdefabcd/events?")
    );
    assert!(request.contains("event_name=Transfer"));
    assert!(request.contains("min_block_number=10"));
    assert!(request.contains("max_block_number=12"));
    assert!(request.contains("only_confirmed=true"));
    assert!(request.contains("limit=50"));
    assert!(request.contains("fingerprint=previous-page"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("tron-pro-api-key: secret-key")
    );
}

#[test]
fn test_trongrid_missing_api_key_disables_contract_events() {
    let provider =
        TronHttpProvider::new("http://unused").with_trongrid("http://trongrid.invalid", None);

    assert!(!provider.supports_contract_event_query());
}

#[test]
fn test_trongrid_contract_events_response_is_normalized() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(
        seen,
        r#"{
            "data":[{
                "contract_address":"T111111111111111111111111111111111",
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
            contract_address: "T111111111111111111111111111111111".to_owned(),
            event_name: Some("MessageAccepted".to_owned()),
            range: LedgerRange::blocks(123, 123).expect("range"),
            only_confirmed: true,
            limit: 20,
            fingerprint: None,
        })
        .expect("contract events");

    assert_eq!(page.events.len(), 1);
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
fn test_trongrid_contract_events_malformed_response_is_invalid_request() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::spawn(seen, r#"{"meta":{}}"#, "HTTP/1.1 200 OK");
    let provider = TronHttpProvider::new("http://unused")
        .with_trongrid(server.url(), Some("secret-key".to_owned()));

    let error = provider
        .get_contract_events(TronContractEventRequest {
            contract_address: normalize_tron_contract_address(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            )
            .expect("address"),
            event_name: Some("Transfer".to_owned()),
            range: LedgerRange::blocks(10, 12).expect("range"),
            only_confirmed: true,
            limit: 50,
            fingerprint: None,
        })
        .expect_err("malformed response should stay visible");

    assert_eq!(error.kind, DatalensErrorKind::InvalidRequest);
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

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().expect("server thread");
        }
    }
}
