use datalens_example_degov_client::{
    config::{AppConfig, DEFAULT_EVENT_TOPIC0},
    datalens::DatalensDegovClient,
};
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

#[test]
fn test_fetch_vote_cast_page_decodes_raw_native_log() {
    let topic0 = vote_cast_topic0();
    assert_eq!(DEFAULT_EVENT_TOPIC0, topic0);
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "query": {
                "chain": {"configuredName": "ethereum"},
                "datasetKey": "evm.logs",
                "range": {"kind": "block", "start": 100, "end": 124},
                "cache": {"hitRanges": [], "missingRanges": []},
                "rows": {
                    "dataset_key": "evm.logs",
                    "rows": {
                        "dataset": "logs",
                        "rows": [{
                            "block_number": 100,
                            "block_hash": "0xblock1",
                            "transaction_hash": "0xtx1",
                            "transaction_index": 0,
                            "log_index": 1,
                            "address": "0xgovernor",
                            "topics": [topic0, padded_address_topic("0x1111111111111111111111111111111111111111")],
                            "data": vote_cast_data("42", 1, "7", "because"),
                            "removed": false
                        }]
                    }
                }
            }
        }
    })]);
    let client = DatalensDegovClient::new(client(&server));
    let config = test_config(&server);

    let page = client
        .fetch_vote_cast_page(&config, 100, 124)
        .expect("consume vote page");

    assert_eq!(page.next_cursor.as_deref(), Some("125"));
    assert!(page.has_next_page);
    assert_eq!(page.events.len(), 1);
    assert_eq!(
        page.events[0].event.decoded_args,
        json!({
            "voter": "0x1111111111111111111111111111111111111111",
            "proposalId": "42",
            "support": "1",
            "weight": "7",
            "reason": "because"
        })
    );
    assert_eq!(
        page.events[0].event.decode_status.as_deref(),
        Some("decoded")
    );

    let request = server.only_request();
    assert!(request.query.contains("query($input: QueryInput!)"));
    assert_eq!(
        request.variables["input"]["chain"]["configuredName"],
        "ethereum"
    );
    assert_eq!(request.variables["input"]["datasetKey"]["family"], "evm");
    assert_eq!(request.variables["input"]["datasetKey"]["name"], "logs");
    assert_eq!(request.variables["input"]["selector"]["kind"], "evm_logs");
    assert_eq!(request.variables["input"]["range"]["start"], 100);
    assert_eq!(request.variables["input"]["range"]["end"], 124);
}

fn client(server: &MockGraphqlServer) -> DatalensClient {
    DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: Some("degov-client-test".to_owned()),
        timeout: None,
        user_agent: Some("datalens-degov-client-example-tests".to_owned()),
    })
    .expect("client config")
}

fn test_config(server: &MockGraphqlServer) -> AppConfig {
    AppConfig {
        datalens_endpoint: server.endpoint(),
        token: None,
        application: "degov-client-test".to_owned(),
        database_url: "sqlite::memory:".to_owned(),
        chain_name: "ethereum".to_owned(),
        chain_id: 1,
        dataset_family: "evm".to_owned(),
        dataset_name: "logs".to_owned(),
        contract_address: "0xgovernor".to_owned(),
        event_topic0: vote_cast_topic0(),
        event_signature: datalens_example_degov_client::datalens::VOTE_CAST_SIGNATURE.to_owned(),
        start_block: 100,
        end_block: Some(200),
        chunk_size: 25,
        reset_checkpoint: false,
        consumer_name: "degov-vote-consumer".to_owned(),
    }
}

fn vote_cast_topic0() -> String {
    format!(
        "{:#x}",
        alloy_primitives::keccak256("VoteCast(address,uint256,uint8,uint256,string)")
    )
}

fn padded_address_topic(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

fn vote_cast_data(proposal_id: &str, support: u8, weight: &str, reason: &str) -> String {
    let proposal_id = proposal_id.parse::<u128>().expect("proposal id");
    let weight = weight.parse::<u128>().expect("weight");
    let reason_hex = reason
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let padded_reason = format!("{reason_hex:0<64}");
    format!(
        "0x{proposal_id:0>64x}{support:0>64x}{weight:0>64x}{offset:0>64x}{len:0>64x}{padded_reason}",
        offset = 128,
        len = reason.len()
    )
}
