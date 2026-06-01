use datalens_example_degov_client::{config::AppConfig, datalens::DatalensDegovClient};
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

#[test]
fn test_fetch_vote_cast_page_uses_decoded_events_connection_shape() {
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
                            "topics": ["0xtopic0"],
                            "data": "0x",
                            "removed": false,
                            "decodedArgs": {
                                "proposalId": "42",
                                "support": 1,
                                "weight": "7"
                            }
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
        event_topic0: "0xtopic0".to_owned(),
        event_signature: datalens_example_degov_client::datalens::VOTE_CAST_SIGNATURE.to_owned(),
        start_block: 100,
        end_block: Some(200),
        chunk_size: 25,
        reset_checkpoint: false,
        consumer_name: "degov-vote-consumer".to_owned(),
    }
}
