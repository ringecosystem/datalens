use datalens_example_ormp_client::config::AppConfig;
use datalens_example_ormp_client::fetch_message_accepted_page;
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

#[test]
fn test_fetch_message_accepted_page_queries_sdk_connection_with_cursor() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "query": {
                "chain": {"configuredName": "ethereum"},
                "datasetKey": "evm.logs",
                "range": {"kind": "block", "start": 10, "end": 11},
                "cache": {"hitRanges": [], "missingRanges": []},
                "rows": {
                    "dataset_key": "evm.logs",
                    "rows": {
                        "dataset": "logs",
                        "rows": [{
                            "block_number": 10,
                            "block_hash": "0xblock",
                            "transaction_hash": "0xtx",
                            "transaction_index": 0,
                            "log_index": 3,
                            "address": "0x13b2211a7ca45db2808f6db05557ce5347e3634e",
                            "topics": ["0xtopic0"],
                            "data": "0x",
                            "removed": false,
                            "decodedArgs": {"msgHash": "0xhash"}
                        }]
                    }
                }
            }
        }
    })]);
    let client = client(&server);
    let config = test_config(&server);

    let page =
        fetch_message_accepted_page(&client, &config, 10, 11).expect("message accepted page");

    assert_eq!(page.events.len(), 1);
    assert_eq!(
        page.events[0].event.decoded_args["msgHash"].as_str(),
        Some("0xhash")
    );
    assert_eq!(page.next_cursor.as_deref(), Some("12"));
    assert!(!page.has_next_page);

    let request = server.only_request();
    assert!(request.query.contains("query($input: QueryInput!)"));
    assert_eq!(
        request.variables["input"]["chain"]["configuredName"],
        "ethereum"
    );
    assert_eq!(
        request.variables["input"]["chain"]["networkId"]["numeric"],
        1
    );
    assert_eq!(request.variables["input"]["datasetKey"]["family"], "evm");
    assert_eq!(request.variables["input"]["datasetKey"]["name"], "logs");
    assert_eq!(request.variables["input"]["selector"]["kind"], "evm_logs");
    assert_eq!(
        request.variables["input"]["selector"]["evmLogs"]["addresses"][0],
        "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
    );
    assert_eq!(
        request.variables["input"]["selector"]["evmLogs"]["topics"][0][0],
        "0xtopic0"
    );
    assert_eq!(request.variables["input"]["range"]["start"], 10);
    assert_eq!(request.variables["input"]["range"]["end"], 11);
}

fn client(server: &MockGraphqlServer) -> DatalensClient {
    DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: Some("ormp-client-test".to_owned()),
        timeout: None,
        user_agent: Some("datalens-ormp-client-example-tests".to_owned()),
    })
    .expect("client config")
}

fn test_config(server: &MockGraphqlServer) -> AppConfig {
    AppConfig {
        datalens_endpoint: server.endpoint(),
        token: None,
        application: "ormp-client-test".to_owned(),
        database_url: "sqlite::memory:".to_owned(),
        chain_name: "ethereum".to_owned(),
        chain_id: 1,
        dataset_family: "evm".to_owned(),
        dataset_name: "logs".to_owned(),
        contract_address: "0x13b2211a7ca45db2808f6db05557ce5347e3634e".to_owned(),
        event_topic0: "0xtopic0".to_owned(),
        event_signature: datalens_example_ormp_client::datalens::MESSAGE_ACCEPTED_SIGNATURE
            .to_owned(),
        start_block: 10,
        end_block: Some(11),
        chunk_size: 2,
        reset_checkpoint: false,
        consumer_name: "ormp-message-consumer".to_owned(),
    }
}
