use datalens_example_ormp_client::config::AppConfig;
use datalens_example_ormp_client::fetch_message_accepted_page;
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

const MESSAGE_ACCEPTED_TOPIC0: &str =
    "0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18";
const MESSAGE_ACCEPTED_MSG_HASH: &str =
    "0x60f5743a8b3bbe4e4bd99607b19985203a9310f4859e03912ed086f4d32bdff8";
const MESSAGE_ACCEPTED_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000013b2211a7ca45db2808f6db05557ce5347e3634e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000002cd1867fb8016f93710b6386f7f9f1d540a60812000000000000000000000000000000000000000000000000000000000000002e0000000000000000000000002cd1867fb8016f93710b6386f7f9f1d540a60812000000000000000000000000000000000000000000000000000000000001d874000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000a4394d1bca0000000000000000000000009f33a4809aa708d7a399fedba514e0a0d15efa850000000000000000000000009f33a4809aa708d7a399fedba514e0a0d15efa8500000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000008844866883501484100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

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
                            "topics": [MESSAGE_ACCEPTED_TOPIC0, MESSAGE_ACCEPTED_MSG_HASH],
                            "data": MESSAGE_ACCEPTED_DATA,
                            "removed": false
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
        Some(MESSAGE_ACCEPTED_MSG_HASH)
    );
    assert_eq!(
        page.events[0].event.decode_status.as_deref(),
        Some("decoded")
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
        MESSAGE_ACCEPTED_TOPIC0
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
        event_topic0: MESSAGE_ACCEPTED_TOPIC0.to_owned(),
        event_signature: datalens_example_ormp_client::datalens::MESSAGE_ACCEPTED_SIGNATURE
            .to_owned(),
        start_block: 10,
        end_block: Some(11),
        chunk_size: 2,
        reset_checkpoint: false,
        consumer_name: "ormp-message-consumer".to_owned(),
    }
}
