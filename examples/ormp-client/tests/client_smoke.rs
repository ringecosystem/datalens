use datalens_example_ormp_client::fetch_message_accepted_page;
use datalens_sdk::{
    ClientConfig, DatalensClient,
    index::{EventFilter, PageRequest},
};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

#[test]
fn test_fetch_message_accepted_page_queries_sdk_connection_with_cursor() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "decodedEventsConnection": {
                "edges": [{
                    "cursor": "cursor-2",
                    "node": {
                        "indexName": "ormp",
                        "chain": "ethereum",
                        "chainId": 1,
                        "dataset": "evm.logs",
                        "blockNumber": 20009590,
                        "blockHash": "0xblock",
                        "transactionHash": "0xtx",
                        "transactionIndex": 0,
                        "logIndex": 3,
                        "address": "0x13b2211a7ca45db2808f6db05557ce5347e3634e",
                        "eventName": "MessageAccepted",
                        "signature": "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))",
                        "topic0": "0xtopic0",
                        "decodedArgs": {"msgHash": "0xhash"},
                        "decodeStatus": "decoded",
                        "decodeError": null,
                        "payload": {"source": "mock"},
                        "createdAt": "2026-05-31T00:00:00Z"
                    }
                }],
                "nodes": [],
                "pageInfo": {
                    "endCursor": "cursor-2",
                    "hasNextPage": false
                }
            }
        }
    })]);
    let client = client(&server);

    let page = fetch_message_accepted_page(&client, Some("cursor-1".to_owned()), 2)
        .expect("message accepted page");

    assert_eq!(page.events.len(), 1);
    assert_eq!(
        page.events[0].event.decoded_args["msgHash"].as_str(),
        Some("0xhash")
    );
    assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));
    assert!(!page.has_next_page);

    let request = server.only_request();
    assert!(request.query.contains("decodedEventsConnection("));
    assert_eq!(request.variables["dataset"], "evm.logs");
    assert_eq!(request.variables["indexName"], "ormp");
    assert_eq!(request.variables["eventName"], "MessageAccepted");
    assert_eq!(request.variables["first"], 2);
    assert_eq!(request.variables["after"], "cursor-1");
}

fn client(server: &MockGraphqlServer) -> DatalensClient {
    DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        timeout: None,
        user_agent: Some("datalens-ormp-client-example-tests".to_owned()),
    })
    .expect("client config")
}

#[allow(dead_code)]
fn _sdk_usage_shape(client: &DatalensClient) {
    let _ = client.index().decoded_events_connection(
        EventFilter::new("evm.logs").with_index_name("ormp"),
        PageRequest::first(10),
    );
}
