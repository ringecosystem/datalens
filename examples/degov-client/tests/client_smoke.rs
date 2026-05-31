use datalens_example_degov_client::{ProposalMaterializer, ProposalProjection, consume_vote_page};
use datalens_sdk::{ClientConfig, DatalensClient};
use serde_json::json;

mod support;

use support::MockGraphqlServer;

#[test]
fn test_consume_vote_page_updates_application_checkpoint_and_projection() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "decodedEventsConnection": {
                "edges": [
                    {
                        "cursor": "vote-cursor-1",
                        "node": {
                            "indexName": "degov",
                            "chain": "ethereum",
                            "chainId": 1,
                            "dataset": "evm.logs",
                            "blockNumber": 100,
                            "blockHash": "0xblock1",
                            "transactionHash": "0xtx1",
                            "transactionIndex": 0,
                            "logIndex": 1,
                            "address": "0xgovernor",
                            "eventName": "VoteCast",
                            "signature": "VoteCast(address,uint256,uint8,uint256,string)",
                            "topic0": "0xtopic0",
                            "decodedArgs": {
                                "proposalId": "42",
                                "support": 1,
                                "weight": "7"
                            },
                            "decodeStatus": "decoded",
                            "decodeError": null,
                            "payload": {},
                            "createdAt": "2026-05-31T00:00:00Z"
                        }
                    }
                ],
                "nodes": [],
                "pageInfo": {
                    "endCursor": "vote-cursor-1",
                    "hasNextPage": true
                }
            }
        }
    })]);
    let client = client(&server);
    let mut projection = ProposalProjection::default();
    let mut materializer = ProposalMaterializer::default();

    let checkpoint = consume_vote_page(&client, &mut materializer, &mut projection, None, 25)
        .expect("consume vote page");

    assert_eq!(checkpoint.cursor.as_deref(), Some("vote-cursor-1"));
    assert!(checkpoint.has_next_page);
    assert_eq!(projection.for_votes("42"), 7);

    let request = server.only_request();
    assert!(request.query.contains("decodedEventsConnection("));
    assert_eq!(request.variables["dataset"], "evm.logs");
    assert_eq!(request.variables["indexName"], "degov");
    assert_eq!(request.variables["eventName"], "VoteCast");
    assert_eq!(request.variables["first"], 25);
}

fn client(server: &MockGraphqlServer) -> DatalensClient {
    DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        timeout: None,
        user_agent: Some("datalens-degov-client-example-tests".to_owned()),
    })
    .expect("client config")
}
