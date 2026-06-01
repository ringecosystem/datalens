use std::{
    env,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

use serde_json::{Value, json};

const DEFAULT_ADDR: &str = "127.0.0.1:3100";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("ORMP_FIXTURE_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_owned());
    let listener = TcpListener::bind(&addr)?;
    let addr = listener.local_addr()?;
    println!("listening http://{addr}/graphql");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream)?,
            Err(error) => eprintln!("failed to accept ORMP fixture request: {error}"),
        }
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let request = read_graphql_request(&stream)?;
    let after = request
        .get("variables")
        .and_then(|variables| variables.get("after"))
        .and_then(Value::as_str);
    let first = request
        .get("variables")
        .and_then(|variables| variables.get("first"))
        .and_then(Value::as_u64)
        .unwrap_or(25) as usize;
    let response = graphql_page(after, first);
    let body = response.to_string();

    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )?;
    Ok(())
}

fn read_graphql_request(stream: &TcpStream) -> Result<Value, Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut content_length = 0usize;

    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length: ") {
            content_length = value.parse()?;
        }
    }

    let mut body = vec![0; content_length];
    reader.read_exact(&mut body)?;
    Ok(serde_json::from_slice(&body)?)
}

fn graphql_page(after: Option<&str>, first: usize) -> Value {
    let events = fixture_events();
    let start = after
        .and_then(|cursor| {
            events
                .iter()
                .position(|event| event["cursor"].as_str() == Some(cursor))
                .map(|index| index + 1)
                .or_else(|| (cursor == "ormp-cursor-0").then_some(0))
        })
        .unwrap_or(0);
    let end = usize::min(start.saturating_add(first), events.len());
    let edges = events[start..end].to_vec();
    let end_cursor = edges
        .last()
        .and_then(|edge| edge["cursor"].as_str())
        .map(str::to_owned);

    json!({
        "data": {
            "decodedEventsConnection": {
                "edges": edges,
                "nodes": [],
                "pageInfo": {
                    "endCursor": end_cursor,
                    "hasNextPage": end < events.len()
                }
            }
        }
    })
}

fn fixture_events() -> Vec<Value> {
    vec![
        message_edge(
            "ormp-cursor-1",
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            20_009_590,
            3,
            137,
            "0xsender000000000000000000000000000000000001",
            "0xreceiver0000000000000000000000000000000001",
        ),
        message_edge(
            "ormp-cursor-2",
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            20_009_591,
            4,
            42161,
            "0xsender000000000000000000000000000000000002",
            "0xreceiver0000000000000000000000000000000002",
        ),
    ]
}

fn message_edge(
    cursor: &str,
    message_hash: &str,
    block_number: u64,
    log_index: u64,
    target_chain_id: u64,
    sender: &str,
    receiver: &str,
) -> Value {
    json!({
        "cursor": cursor,
        "node": {
            "indexName": "ormp",
            "chain": "ethereum",
            "chainId": 1,
            "dataset": "evm.logs",
            "blockNumber": block_number,
            "blockHash": format!("0xblock{block_number:x}"),
            "transactionHash": format!("0xtx{log_index:x}"),
            "transactionIndex": 0,
            "logIndex": log_index,
            "address": "0x13b2211a7ca45db2808f6db05557ce5347e3634e",
            "eventName": "MessageAccepted",
            "signature": "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))",
            "topic0": "0x9b7e1f2f8b08c3e25a8e8f447d5dddeaa802af9a8904887f42cbf9b0c924f300",
            "decodedArgs": {
                "msgHash": message_hash,
                "sourceChainId": 1,
                "targetChainId": target_chain_id,
                "sender": sender,
                "receiver": receiver
            },
            "decodeStatus": "decoded",
            "decodeError": null,
            "payload": {"fixture": "ormp-app-index"},
            "createdAt": "2026-05-31T00:00:00Z"
        }
    })
}
