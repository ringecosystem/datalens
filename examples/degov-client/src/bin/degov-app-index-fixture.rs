use std::{
    env,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

use serde_json::{Value, json};

const DEFAULT_ADDR: &str = "127.0.0.1:3101";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("DEGOV_FIXTURE_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_owned());
    let listener = TcpListener::bind(&addr)?;
    let addr = listener.local_addr()?;
    println!("listening http://{addr}/graphql");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream)?,
            Err(error) => eprintln!("failed to accept Degov fixture request: {error}"),
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
                .or_else(|| (cursor == "degov-cursor-0").then_some(0))
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
        vote_edge("degov-cursor-1", 1, "7", 10),
        vote_edge("degov-cursor-2", 0, "3", 11),
        vote_edge("degov-cursor-3", 2, "2", 12),
    ]
}

fn vote_edge(cursor: &str, support: u64, weight: &str, log_index: u64) -> Value {
    json!({
        "cursor": cursor,
        "node": {
            "indexName": "degov",
            "chain": "ethereum",
            "chainId": 1,
            "dataset": "evm.logs",
            "blockNumber": 20_100_000 + log_index,
            "blockHash": format!("0xdegovblock{log_index:x}"),
            "transactionHash": format!("0xdegovtx{log_index:x}"),
            "transactionIndex": 0,
            "logIndex": log_index,
            "address": "0x00000000000000000000000000000000000degov",
            "eventName": "VoteCast",
            "signature": "VoteCast(address,uint256,uint8,uint256,string)",
            "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "decodedArgs": {
                "voter": format!("0xvoter{log_index:032x}"),
                "proposalId": "42",
                "support": support,
                "weight": weight,
                "reason": "fixture vote"
            },
            "decodeStatus": "decoded",
            "decodeError": null,
            "payload": {"fixture": "degov-app-index"},
            "createdAt": "2026-05-31T00:00:00Z"
        }
    })
}
