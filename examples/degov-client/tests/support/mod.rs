use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use serde_json::Value;

#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub query: String,
    pub variables: Value,
}

pub struct MockGraphqlServer {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl MockGraphqlServer {
    pub fn new(responses: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let address = listener.local_addr().expect("mock server address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let worker_requests = Arc::clone(&requests);
        let worker = thread::spawn(move || {
            for response in responses {
                let (stream, _) = listener.accept().expect("accept mock request");
                handle_connection(stream, &worker_requests, 200, &response);
            }
        });

        Self {
            address,
            requests,
            worker: Some(worker),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn only_request(&self) -> RecordedRequest {
        let requests = self.requests.lock().expect("recorded requests");
        assert_eq!(requests.len(), 1);
        requests[0].clone()
    }
}

impl Drop for MockGraphqlServer {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.join().expect("mock server worker");
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
    status: u16,
    response: &Value,
) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read header");
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some(value) = trimmed.strip_prefix("content-length: ") {
            content_length = value.parse().expect("content length");
        }
    }

    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).expect("read request body");
    let body: Value = serde_json::from_slice(&body).expect("request json");
    requests
        .lock()
        .expect("record requests")
        .push(RecordedRequest {
            query: body["query"].as_str().unwrap_or_default().to_owned(),
            variables: body["variables"].clone(),
        });

    let response_body = response.to_string();
    let reason = if status == 200 { "OK" } else { "ERROR" };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        response_body.len(),
        response_body
    )
    .expect("write response");
}
