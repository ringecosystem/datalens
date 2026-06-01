use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
};

use datalens_example_ormp_client::{
    RunSummary, config::AppConfig, datalens::DatalensOrmpClient, db::AppDatabase,
};
use datalens_sdk::{ClientConfig, DatalensClient};

#[test]
fn test_app_index_fixture_supports_checkpoint_and_duplicate_replay() {
    let fixture = FixtureProcess::start();
    let db = AppDatabase::open("sqlite::memory:").expect("open database");
    db.migrate().expect("run migrations");

    let first = run_once(&db, fixture.endpoint(), None);
    assert_eq!(
        first,
        RunSummary {
            fetched_rows: 2,
            inserted_rows: 2,
            skipped_duplicates: 0,
            skipped_invalid: 0,
            checkpoint_cursor: Some("ormp-cursor-2".to_owned()),
            has_next_page: false,
        }
    );
    assert_eq!(db.message_count().expect("message count"), 2);

    let second = run_once(&db, fixture.endpoint(), Some("ormp-cursor-0".to_owned()));
    assert_eq!(
        second,
        RunSummary {
            fetched_rows: 2,
            inserted_rows: 0,
            skipped_duplicates: 2,
            skipped_invalid: 0,
            checkpoint_cursor: Some("ormp-cursor-2".to_owned()),
            has_next_page: false,
        }
    );
    assert_eq!(db.message_count().expect("message count"), 2);
    assert_eq!(
        db.checkpoint("ormp-message-consumer").expect("checkpoint"),
        Some("ormp-cursor-2".to_owned())
    );
}

fn run_once(db: &AppDatabase, endpoint: &str, start_cursor: Option<String>) -> RunSummary {
    let config = AppConfig {
        index_graphql_url: endpoint.to_owned(),
        token: None,
        database_url: "sqlite::memory:".to_owned(),
        page_size: 25,
        start_cursor,
        consumer_name: "ormp-message-consumer".to_owned(),
    };
    let sdk = DatalensClient::new(ClientConfig {
        endpoint: endpoint.to_owned(),
        bearer_token: None,
        timeout: None,
        user_agent: Some("datalens-ormp-client-fixture-e2e".to_owned()),
    })
    .expect("client config");
    let client = DatalensOrmpClient::new(sdk);
    datalens_example_ormp_client::run_once(&config, db, &client).expect("run fixture client")
}

struct FixtureProcess {
    child: Child,
    endpoint: String,
}

impl FixtureProcess {
    fn start() -> Self {
        let binary =
            std::env::var("CARGO_BIN_EXE_ormp-app-index-fixture").expect("fixture binary path");
        let mut child = Command::new(binary)
            .env("ORMP_FIXTURE_ADDR", "127.0.0.1:0")
            .stdout(Stdio::piped())
            .spawn()
            .expect("start ORMP fixture");
        let stdout = child.stdout.take().expect("fixture stdout");
        let mut reader = BufReader::new(stdout);
        let mut endpoint = String::new();
        reader.read_line(&mut endpoint).expect("read endpoint");
        assert!(
            endpoint.starts_with("listening "),
            "unexpected fixture output: {endpoint}"
        );

        Self {
            child,
            endpoint: endpoint.trim_start_matches("listening ").trim().to_owned(),
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for FixtureProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
