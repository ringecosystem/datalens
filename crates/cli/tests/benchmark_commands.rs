use std::{fs, process::Command as ProcessCommand};

use clap::Parser;
use datalens_cli::*;

#[test]
fn test_benchmark_run_accepts_config_path() {
    let cli = Cli::parse_from(["datalens", "benchmark", "run", "--config", "bench.toml"]);

    match cli.command {
        Command::Benchmark(BenchmarkCommand {
            command: BenchmarkSubcommand::Run(command),
        }) => assert_eq!(command.config, "bench.toml"),
        command => panic!("expected benchmark run command, got {command:?}"),
    }
}

#[test]
fn test_benchmark_run_generates_mock_report_schema() {
    let root = temp_storage_root("benchmark-report");
    let config_path = root.join("benchmark.toml");
    let report_path = root.join("report.json");
    fs::write(
        &config_path,
        format!(
            r#"
name = "capacity-smoke"
mode = "mock"
output_backend = "jsonl"
concurrency_limit = 2
cache_server_endpoint = "http://127.0.0.1:9000"
database_url = "sqlite:{}/index.db"
report_path = "{}"

[[chains]]
name = "ethereum"
family = "evm"
start_block = 10
end_block = 12
contracts = ["0x0000000000000000000000000000000000000001"]
datasets = ["evm.logs"]

[[chains]]
name = "tron"
family = "tron"
start_block = 20
end_block = 21
contracts = ["TContract"]
datasets = ["tron.events"]

[graphql]
enabled = true
endpoint = "http://127.0.0.1:9090/graphql"
sample_count = 2
"#,
            root.display(),
            report_path.display()
        ),
    )
    .expect("write benchmark config");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["benchmark", "run", "--config"])
        .arg(&config_path)
        .output()
        .expect("run datalens benchmark");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(report_path.exists());

    let summary: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("benchmark stdout JSON");
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(&report_path).expect("report file")).expect("report JSON");

    assert_eq!(summary["scenario"], "capacity-smoke");
    assert_eq!(summary["mode"], "mock");
    assert_eq!(
        summary["report_path"],
        report_path.to_string_lossy().as_ref()
    );
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["scenario"], "capacity-smoke");
    assert_eq!(report["config"]["output_backend"], "jsonl");
    assert_eq!(report["config"]["concurrency_limit"], 2);
    assert_eq!(report["chains"].as_array().expect("chains").len(), 2);

    let ethereum = &report["chains"][0];
    assert_eq!(ethereum["chain"], "ethereum");
    assert_eq!(ethereum["initial_sync_duration_ms"], 6);
    assert_eq!(ethereum["repeat_sync_duration_ms"], 2);
    assert_eq!(ethereum["records_fetched"], 6);
    assert_eq!(ethereum["records_written"], 6);
    assert_eq!(ethereum["provider_hit_count"], 3);
    assert_eq!(ethereum["cache_hit_count"], 3);
    assert_eq!(ethereum["latest_indexed_block"], 12);
    assert!(ethereum["object_storage_size_bytes"].as_u64().unwrap() > 0);
    assert!(ethereum["database_size_bytes"].as_u64().unwrap() > 0);
    assert_eq!(ethereum["output_file_count"], 1);
    assert!(ethereum["output_size_bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        ethereum["graphql_latency_samples_ms"]
            .as_array()
            .expect("latency samples")
            .len(),
        2
    );
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-cli-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("create temp dir");
    root
}
