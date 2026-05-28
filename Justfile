fmt:
  cargo fmt --all

fmt-check:
  cargo fmt --all --check

check:
  cargo check --workspace

clippy:
  cargo clippy --workspace --all-targets -- -D warnings

e2e:
  cargo test -p datalens-api --test query_flow

e2e-lifecycle:
  cargo test -p datalens-api --test query_flow
  cargo test -p datalens-api --test lifecycle
  cargo test -p datalens-cli --test cli_commands test_inspect
  cargo test -p datalens-metrics --test metrics_encoding
