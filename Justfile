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
