fmt:
  cargo fmt --all

fmt-check:
  cargo fmt --all --check

check:
  cargo check --workspace

clippy:
  cargo clippy --workspace --all-targets -- -D warnings

e2e:
  cargo test -p datalens-edge --test query_flow

e2e-lifecycle:
  cargo test -p datalens-edge --test query_flow
  cargo test -p datalens-edge --test lifecycle
  cargo test -p datalens-executor --test query_execution
  cargo test -p datalens-warmup --test warmup_flow
  cargo test -p datalens-storage --test read_through_cache
  cargo test -p datalens-cli --test cli_commands test_inspect
  cargo test -p datalens-metrics --test metrics_encoding

multi-chain-e2e:
  cargo test -p datalens-solana
  cargo test -p datalens-tron
  cargo test -p datalens-indexer --test full_indexing_e2e
  cargo test -p datalens-cli --test index_commands

production-readiness:
  cargo test -p datalens-edge --test production_readiness
  cargo test -p datalens-cli --test index_commands test_index_backfill_persists_cursor_under_configured_cursor_path
  cargo test -p datalens-cli --test index_commands test_index_resume_uses_persisted_cursor_after_process_restart
  cargo test -p datalens-cli --test index_commands test_index_verify_does_not_write_data

s3-e2e:
  cargo test -p datalens-storage --test object_store test_s3_object_store_put_get_exists_list_delete_with_prefix
  cargo test -p datalens-edge --test lifecycle test_s3_lifecycle_is_gated_and_uses_dedicated_prefix

container-smoke:
  docker build -t datalens:smoke .
  docker run --rm datalens:smoke --help

config-doctor-smoke:
  cargo run -p datalens-cli -- doctor --config config/datalens.production.toml

release-check:
  just fmt-check
  just check
  just clippy
  just e2e-lifecycle
  just multi-chain-e2e
  just container-smoke
  just config-doctor-smoke
