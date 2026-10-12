fmt:
  cargo fmt --all

fmt-check:
  cargo fmt --all --check

clippy:
  cargo clippy --workspace --all-targets -- -D warnings

# Needs DATABASE_URL pointing at a PostgreSQL superuser, e.g. from `just pg-up`.
test:
  cargo test --workspace

check: fmt-check clippy test

pg-up:
  docker compose up -d --wait postgres

pg-down:
  docker compose down

serve config="config/datalens.dev.toml":
  cargo run -p datalens-server --bin datalens -- serve --config {{config}}
