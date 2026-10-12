# Local development

Purpose: Start PostgreSQL and the datalens process on a workstation.

Read when: Running the server or the test suite locally.

Does not cover: Production env layout (`docs/runbook/deployment.md`).

## Postgres

```sh
just pg-up
export DATABASE_URL=postgres://datalens:datalens@127.0.0.1:5432/datalens
```

`just pg-down` stops the compose Postgres. Tests need a superuser URL in `DATABASE_URL` because `#[sqlx::test]` creates a database per test.

Shared redesign test Postgres, when that container is running:

```sh
export DATABASE_URL=postgres://datalens:datalens@127.0.0.1:55901/datalens
```

## Gates

```sh
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR=/code/helixbox/.cargo-target/datalens
just check
```

`just check` is `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.

SDK gates: `just sdk` (TypeScript `npm test`, Go `go test`).

## Serve

```sh
just serve
```

Default config is `config/datalens.dev.toml`. Migrations run on startup. `datalens migrate --config <file>` applies them and exits.

## Load test

```sh
./loadtest/run.sh
```

That is `cargo test --release -p datalens-server --test load`. See `loadtest/README.md`. The debug profile does not meet the 100 ms query budget.
