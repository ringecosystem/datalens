# Deployment

Purpose: What a deployed datalens process needs.

Read when: Rendering a config for a long-running process.

Does not cover: The order clients switch (`docs/runbook/client-cutover.md`). Executing a production cutover is out of scope for this repository change.

## Process

- Image command: `datalens serve --config /config/datalens.toml`.
- One or more replicas. Each replica with `roles = ["api", "worker"]` runs the API and fill workers. The chain leader is elected with `pg_try_advisory_lock(chain_id)`, so only one replica follows each chain.
- Set `DATALENS_DATABASE_URL` and `DATALENS_METRICS_TOKEN`. Chain RPC URLs are further `${...}` placeholders in `config/datalens.example.toml`.
- The API role serves `/health`, `/ready`, `/metrics`, and `/v1/*`. Readiness fails when `select 1` does not return within 2 seconds.

## Migrations

`datalens migrate --config <file>` or process startup applies `crates/store/migrations`. There is no import of a previous store. A new database starts empty and fills on demand.
