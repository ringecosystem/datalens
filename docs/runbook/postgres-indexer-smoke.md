# PostgreSQL Indexer Smoke

Goal: Run the opt-in PostgreSQL integration smoke tests for the datalens indexer.

Read this when: You need real PostgreSQL coverage for schema creation, idempotent
writes, query filters, and GraphQL event queries.

Preconditions:

- Docker Compose is available, or `DATALENS_POSTGRES_TEST_URL` points at a disposable
  PostgreSQL database.
- The database may be modified by integration tests.

Depends on:

- `docker-compose.yml` service `postgres`.
- `Justfile` target `indexer-postgres-e2e`.

Verification: `just indexer-postgres-e2e` exits successfully.

## Local Compose Database

Start PostgreSQL:

```sh
just postgres-up
```

Run the smoke tests:

```sh
just indexer-postgres-e2e
```

Stop the service:

```sh
docker compose down
```

Remove persisted local test data:

```sh
rm -rf .data/postgres
```

## External Database

Set `DATALENS_POSTGRES_TEST_URL` to a disposable test database URL, then run:

```sh
just indexer-postgres-e2e
```

The dedicated target runs with `DATALENS_REQUIRE_POSTGRES_TEST_URL=1`, so direct
strict invocations fail clearly when no PostgreSQL URL is available. Normal indexer
tests remain optional and skip this PostgreSQL smoke path when the URL is unset.
