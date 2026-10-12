---
name: verify-datalens
description: Launch an isolated datalens server against live Ethereum RPC through a counting proxy, drive the real HTTP API as an application, and capture responses plus every provider call as evidence. Use when verifying a datalens change end to end, proving a cache hit/miss, or measuring how many RPC calls a query costs.
---

# verify-datalens

The user surface is the HTTP service (`datalens serve`): `POST /v1/query`, GraphQL
`/native/graphql`, `/v1/chains`, `/v1/discovery`, `/health`, `/metrics`. Callers are
applications (degov, ormp) authenticating with `x-datalens-application` +
`Authorization: Bearer`. The key observable besides rows is **provider cost**, so every
run puts a counting JSON-RPC proxy between datalens and the upstream RPC.

All paths below are relative to this skill directory; scripts resolve the repo root
themselves. Written against the object-storage implementation; the PostgreSQL redesign
(`docs/superpowers/plans/2026-10-12-datalens-postgres-redesign.md`) changes config,
request shape, and storage layout — update this skill when it lands.

## Launch

```sh
RUN=$(scripts/launch.sh)            # builds release binary; --no-build to reuse target/release/datalens
# optional: --upstream https://other-ethereum-rpc   (default https://ethereum-rpc.publicnode.com)
```

Each run gets a free server port, a free proxy port, a fresh local storage dir under
`.data/verify/$RUN/`, and a random application token. Config is rendered from
`templates/datalens.verify.toml` (auth required, warmup off, staging off). Several runs
can coexist. `PIN_START` (upstream head − 500 at launch) pins the query window so repeated
queries in one run are identical.

## Doctor

```sh
scripts/doctor.sh "$RUN"
```

Read-only. Confirms the recorded pids are alive, the server port belongs to *our* pid,
`/health` is up, unauthenticated `/v1/chains` is 401, authenticated `/v1/chains` and
`/v1/discovery` work, and the upstream answers through the proxy. Any `FAIL` means do
not drive — read `.data/verify-evidence/$RUN/server.log`.

## Drive

```sh
scripts/query.sh "$RUN" requests/logs-usdc-transfer-durable.json logs-miss
scripts/query.sh "$RUN" requests/blocks-durable.json blocks-miss
scripts/wait-durable.sh "$RUN" evm.blocks      # before expecting a hit
VERIFY_START=N VERIFY_END=M scripts/query.sh "$RUN" requests/<file>.json <label>
```

`query.sh` substitutes `"@START@"`/`"@END@"`, posts as the verify application, and prints
a summary: HTTP status, row count, `hit_ranges`/`missing_ranges`/`provider_fill_ranges`,
and the RPC calls this query caused (by method). For other endpoints use `curl` with the
values in `.data/verify/$RUN/state.env` (`BASE_URL`, `APP`, `TOKEN`, `METRICS_TOKEN`).
Feature-specific recipes live in `features/`.

## Evidence

Everything lands in `.data/verify-evidence/$RUN/` (git-ignored, survives cleanup):

- `queries/NN-<label>.{request,response,rpc-calls,summary}.json[l]` — per query.
- `rpc-calls.jsonl` — every JSON-RPC call (`method`, `params`, `http_request`, `error`).
- `server.log`, `proxy.log`, `run.env` (state without tokens, includes `GIT_REV`).

`scripts/rpc-calls.sh "$RUN" [file.jsonl]` summarizes calls by method, `eth_getLogs`
block spans, and JSON-RPC errors. Quote the summary JSON and the rpc-calls counts when
reporting; "it returned 200" is not evidence of cache behavior.

## Cleanup

```sh
scripts/cleanup.sh "$RUN"
```

Kills only the two pids recorded for this run, deletes `.data/verify/$RUN/`, keeps
evidence. Delete old evidence dirs by hand when no longer needed.

## Helpers

- `scripts/rpc_proxy.py` — counting forwarder; batch items are logged individually and
  share one `http_request` id.
- `scripts/wait-durable.sh` — waits for `storage wrote coverage` for a dataset/range in
  `server.log`.
- `requests/*.json` — native query bodies with `"@START@"`/`"@END@"` placeholders.
- `templates/datalens.verify.toml` — instance config; edit here to change finality lag,
  chunk sizes, or enable warmup/staging for a feature.

## Gotchas

- publicnode serves only roughly the last ~1000 blocks without an archive token; older
  ranges fail with `provider_failure` "Archive requests require a personal token". Keep
  windows near `PIN_START` or pass `--upstream` with an archive endpoint.
- A durable logs miss also fills `evm.block_headers` for the whole aligned 1000-block
  bucket: a 10-block logs miss costs ~1020 `eth_getBlockByNumber` calls and ~2 minutes on
  publicnode (batches of 20; publicnode intermittently 502s a batch and datalens falls back
  to ~1.4 s single calls).
- Durable writes are asynchronous: the query returns provider rows and submits a durable
  intent; the intent worker re-fetches the same range from the provider and only then
  writes coverage (~15–20 s on publicnode for 10 blocks). Every miss therefore costs the
  provider twice, and an identical query before the write costs a third time. Use
  `scripts/wait-durable.sh "$RUN" <dataset>` instead of a fixed sleep before asserting a hit.
- With `applications.required = true`, `/v1/chains` and `/v1/discovery` need credentials
  too; only `/health` is open. `/metrics` uses `METRICS_TOKEN` as bearer.
- Range values in request JSON are numbers; the placeholders are quoted strings that
  `query.sh` replaces including the quotes.
