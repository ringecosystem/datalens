# Load test

`cargo test --release -p datalens-server --test load` boots datalens against local mock chains and the test Postgres from `DATABASE_URL`. Release mode is the measured server. A debug build spends most of the query in unoptimized JSON encoding.

Eight chains use the production lag and range settings. Arbitrum's follower tick in production is 2 seconds. The test ticks every 100 ms, which is a 20× compression, and measures 30 wall-clock seconds. That is 10 simulated minutes. `arbitrum_steady_rps` is provider calls divided by those simulated seconds, so it is the rate at the 2 second tick. The steady window starts only after follow lag is 0.

The covered query is one contract over 10 000 blocks already filled into Postgres. `covered_query_p95_ms` is the 95th percentile of 20 HTTP queries of that range.

```
DATABASE_URL=postgres://datalens:datalens@127.0.0.1:55901/datalens \
CARGO_INCREMENTAL=0 \
CARGO_TARGET_DIR=/code/helixbox/.cargo-target/datalens \
./loadtest/run.sh
```
