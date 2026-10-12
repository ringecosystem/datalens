# Load test

`cargo test --release -p datalens-server --test load` boots datalens against local mock chains and the test Postgres from `DATABASE_URL`. Release mode is the measured server. A debug build spends most of the query in unoptimized JSON encoding.

Eight chains use the production lag and range settings. Arbitrum's follower tick in production is 2 seconds. The test ticks every 100 ms, which is a 20× compression, and measures 30 wall-clock seconds. That is 10 simulated minutes. `degov-live`, `degov-staging`, `degov-realtime`, and `ormp` each watch the same Arbitrum log selector. After follow lag is 0, the mock head advances 8 blocks per tick (about 4 blocks/s at the production tick). `arbitrum_steady_rps` is provider calls in that window divided by the simulated seconds. `arbitrum_eth_getLogs` counts log fetches while the head is moving.

The covered query is one contract over 10 000 blocks already filled into Postgres. `covered_query_p95_ms` is the 95th percentile of 20 HTTP queries of that range.

```
DATABASE_URL=postgres://datalens:datalens@127.0.0.1:55901/datalens \
CARGO_INCREMENTAL=0 \
CARGO_TARGET_DIR=/code/helixbox/.cargo-target/datalens \
./loadtest/run.sh
```
