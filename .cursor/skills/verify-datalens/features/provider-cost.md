# Provider cost

The first durable logs query records `eth_getLogs` in `rpc-calls.jsonl`. The second identical query must add zero `eth_getLogs` lines. `scripts/e2e.sh` records that delta as `logs-cache-hit`.
