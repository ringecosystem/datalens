# Durable logs query

`scripts/e2e.sh` posts `requests/logs-usdc-transfer-durable.json` for `PIN_START`..`PIN_END`.

Pass: HTTP 200, `served` is an object, `incomplete_reason` is null. The next identical post adds zero `eth_getLogs` lines to `rpc-calls.jsonl`.
