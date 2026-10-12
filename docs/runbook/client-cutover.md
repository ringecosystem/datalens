# Client cutover

Purpose: The order in which clients move to this API.

Read when: Planning a switch from the previous datalens service.

Does not cover: Executing that switch. Production cutover is not performed from this change.

## Order

1. Point the staging indexer (`degov-staging`) at the new base URL, application id, and token. Confirm a durable checkpoint advances and a second pass adds no `query_fill` calls.
2. Point ormpindexer at the new REST query API for `evm.logs` with `transaction_from` requested only on the fields that need the sender.
3. Point the production indexer (`degov-live`) and the realtime worker (`degov-realtime`, a separate application id and token) after staging has stayed caught up.

## Client rules

- There is no compatibility with the previous request body or response shape.
- Durable catch-up follows `next_start` and does not checkpoint past `served.end`.
- Realtime uses `latest_only`, reads `source = hot` segments, and rolls back when the anchor hash changes.
- `POST /v1/watch` keeps the hot selector subscribed. The subscription expires after `follow.subscription_ttl_s` unless renewed.
