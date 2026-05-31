# Bridge Processor SDK Validation

Purpose: Reduced ORMP-style processor example for validating application-owned
entity projection, duplicate guards, transaction rollback, mockable chain reads,
and GraphQL entity queries.

Run the validation tests:

```sh
cargo test -p datalens-bridge-processor --test bridge_processor
```

The example uses in-memory SQLite in tests and does not require live RPC.
