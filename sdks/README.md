# datalens SDKs

Clients for the datalens query API (`POST /v1/query`, `/v1/heads`, `/v1/watch`). All three expose the
same request and response types, `query`, `queryAll`, `heads`, `waitHeads`, and `follow`.

`follow` reads from a start block in chunks, then keeps up with the head. It long-polls
`/v1/heads/{chain}/wait` when caught up and resumes at `next_start`. It waits on `pending` and
`not_followed_yet`, and backs off on `provider_error` and `budget_exhausted`. `durable_only`
follows the safe head. `safe_to_latest` and `latest_only` follow latest.

Every call retries 429 and 5xx responses and transport failures with jittered backoff, honoring
`Retry-After`. Errors carry the stable `error.kind`: `unauthorized`, `forbidden`, `invalid_input`,
`quota_exceeded`, `unsupported`, or `internal`.

The GraphQL schema is in [`schemas/native.graphql`](../schemas/native.graphql). Regenerate it
with `just schema`.

## Rust (`sdks/rust`, crate `datalens-sdk`)

```toml
datalens-sdk = { git = "https://github.com/ringecosystem/datalens", rev = "<sha>" }
```

```rust
use datalens_sdk::{
    Client, ClientConfig, FinalizedCursor, FollowEvent, FollowRequest, LogSelector, ChainName,
};
use futures::StreamExt;

let client = Client::new(ClientConfig::new(endpoint, "degov-live", token))?;
let mut request = FollowRequest::logs(ChainName::new("arbitrum")?, selector, checkpoint);
request.chunk_blocks = 1_000;
let mut cursor = FinalizedCursor::new(checkpoint);
let mut events = client.follow(request);
while let Some(event) = events.next().await {
    match event? {
        FollowEvent::Batch(batch) => {
            write_rows(&batch.rows).await?;
            save_checkpoint(cursor.advance(&batch)).await?;
        }
        FollowEvent::CaughtUp { .. }
        | FollowEvent::Stalled { .. }
        | FollowEvent::Skipped { .. } => {}
    }
}
```

The stream yields a retryable error as `Err` and keeps going after a backoff. Any other error
ends it.

`ProvisionalCursor` is for re-reading the hot tail with `latest_only`. It remembers each hot block
hash, from anchors and row `block_hash`. When a re-read disagrees, it returns
`Observation::Rollback { from }`: drop provisional data from `from` and query again from there.

```rust
let mut tail = ProvisionalCursor::new(heads.safe + 1);
loop {
    let response = client.query_all(&tail_request(tail.settled(), heads.latest)).await?;
    match tail.observe(&response) {
        Observation::Consistent => upsert_provisional(&response).await?,
        Observation::Rollback { from } => drop_provisional_from(from).await?,
    }
    heads = client.wait_heads(&chain, heads.latest, HeadKind::Latest, Duration::from_secs(30)).await?;
}
```

## TypeScript (`sdks/typescript`)

```ts
import { Client } from "@ringecosystem/datalens";

const client = new Client({ endpoint, application: "ormp", token });
for await (const event of client.follow({
  chain: "arbitrum",
  dataset: "evm.logs",
  selector: { addresses: [msgport], topics: [[messageSent]] },
  fields: ["transaction_from"],
  start: checkpoint,
})) {
  if (event.type === "batch") {
    await write(event.response.rows);
    checkpoint = event.response.next_start ?? event.response.requested.end + 1;
  } else if (event.type === "error") {
    console.warn(event.error.message);
  }
}
```

A failure that is not retryable is thrown. `npm ci && npm run build && npm test` runs the tests.

## Go (`sdks/go`, module `github.com/ringecosystem/datalens/sdks/go`)

```go
client := datalens.NewClient(datalens.ClientConfig{Endpoint: endpoint, Application: "ormp", Token: token})
events := client.Follow(ctx, datalens.FollowRequest{
	Chain: "arbitrum", Dataset: datalens.EvmLogs, Start: checkpoint,
	Selector: &datalens.LogSelector{Addresses: []string{msgport}, Topics: [][]string{{messageSent}}},
})
for event := range events {
	switch event.Kind {
	case datalens.FollowBatch:
		logs, err := event.Response.LogRows()
		// write logs, then save the checkpoint
	case datalens.FollowError:
		log.Print(event.Err) // the channel closes after an error that is not retryable
	}
}
```

`go test ./...` runs the tests.
