import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { after, before, test } from "node:test";

import {
  DatalensAuthError,
  DatalensClient,
  DatalensGraphQLError,
  DatalensHttpError,
  DatalensRateLimitError,
  type JsonValue,
} from "../src/index.js";

type RequestRecord = {
  body: {
    query?: string;
    variables?: Record<string, unknown>;
    operationName?: string;
  };
  headers: IncomingMessage["headers"];
  path: string;
};

const requests: RequestRecord[] = [];

let nextResponse:
  | {
      status?: number;
      headers?: Record<string, string>;
      body: unknown;
    }
  | undefined;
let queuedResponses: NonNullable<typeof nextResponse>[] = [];

const server = createServer(async (request: IncomingMessage, response: ServerResponse) => {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.from(chunk));
  }

  const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  requests.push({
    body,
    headers: request.headers,
    path: request.url ?? "",
  });

  const planned = queuedResponses.shift() ??
    nextResponse ?? {
    body: {
      data: {},
    },
  };
  nextResponse = undefined;

  response.statusCode = planned.status ?? 200;
  response.setHeader("content-type", "application/json");
  for (const [name, value] of Object.entries(planned.headers ?? {})) {
    response.setHeader(name, value);
  }
  response.end(JSON.stringify(planned.body));
});

let endpoint = "";

before(async () => {
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert(address && typeof address === "object");
  endpoint = `http://127.0.0.1:${address.port}`;
});

after(async () => {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
});

function resetServer(response: typeof nextResponse): void {
  requests.length = 0;
  queuedResponses = [];
  nextResponse = response;
}

test("queryDecodedEvents sends bearer auth and index GraphQL request shape", async () => {
  resetServer({
    body: {
      data: {
        decodedEventsConnection: {
          edges: [
            {
              cursor: "0",
              node: {
                blockNumber: 10,
                decodedArgs: { msgHash: "0xabc" },
                eventName: "MessageAccepted",
                logIndex: 0,
              },
            },
          ],
          nodes: [
            {
              blockNumber: 10,
              decodedArgs: { msgHash: "0xabc" },
              eventName: "MessageAccepted",
              logIndex: 0,
            },
          ],
          pageInfo: {
            endCursor: "0",
            hasNextPage: false,
          },
        },
      },
    },
  });

  const client = new DatalensClient({
    endpoint,
    token: "secret-token",
    application: "query-app",
  });

  const page = await client.index.queryDecodedEvents({
    dataset: "evm.logs",
    eventName: "MessageAccepted",
    first: 25,
  });

  assert.equal(requests[0].path, "/index/graphql");
  assert.equal(requests[0].headers.authorization, "Bearer secret-token");
  assert.equal(requests[0].headers["x-datalens-application"], "query-app");
  assert.match(requests[0].headers["user-agent"] as string, /^datalens-typescript-sdk\//);
  assert.match(requests[0].body.query ?? "", /decodedEventsConnection/);
  assert.deepEqual(requests[0].body.variables, {
    dataset: "evm.logs",
    eventName: "MessageAccepted",
    first: 25,
  });
  assert.equal(page.nodes[0].decodedArgs.msgHash, "0xabc");
});

test("queryRawEvents uses index GraphQL eventsConnection", async () => {
  resetServer({
    body: {
      data: {
        eventsConnection: {
          edges: [
            {
              cursor: "0",
              node: {
                blockNumber: 20,
                eventIndex: 1,
                payload: { raw: true },
                topics: ["0xtopic"],
              },
            },
          ],
          nodes: [
            {
              blockNumber: 20,
              eventIndex: 1,
              payload: { raw: true },
              topics: ["0xtopic"],
            },
          ],
          pageInfo: {
            endCursor: "0",
            hasNextPage: false,
          },
        },
      },
    },
  });

  const client = new DatalensClient({ endpoint });
  const page = await client.index.queryRawEvents({
    dataset: "evm.logs",
    first: 10,
    topic0: "0xtopic",
  });

  assert.equal(requests[0].path, "/index/graphql");
  assert.match(requests[0].body.query ?? "", /eventsConnection/);
  assert.equal((page.nodes[0].payload as { raw: JsonValue }).raw, true);
});

test("paginate follows cursor pageInfo until the connection is exhausted", async () => {
  const pages = [
    {
      data: {
        decodedEventsConnection: {
          edges: [{ cursor: "0", node: { blockNumber: 10, decodedArgs: {} } }],
          nodes: [{ blockNumber: 10, decodedArgs: {} }],
          pageInfo: { endCursor: "0", hasNextPage: true },
        },
      },
    },
    {
      data: {
        decodedEventsConnection: {
          edges: [{ cursor: "1", node: { blockNumber: 20, decodedArgs: {} } }],
          nodes: [{ blockNumber: 20, decodedArgs: {} }],
          pageInfo: { endCursor: "1", hasNextPage: false },
        },
      },
    },
  ];

  requests.length = 0;
  queuedResponses = pages.map((body) => ({ body }));
  nextResponse = undefined;

  const client = new DatalensClient({ endpoint });
  const blocks: number[] = [];

  for await (const event of client.index.paginateDecodedEvents({
    dataset: "evm.logs",
    first: 1,
  })) {
    blocks.push(event.blockNumber ?? 0);
  }

  assert.deepEqual(blocks, [10, 20]);
  assert.deepEqual(requests.map((request) => request.body.variables), [
    { dataset: "evm.logs", first: 1 },
    { dataset: "evm.logs", first: 1, after: "0" },
  ]);
});

test("native query and discovery use native GraphQL operations", async () => {
  resetServer({
    body: {
      data: {
        query: {
          chain: { configuredName: "ethereum" },
          datasetKey: "evm.blocks",
          range: { kind: "block", start: 10, end: 10 },
          cache: { hit_ranges: [] },
          rows: { rows: [{ number: 10 }] },
        },
      },
    },
  });

  const client = new DatalensClient({ endpoint });
  const query = await client.native.query({
    chain: {
      family: { kind: "evm" },
      configuredName: "ethereum",
      networkId: { numeric: 1 },
    },
    datasetKey: { family: "evm", name: "blocks" },
    selector: { kind: "all" },
    range: { kind: "block", start: 10, end: 10 },
    finality: "durable_only",
    fields: {},
  });

  assert.equal(requests[0].path, "/native/graphql");
  assert.match(requests[0].body.query ?? "", /query\(input: \$input\)/);
  assert.equal(query.datasetKey, "evm.blocks");

  resetServer({
    body: {
      data: {
        discovery: {
          chains: [
            {
              identity: { configuredName: "ethereum" },
              datasets: [
                {
                  datasetKey: "evm.blocks",
                  enabled: true,
                  rangeKinds: [{ kind: "block" }],
                  selectors: ["all"],
                },
              ],
            },
          ],
        },
      },
    },
  });

  const discovery = await client.native.discovery();
  assert.match(requests[0].body.query ?? "", /discovery/);
  assert.equal(discovery.chains[0].datasets[0].datasetKey, "evm.blocks");
});

test("GraphQL errors are exposed as stable SDK errors", async () => {
  resetServer({
    body: {
      errors: [
        {
          message: "rate limit exceeded",
          extensions: {
            code: "RATE_LIMITED",
            kind: "RateLimited",
          },
        },
      ],
      data: null,
    },
  });

  const client = new DatalensClient({ endpoint });

  await assert.rejects(
    client.index.queryRawEvents({ dataset: "evm.logs" }),
    (error: unknown) =>
      error instanceof DatalensGraphQLError &&
      error instanceof DatalensRateLimitError &&
      error.kind === "RateLimited",
  );
});

test("GraphQL auth errors are exposed as auth errors", async () => {
  resetServer({
    body: {
      errors: [
        {
          message: "invalid credentials",
          extensions: {
            code: "AUTHENTICATION_FAILED",
            kind: "AuthenticationFailed",
          },
        },
      ],
      data: null,
    },
  });

  const client = new DatalensClient({ endpoint });

  await assert.rejects(
    client.native.discovery(),
    (error: unknown) =>
      error instanceof DatalensGraphQLError &&
      error instanceof DatalensAuthError &&
      error.kind === "AuthenticationFailed",
  );
});

test("HTTP auth and rate-limit responses use specialized errors", async () => {
  const client = new DatalensClient({ endpoint });

  resetServer({
    status: 401,
    body: { error: { kind: "unauthorized", message: "missing token" } },
  });
  await assert.rejects(
    client.native.chains(),
    (error: unknown) => error instanceof DatalensAuthError && error.status === 401,
  );

  resetServer({
    status: 429,
    headers: { "retry-after": "30" },
    body: { error: { kind: "rate_limited", message: "too many requests" } },
  });
  await assert.rejects(
    client.native.chains(),
    (error: unknown) =>
      error instanceof DatalensRateLimitError &&
      error instanceof DatalensHttpError &&
      (error as DatalensRateLimitError).retryAfter === "30",
  );
});
