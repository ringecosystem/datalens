import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { after, test } from "node:test";

import {
  Client,
  DatalensError,
  type ChainHeads,
  type FollowEvent,
  type QueryRequest,
  type QueryResponse,
} from "../src/index.js";

interface Fake {
  safe: number;
  latest: number;
  requests: { method: string; url: string; headers: IncomingMessage["headers"]; body: any }[];
  /** Replies to the next requests, before the default behavior. */
  script: { status: number; body: string; headers?: Record<string, string> }[];
  /** `range.start` values whose first query stops two blocks in with `pending`. */
  pendingAt: Set<number>;
  onWait?: () => void;
}

const fakes: { close(): void }[] = [];
after(() => fakes.forEach((server) => server.close()));

function heads(fake: Fake): ChainHeads {
  return {
    chain: "ethereum",
    latest: fake.latest,
    latest_hash: `0x${"aa".repeat(32)}`,
    safe: fake.safe,
    finalized: fake.safe - 4,
  };
}

function row(block: number) {
  return { block_number: block, log_index: 0, block_hash: `0x${block.toString(16).padStart(64, "0")}` };
}

function query(fake: Fake, request: QueryRequest): QueryResponse | undefined {
  const { start } = request.range;
  let end = request.range.end;
  if (end > fake.safe) {
    return undefined;
  }
  const pending = fake.pendingAt.delete(start);
  if (pending) {
    end = start + 1;
  }
  const limit = request.page?.limit ?? 10_000;
  const from = request.page?.cursor === undefined ? start : Number(request.page.cursor);
  const to = Math.min(end, from + limit - 1);
  const rows = [];
  for (let block = from; block <= to; block++) {
    rows.push(row(block));
  }
  return {
    chain: request.chain,
    dataset: request.dataset,
    requested: request.range,
    served: { start, end },
    next_start: end < request.range.end ? end + 1 : null,
    incomplete_reason: pending ? "pending" : null,
    incomplete_detail: null,
    segments: [{ start, end, source: "durable", finality: "safe" }],
    rows: rows as never,
    page: { next_cursor: to < end ? String(to + 1) : null },
  };
}

async function fake(init: Partial<Fake> = {}): Promise<{ fake: Fake; client: Client }> {
  const state: Fake = { safe: 9, latest: 17, requests: [], script: [], pendingAt: new Set(), ...init };
  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    let text = "";
    for await (const chunk of req) {
      text += chunk;
    }
    const body = text === "" ? undefined : JSON.parse(text);
    state.requests.push({ method: req.method ?? "", url: req.url ?? "", headers: req.headers, body });
    const reply = (status: number, value: unknown, headers: Record<string, string> = {}) => {
      res.writeHead(status, { "content-type": "application/json", ...headers });
      res.end(typeof value === "string" ? value : JSON.stringify(value));
    };
    const scripted = state.script.shift();
    if (scripted) {
      return reply(scripted.status, scripted.body, scripted.headers);
    }
    const url = new URL(req.url ?? "/", "http://fake");
    if (url.pathname === "/v1/heads/ethereum") {
      return reply(200, heads(state));
    }
    if (url.pathname === "/v1/heads/ethereum/wait") {
      state.onWait?.();
      return reply(200, heads(state));
    }
    if (url.pathname === "/v1/query") {
      const response = query(state, body);
      return response
        ? reply(200, response)
        : reply(400, { error: { kind: "invalid_input", message: "above the safe head" } });
    }
    if (url.pathname === "/v1/watch" && req.method === "DELETE") {
      res.writeHead(204);
      return res.end();
    }
    reply(404, { error: { kind: "invalid_input", message: "no route" } });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  fakes.push(server);
  const { port } = server.address() as AddressInfo;
  const client = new Client({
    endpoint: `http://127.0.0.1:${port}/`,
    application: "indexer",
    token: "secret",
    retry: { maxAttempts: 3, initialBackoffMs: 1, maxBackoffMs: 2 },
  });
  return { fake: state, client };
}

const request: QueryRequest = {
  chain: "ethereum",
  dataset: "evm.logs",
  selector: { addresses: ["0xb1"], topics: [["0x11"]] },
  range: { start: 0, end: 6 },
  page: { limit: 3 },
};

test("query sends the application headers and queryAll merges every page", async () => {
  const { fake: state, client } = await fake();
  const first = await client.query(request);
  assert.deepEqual(
    first.rows.map((r: any) => r.block_number),
    [0, 1, 2],
  );
  assert.equal(first.page.next_cursor, "3");
  assert.equal(state.requests[0].headers["x-datalens-application"], "indexer");
  assert.equal(state.requests[0].headers.authorization, "Bearer secret");
  assert.deepEqual(state.requests[0].body, request);

  const all = await client.queryAll(request);
  assert.deepEqual(
    all.rows.map((r: any) => r.block_number),
    [0, 1, 2, 3, 4, 5, 6],
  );
  assert.deepEqual(all.served, { start: 0, end: 6 });
  assert.equal(all.page.next_cursor, null);
  assert.deepEqual(
    state.requests.slice(1).map((r) => r.body.page),
    [{ limit: 3 }, { limit: 3, cursor: "3" }, { limit: 3, cursor: "6" }],
  );
});

test("errors carry kind and status, and only retryable ones are retried", async () => {
  const { fake: state, client } = await fake({
    script: [
      { status: 403, body: '{"error":{"kind":"forbidden","message":"may not read base"}}' },
    ],
  });
  const forbidden = await client.heads("ethereum").catch((error) => error);
  assert.ok(forbidden instanceof DatalensError);
  assert.equal(forbidden.type, "api");
  assert.equal(forbidden.kind, "forbidden");
  assert.equal(forbidden.status, 403);
  assert.equal(forbidden.message, "datalens 403 forbidden: may not read base");
  assert.equal(forbidden.isRetryable(), false);
  assert.equal(state.requests.length, 1);

  state.script.push(
    { status: 503, body: "upstream down" },
    { status: 429, body: '{"error":{"kind":"quota_exceeded","message":"rpm"}}', headers: { "retry-after": "0" } },
  );
  assert.equal((await client.heads("ethereum")).safe, 9);
  assert.equal(state.requests.length, 4);

  state.script.push(
    { status: 502, body: "<html>bad gateway</html>" },
    { status: 502, body: "<html>bad gateway</html>" },
    { status: 502, body: "<html>bad gateway</html>" },
  );
  const gateway = await client.heads("ethereum").catch((error) => error);
  assert.equal(gateway.kind, "http");
  assert.equal(gateway.status, 502);
  assert.equal(gateway.isRetryable(), true);
  assert.equal(state.requests.length, 7);
});

test("waitHeads sends after, kind, and timeout_ms; unwatch accepts 204", async () => {
  const { fake: state, client } = await fake();
  const passed = await client.waitHeads("ethereum", 8, "safe", 250);
  assert.equal(passed.safe, 9);
  assert.equal(state.requests[0].url, "/v1/heads/ethereum/wait?after=8&kind=safe&timeout_ms=250");
  await client.unwatch({ chain: "ethereum", dataset: "evm.logs", selector: request.selector });
  assert.equal(state.requests[1].method, "DELETE");
});

function summary(event: FollowEvent): unknown {
  switch (event.type) {
    case "batch":
      return ["batch", event.response.served, event.response.rows.length];
    case "caught_up":
      return ["caught_up", event.head];
    case "stalled":
      return ["stalled", event.nextStart, event.reason];
    case "error":
      return ["error", event.error.status];
    case "skipped":
      return ["skipped", event.blocks];
  }
}

test("follow reads in chunks, waits on heads when caught up, and resumes at next_start", async () => {
  const { fake: state, client } = await fake({ pendingAt: new Set([10]) });
  state.onWait = () => {
    state.safe = 14;
  };
  const events = [];
  for await (const event of client.follow({
    chain: "ethereum",
    dataset: "evm.logs",
    selector: request.selector,
    start: 0,
    end: 14,
    chunkBlocks: 5,
    pageLimit: 2,
    pollIntervalMs: 1,
  })) {
    events.push(summary(event));
  }
  assert.deepEqual(events, [
    ["batch", { start: 0, end: 4 }, 5],
    ["batch", { start: 5, end: 9 }, 5],
    ["caught_up", 9],
    ["batch", { start: 10, end: 11 }, 2],
    ["stalled", 12, "pending"],
    ["batch", { start: 12, end: 14 }, 3],
  ]);
  const waits = state.requests.filter((r) => r.url.startsWith("/v1/heads/ethereum/wait"));
  assert.deepEqual(
    waits.map((r) => r.url),
    ["/v1/heads/ethereum/wait?after=9&kind=safe&timeout_ms=30000"],
  );
});

test("follow reports a retryable failure as an event and carries on", async () => {
  const { client } = await fake({
    script: [
      { status: 503, body: "down" },
      { status: 503, body: "down" },
      { status: 503, body: "down" },
    ],
  });
  const events = [];
  for await (const event of client.follow({
    chain: "ethereum",
    dataset: "evm.logs",
    selector: request.selector,
    start: 0,
    end: 3,
  })) {
    events.push(summary(event));
  }
  assert.deepEqual(events, [
    ["error", 503],
    ["batch", { start: 0, end: 3 }, 4],
  ]);
});

test("follow throws a failure that is not retryable", async () => {
  const { client } = await fake({
    script: [{ status: 401, body: '{"error":{"kind":"unauthorized","message":"bad token"}}' }],
  });
  const iterator = client.follow({ chain: "ethereum", dataset: "evm.logs", start: 0 });
  await assert.rejects(iterator.next(), (error: DatalensError) => error.kind === "unauthorized");
});

function page(hash: string, cursor: string | null): string {
  return JSON.stringify({
    chain: "ethereum",
    dataset: "evm.logs",
    requested: { start: 10, end: 11 },
    served: { start: 10, end: 11 },
    next_start: null,
    incomplete_reason: null,
    segments: [],
    rows: [{ block_number: 10, log_index: 0, block_hash: hash }],
    page: { next_cursor: cursor },
  });
}

test("queryAll reads again when a later page disagrees on a block hash", async () => {
  const hash = (salt: string) => `0x${salt.repeat(32)}`;
  const { fake: state, client } = await fake({
    script: [
      { status: 200, body: page(hash("11"), "c") },
      { status: 200, body: page(hash("22"), null) },
      { status: 200, body: page(hash("11"), "c") },
      { status: 200, body: page(hash("11"), null) },
    ],
  });
  const merged = await client.queryAll({
    chain: "ethereum",
    dataset: "evm.logs",
    range: { start: 10, end: 11 },
  });
  assert.equal(merged.rows.length, 2);
  assert.equal(state.requests.length, 4);
});

test("latest_only follow names blocks the server left below a moved safe head", async () => {
  const headsBody = JSON.stringify({
    chain: "ethereum",
    latest: 17,
    latest_hash: `0x${"aa".repeat(32)}`,
    safe: 9,
    finalized: 4,
  });
  const queryBody = JSON.stringify({
    chain: "ethereum",
    dataset: "evm.logs",
    requested: { start: 10, end: 14 },
    served: { start: 12, end: 14 },
    next_start: null,
    incomplete_reason: "below_safe",
    segments: [],
    rows: [],
    page: { next_cursor: null },
  });
  const { fake: state, client } = await fake({
    script: [
      { status: 200, body: headsBody },
      { status: 200, body: queryBody },
    ],
  });
  const events = [];
  for await (const event of client.follow({
    chain: "ethereum",
    dataset: "evm.logs",
    finality: "latest_only",
    start: 10,
    end: 14,
  })) {
    events.push(summary(event));
  }
  assert.deepEqual(events, [
    ["skipped", { start: 10, end: 11 }],
    ["batch", { start: 12, end: 14 }, 0],
  ]);
  assert.equal(state.requests.length, 2);
});

test("a response integer past Number.MAX_SAFE_INTEGER is a decode error", async () => {
  const { client } = await fake({
    script: [{ status: 200, body: '{"latest":9007199254740993}' }],
  });
  await assert.rejects(client.heads("ethereum"), (error: DatalensError) => {
    assert.equal(error.type, "decode");
    assert.match(error.message, /9007199254740993/);
    return true;
  });
});
