import assert from "node:assert/strict";
import { test } from "node:test";

import type { NativeQueryResponse } from "@helixbox/datalens";
import {
  buildExampleQueries,
  buildRuntimeConfig,
  formatDecodedTransfers,
  formatNativeRows,
  runExample,
} from "../src/example.js";

test("runExample uses native queries only for live smoke", async () => {
  const calls: string[] = [];
  const queries = buildExampleQueries({
    endpoint: "http://127.0.0.1:3000",
    application: "token-sdk-typescript",
    ethereum: { fromBlock: 19000000, toBlock: 19000010, first: 3 },
    solana: { fromSlot: 250000000, toSlot: 250000003 },
    tron: { fromBlock: 60000000, toBlock: 60000002 },
  });

  const lines = await runExample(
    {
      index: {
        async queryDecodedEvents() {
          calls.push("index");
          return { edges: [], nodes: [], pageInfo: { hasNextPage: false, hasPreviousPage: false } };
        },
      },
      native: {
        async query(input) {
          calls.push(`${input.datasetKey.family}.${input.datasetKey.name}`);
          return {
            chain: { configuredName: input.chain.configuredName },
            datasetKey: `${input.datasetKey.family}.${input.datasetKey.name}`,
            range: input.range,
            cache: { outcome: "miss" },
            rows: [],
          };
        },
      },
    },
    queries,
  );

  assert.deepEqual(calls, ["solana.account_updates", "tron.events"]);
  assert.deepEqual(lines, [
    "solana-usdc cache={\"outcome\":\"miss\"}",
    "tron-usdt cache={\"outcome\":\"miss\"}",
  ]);
});

test("buildExampleQueries uses official token targets and bounded ranges", () => {
  const queries = buildExampleQueries({
    endpoint: "http://127.0.0.1:3000",
    application: "token-sdk-typescript",
    ethereum: { fromBlock: 19000000, toBlock: 19000010, first: 3 },
    solana: { fromSlot: 250000000, toSlot: 250000003 },
    tron: { fromBlock: 60000000, toBlock: 60000002 },
  });

  assert.deepEqual(queries.ethereum, {
    chain: "ethereum",
    chainId: 1,
    dataset: "evm.logs",
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    eventName: "Transfer",
    signature: "Transfer(address,address,uint256)",
    topic0: "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    fromBlock: 19000000,
    toBlock: 19000010,
    first: 3,
  });

  assert.equal(queries.solana.datasetKey.name, "account_updates");
  assert.deepEqual(queries.solana.selector, {
    kind: "other",
    other: {
      kind: "solana_address",
      fingerprint: "solana-address/f249bbf137c2e667",
      canonicalKey: "address/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    },
  });
  assert.deepEqual(queries.solana.range, { kind: "slot", start: 250000000, end: 250000003 });

  assert.equal(queries.tron.datasetKey.name, "events");
  assert.deepEqual(queries.tron.selector, {
    kind: "other",
    other: {
      kind: "tron_events",
      fingerprint: "tron-events/8b35d4984847524df4944061",
      canonicalKey: "contracts/41a614f803b6fd780986a42c78ec9c7f77e6ded13c/events/Transfer",
    },
  });
  assert.deepEqual(queries.tron.range, { kind: "block", start: 60000000, end: 60000002 });
});

test("buildRuntimeConfig reads endpoint token application and range overrides", () => {
  const config = buildRuntimeConfig({
    DATALENS_ENDPOINT: "http://datalens.example",
    DATALENS_TOKEN: "secret-token",
    DATALENS_APPLICATION: "demo-app",
    DATALENS_ETHEREUM_FROM_BLOCK: "19100000",
    DATALENS_ETHEREUM_TO_BLOCK: "19100001",
    DATALENS_ETHEREUM_FIRST: "2",
    DATALENS_SOLANA_FROM_SLOT: "251000000",
    DATALENS_SOLANA_TO_SLOT: "251000005",
    DATALENS_TRON_FROM_BLOCK: "60100000",
    DATALENS_TRON_TO_BLOCK: "60100004",
  });

  assert.equal(config.endpoint, "http://datalens.example");
  assert.equal(config.token, "secret-token");
  assert.equal(config.application, "demo-app");
  assert.deepEqual(config.ethereum, { fromBlock: 19100000, toBlock: 19100001, first: 2 });
  assert.deepEqual(config.solana, { fromSlot: 251000000, toSlot: 251000005 });
  assert.deepEqual(config.tron, { fromBlock: 60100000, toBlock: 60100004 });
});

test("formatters print normalized event and cache summaries", () => {
  const decoded = formatDecodedTransfers([
    {
      blockNumber: 19000000,
      transactionHash: "0xabc",
      logIndex: 1,
      decodedArgs: {
        from: "0x1111111111111111111111111111111111111111",
        to: "0x2222222222222222222222222222222222222222",
        value: "1000000",
      },
    },
  ]);

  assert.deepEqual(decoded, [
    "ethereum-usdc block=19000000 tx=0xabc log=1 from=0x1111111111111111111111111111111111111111 to=0x2222222222222222222222222222222222222222 value=1000000",
  ]);

  const nativeResponse: NativeQueryResponse = {
    chain: { configuredName: "solana-mainnet-beta" },
    datasetKey: "solana.account_updates",
    range: { kind: "slot", start: 250000000, end: 250000003 },
    cache: { outcome: "hit", hit_ranges: [{ start: 250000000, end: 250000003 }] },
    rows: {
      rows: [
        {
          slot: 250000001,
          signature: "5sig",
          account: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
          post_amount: "42",
        },
      ],
    },
  };

  assert.deepEqual(formatNativeRows("solana-usdc", nativeResponse), [
    "solana-usdc cache={\"outcome\":\"hit\",\"hit_ranges\":[{\"start\":250000000,\"end\":250000003}]}",
    "solana-usdc row={\"slot\":250000001,\"signature\":\"5sig\",\"account\":\"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v\",\"post_amount\":\"42\"}",
  ]);
});
