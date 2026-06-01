import { createHash } from "node:crypto";

import type {
  DecodedEvent,
  JsonValue,
  NativeQueryInput,
  NativeQueryResponse,
} from "@helixbox/datalens";

export const ethereumUSDCAddress = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
export const solanaUSDCMint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
export const tronUSDTContract = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
export const tronUSDTContractHex = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c";
export const erc20TransferSignature = "Transfer(address,address,uint256)";
export const erc20TransferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

export type RuntimeConfig = {
  endpoint: string;
  token?: string;
  application: string;
  ethereum: {
    fromBlock: number;
    toBlock: number;
  };
  solana: {
    fromSlot: number;
    toSlot: number;
  };
  tron: {
    fromBlock: number;
    toBlock: number;
  };
};

export type ExampleQueries = {
  ethereum: NativeQueryInput;
  solana: NativeQueryInput;
  tron: NativeQueryInput;
};

export type ExampleClient = {
  native: {
    query(query: NativeQueryInput): Promise<NativeQueryResponse>;
  };
};

type Environment = Record<string, string | undefined>;

export function buildRuntimeConfig(env: Environment = process.env): RuntimeConfig {
  return {
    endpoint: stringEnv(env, "DATALENS_ENDPOINT", "http://127.0.0.1:3000"),
    token: optionalStringEnv(env, "DATALENS_TOKEN"),
    application: stringEnv(env, "DATALENS_APPLICATION", "token-sdk-typescript"),
    ethereum: {
      fromBlock: intEnv(env, "DATALENS_ETHEREUM_FROM_BLOCK", 19000000),
      toBlock: intEnv(env, "DATALENS_ETHEREUM_TO_BLOCK", 19000010),
    },
    solana: {
      fromSlot: intEnv(env, "DATALENS_SOLANA_FROM_SLOT", 250000000),
      toSlot: intEnv(env, "DATALENS_SOLANA_TO_SLOT", 250000003),
    },
    tron: {
      fromBlock: intEnv(env, "DATALENS_TRON_FROM_BLOCK", 83200000),
      toBlock: intEnv(env, "DATALENS_TRON_TO_BLOCK", 83200002),
    },
  };
}

export function buildExampleQueries(config: RuntimeConfig): ExampleQueries {
  return {
    ethereum: {
      chain: {
        family: { kind: "evm" },
        configuredName: "ethereum",
        networkId: { numeric: 1 },
      },
      datasetKey: { family: "evm", name: "logs" },
      selector: {
        kind: "evm_logs",
        evmLogs: {
          addresses: [ethereumUSDCAddress],
          topics: [[erc20TransferTopic0]],
        },
      },
      range: {
        kind: "block",
        start: config.ethereum.fromBlock,
        end: config.ethereum.toBlock,
      },
      finality: "durable_only",
    },
    solana: {
      chain: {
        family: { kind: "other", other: "solana" },
        configuredName: "solana-mainnet-beta",
        networkId: { numeric: 101 },
      },
      datasetKey: { family: "solana", name: "transactions" },
      selector: otherSelector("solana_address", "address", solanaUSDCMint, "solana-address"),
      range: {
        kind: "slot",
        start: config.solana.fromSlot,
        end: config.solana.toSlot,
      },
      finality: "durable_only",
    },
    tron: {
      chain: {
        family: { kind: "other", other: "tron" },
        configuredName: "tron-mainnet",
        networkId: { numeric: 728126428 },
      },
      datasetKey: { family: "tron", name: "events" },
      selector: tronEventSelector(tronUSDTContractHex, "Transfer"),
      range: {
        kind: "block",
        start: config.tron.fromBlock,
        end: config.tron.toBlock,
      },
      finality: "durable_only",
    },
  };
}

export function formatDecodedTransfers(events: DecodedEvent[]): string[] {
  return events.map((event) => {
    const decoded = event.decodedArgs;
    return [
      "ethereum-usdc",
      `block=${event.blockNumber ?? ""}`,
      `tx=${event.transactionHash ?? ""}`,
      `log=${event.logIndex ?? ""}`,
      `from=${stringValue(decoded.from)}`,
      `to=${stringValue(decoded.to)}`,
      `value=${stringValue(decoded.value)}`,
    ].join(" ");
  });
}

export function formatNativeRows(label: string, response: NativeQueryResponse): string[] {
  const rows = rowsFrom(response.rows);
  const lines = [`${label} rows=${rows.length} cache=${stableJson(response.cache)}`];
  for (const row of rows) {
    lines.push(`${label} row=${stableJson(row)}`);
  }
  return lines;
}

export async function runExample(client: ExampleClient, queries: ExampleQueries): Promise<string[]> {
  const ethereum = await client.native.query(queries.ethereum);
  const lines = formatNativeRows("ethereum-usdc", ethereum);

  const solana = await client.native.query(queries.solana);
  lines.push(...formatNativeRows("solana-usdc", solana));

  const tron = await client.native.query(queries.tron);
  lines.push(...formatNativeRows("tron-usdt", tron));

  return lines;
}

function otherSelector(kind: string, keyPrefix: string, value: string, fingerprintPrefix: string): NativeQueryInput["selector"] {
  return {
    kind: "other",
    other: {
      kind,
      fingerprint: `${fingerprintPrefix}/${digestPrefix(value)}`,
      canonicalKey: `${keyPrefix}/${value}`,
    },
  };
}

function tronEventSelector(contractHex: string, eventName: string): NativeQueryInput["selector"] {
  const canonicalKey = `contracts/${contractHex}/events/${eventName}`;
  return {
    kind: "other",
    other: {
      kind: "tron_events",
      fingerprint: `tron-events/${digestPrefix(canonicalKey, 12)}`,
      canonicalKey,
    },
  };
}

function digestPrefix(value: string, bytes = 8): string {
  return createHash("sha256").update(value).digest("hex").slice(0, bytes * 2);
}

function rowsFrom(rows: JsonValue): JsonValue[] {
  if (Array.isArray(rows)) {
    return rows;
  }
  if (isJsonObject(rows) && "rows" in rows) {
    return rowsFrom(rows.rows);
  }
  return [];
}

function isJsonObject(value: JsonValue): value is { [key: string]: JsonValue } {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function stableJson(value: JsonValue): string {
  return JSON.stringify(value);
}

function stringValue(value: JsonValue | undefined): string {
  if (value == null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  return JSON.stringify(value);
}

function stringEnv(env: Environment, name: string, fallback: string): string {
  const value = env[name]?.trim();
  return value === undefined || value === "" ? fallback : value;
}

function optionalStringEnv(env: Environment, name: string): string | undefined {
  const value = env[name]?.trim();
  return value === undefined || value === "" ? undefined : value;
}

function intEnv(env: Environment, name: string, fallback: number): number {
  const value = env[name]?.trim();
  if (value === undefined || value === "") {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be an integer`);
  }
  return parsed;
}
