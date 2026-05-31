import { createHash } from "node:crypto";

import type {
  DecodedEvent,
  EventQuery,
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
    first: number;
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
  ethereum: EventQuery;
  solana: NativeQueryInput;
  tron: NativeQueryInput;
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
      first: intEnv(env, "DATALENS_ETHEREUM_FIRST", 10),
    },
    solana: {
      fromSlot: intEnv(env, "DATALENS_SOLANA_FROM_SLOT", 250000000),
      toSlot: intEnv(env, "DATALENS_SOLANA_TO_SLOT", 250000003),
    },
    tron: {
      fromBlock: intEnv(env, "DATALENS_TRON_FROM_BLOCK", 60000000),
      toBlock: intEnv(env, "DATALENS_TRON_TO_BLOCK", 60000002),
    },
  };
}

export function buildExampleQueries(config: RuntimeConfig): ExampleQueries {
  return {
    ethereum: {
      chain: "ethereum",
      chainId: 1,
      dataset: "evm.logs",
      address: ethereumUSDCAddress,
      eventName: "Transfer",
      signature: erc20TransferSignature,
      topic0: erc20TransferTopic0,
      fromBlock: config.ethereum.fromBlock,
      toBlock: config.ethereum.toBlock,
      first: config.ethereum.first,
    },
    solana: {
      chain: {
        family: { kind: "other", other: "solana" },
        configuredName: "solana-mainnet-beta",
        networkId: { textual: "mainnet-beta" },
      },
      datasetKey: { family: "solana", name: "account_updates" },
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
  const lines = [`${label} cache=${stableJson(response.cache)}`];
  for (const row of rowsFrom(response.rows)) {
    lines.push(`${label} row=${stableJson(row)}`);
  }
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
  if (rows != null && typeof rows === "object" && "rows" in rows && Array.isArray(rows.rows)) {
    return rows.rows;
  }
  return [];
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
