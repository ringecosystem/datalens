export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type FetchLike = (
  input: string | URL,
  init?: RequestInit,
) => Promise<Response>;

export type DatalensClientOptions = {
  endpoint: string;
  token?: string;
  application?: string;
  fetch?: FetchLike;
  timeoutMs?: number;
  userAgent?: string;
};

export type EventPageInfo = {
  endCursor?: string | null;
  hasNextPage: boolean;
};

export type ConnectionEdge<TNode> = {
  cursor: string;
  node: TNode;
};

export type ConnectionPage<TNode> = {
  edges: ConnectionEdge<TNode>[];
  nodes: TNode[];
  pageInfo: EventPageInfo;
};

export type EventQuery = {
  indexName?: string;
  chain?: string;
  chainId?: number;
  dataset: string;
  address?: string;
  eventName?: string;
  signature?: string;
  fromBlock?: number;
  toBlock?: number;
  topic0?: string;
  first?: number;
  after?: string;
};

export type DecodedEvent = {
  indexName?: string | null;
  chain?: string | null;
  chainId?: number | null;
  dataset?: string | null;
  blockNumber?: number | null;
  blockHash?: string | null;
  transactionHash?: string | null;
  transactionIndex?: number | null;
  logIndex?: number | null;
  address?: string | null;
  eventName?: string | null;
  signature?: string | null;
  topic0?: string | null;
  decodedArgs: Record<string, JsonValue>;
  decodeStatus?: string | null;
  decodeError?: string | null;
  payload?: JsonValue;
  createdAt?: string | null;
};

export type RawIndexedEvent = {
  indexName?: string | null;
  chain?: string | null;
  chainId?: number | null;
  dataset?: string | null;
  blockNumber?: number | null;
  blockHash?: string | null;
  transactionHash?: string | null;
  transactionIndex?: number | null;
  eventIndex?: number | null;
  address?: string | null;
  selector?: string | null;
  topics: string[];
  topic0?: string | null;
  signature?: string | null;
  eventName?: string | null;
  decoded: JsonValue;
  data?: string | null;
  payload: JsonValue;
  createdAt?: string | null;
};

export type ChainFamilyInput =
  | {
      kind: "evm";
      other?: never;
    }
  | {
      kind: "other";
      other: string;
    };

export type NetworkIdInput = {
  numeric?: number;
  textual?: string;
};

export type ChainIdentityInput = {
  family: ChainFamilyInput;
  configuredName: string;
  networkId?: NetworkIdInput;
};

export type DatasetKeyInput = {
  family: string;
  name: string;
};

export type QueryRangeInput = {
  kind: "block" | "slot" | "height";
  start: number;
  end: number;
};

export type QuerySelectorInput =
  | {
      kind: "all";
    }
  | {
      kind: "evm_logs";
      evmLogs: {
        addresses?: string[];
        topics?: (string[] | null)[];
      };
    }
  | {
      kind: "other";
      other: {
        kind: string;
        fingerprint: string;
        canonicalKey: string;
      };
    };

export type FieldSelectionInput = {
  include?: string[];
};

export type NativeQueryInput = {
  chain: ChainIdentityInput;
  datasetKey: DatasetKeyInput;
  selector: QuerySelectorInput;
  range: QueryRangeInput;
  finality?: string;
  fields?: FieldSelectionInput;
};

export type NativeQueryResponse = {
  chain: JsonValue;
  datasetKey: string;
  range: JsonValue;
  cache: JsonValue;
  rows: JsonValue;
};

export type DatasetDiscovery = {
  datasetKey: string;
  rangeKinds: JsonValue;
  selectors: string[];
  enabled: boolean;
};

export type ChainDiscovery = {
  identity: JsonValue;
  datasets: DatasetDiscovery[];
};

export type Discovery = {
  chains: ChainDiscovery[];
};
