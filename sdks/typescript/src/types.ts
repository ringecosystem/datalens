/** Wire types of the datalens HTTP API. Field names are the server's, in snake_case. */

export type Dataset = "evm.logs" | "evm.blocks";

/** Which part of the chain a query may read. */
export type QueryFinality = "durable_only" | "safe_to_latest" | "latest_only";

export type Finality = "unsafe" | "safe" | "finalized";

export type IncompleteReason =
  | "pending"
  | "provider_error"
  | "budget_exhausted"
  | "not_followed_yet";

export type SegmentSource = "durable" | "hot";

export type HeadKind = "latest" | "safe" | "finalized";

/** Inclusive block range. */
export interface BlockRange {
  start: number;
  end: number;
}

/** `evm.logs` selector. A `null` topic position matches any topic. */
export interface LogSelector {
  addresses?: string[];
  topics?: (string[] | null)[];
}

export interface PageRequest {
  limit?: number;
  cursor?: string;
}

export interface QueryRequest {
  chain: string;
  dataset: Dataset;
  selector?: LogSelector;
  range: BlockRange;
  finality?: QueryFinality;
  /** Omitted means every base field. `block_timestamp` and `transaction_from` only when listed. */
  fields?: string[];
  page?: PageRequest;
  wait_ms?: number;
}

export interface Anchor {
  number: number;
  hash: string;
}

export interface Segment {
  start: number;
  end: number;
  source: SegmentSource;
  finality: Finality;
  /** Hot segments only: the follow head, possibly above `end`. */
  anchor?: Anchor;
}

export interface LogRecord {
  block_number: number;
  block_hash: string;
  block_timestamp?: number;
  transaction_hash: string;
  transaction_index: number;
  log_index: number;
  address: string;
  topics: string[];
  data: string;
  finality: Finality;
  transaction_from?: string;
}

/** Header fields beyond the named ones keep their JSON-RPC names, such as `gasUsed`. */
export interface BlockRecord {
  number: number;
  hash: string;
  parent_hash: string;
  timestamp: number;
  finality: Finality;
  [field: string]: unknown;
}

export interface QueryResponse<Row = LogRecord | BlockRecord> {
  chain: string;
  dataset: Dataset;
  requested: BlockRange;
  /** Contiguous prefix of `requested`. Null when nothing is served. */
  served: BlockRange | null;
  /** First block not served. Null when the whole request is served. */
  next_start: number | null;
  incomplete_reason: IncompleteReason | null;
  incomplete_detail?: string | null;
  segments: Segment[];
  rows: Row[];
  page: { next_cursor: string | null };
}

export interface ChainHeads {
  chain: string;
  latest: number;
  latest_hash: string;
  safe: number;
  finalized: number;
}

export interface WatchRequest {
  chain: string;
  dataset: Dataset;
  selector?: LogSelector;
}

export interface FollowInfo {
  from_block: number;
  head_block: number | null;
  head_hash: string | null;
  promoted_to: number | null;
}

export interface WatchResponse {
  chain: string;
  dataset: Dataset;
  selector_id: number;
  ttl_ms: number;
  follow: FollowInfo;
}
