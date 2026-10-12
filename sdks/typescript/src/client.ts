import { DatalensError } from "./errors.js";
import { follow, type FollowEvent, type FollowOptions, type FollowRequest } from "./follow.js";
import type {
  ChainHeads,
  HeadKind,
  QueryRequest,
  QueryResponse,
  WatchRequest,
  WatchResponse,
} from "./types.js";

/** Retries of one call. The delay doubles from `initialBackoffMs` up to `maxBackoffMs`,
 * scaled by a random factor in [0.5, 1). `Retry-After` overrides it. */
export interface RetryConfig {
  /** Total tries, the first included. 1 disables retries. */
  maxAttempts: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
}

export const defaultRetry: RetryConfig = {
  maxAttempts: 4,
  initialBackoffMs: 250,
  maxBackoffMs: 10_000,
};

export function backoffMs(retry: RetryConfig, failures: number): number {
  const delay = Math.min(
    retry.initialBackoffMs * 2 ** Math.min(Math.max(failures - 1, 0), 20),
    retry.maxBackoffMs,
  );
  return delay * (0.5 + Math.random() / 2);
}

export interface ClientConfig {
  /** Base URL such as `https://datalens.example.com`. */
  endpoint: string;
  application: string;
  token: string;
  /** Per request. A heads long-poll adds its own wait on top. Default 30 s. */
  timeoutMs?: number;
  retry?: Partial<RetryConfig>;
  fetch?: typeof fetch;
}

/** One application's view of a datalens server. */
export class Client {
  readonly retry: RetryConfig;
  readonly #endpoint: string;
  readonly #application: string;
  readonly #token: string;
  readonly #timeoutMs: number;
  readonly #fetch: typeof fetch;

  constructor(config: ClientConfig) {
    this.#endpoint = config.endpoint.replace(/\/+$/, "");
    this.#application = config.application;
    this.#token = config.token;
    this.#timeoutMs = config.timeoutMs ?? 30_000;
    this.retry = { ...defaultRetry, ...config.retry };
    this.#fetch = config.fetch ?? fetch;
  }

  /** One page. `page.next_cursor` names the next page of the same request. */
  query<Row = QueryResponse["rows"][number]>(
    request: QueryRequest,
    signal?: AbortSignal,
  ): Promise<QueryResponse<Row>> {
    return this.#call("POST", "/v1/query", { body: request, signal });
  }

  /** Every page of `request`, rows concatenated. The other fields come from the last page,
   * whose `served` contains the rows of every earlier page. When two pages disagree on a
   * block hash, a reorg landed between them, so the whole request is read again. */
  async queryAll<Row = QueryResponse["rows"][number]>(
    request: QueryRequest,
    signal?: AbortSignal,
  ): Promise<QueryResponse<Row>> {
    for (let failures = 1; ; failures++) {
      const merged = await this.#pages<Row>(request, signal);
      if (merged !== undefined) {
        return merged;
      }
      if (failures >= this.retry.maxAttempts) {
        throw new DatalensError(
          "inconsistent",
          `datalens inconsistent: pages of ${request.range.start}..=${request.range.end} disagreed on block hashes ${failures} times`,
        );
      }
      await sleep(backoffMs(this.retry, failures), signal);
    }
  }

  /** `undefined` when a later page names a block with another hash than an earlier page. */
  async #pages<Row>(
    request: QueryRequest,
    signal?: AbortSignal,
  ): Promise<QueryResponse<Row> | undefined> {
    let merged = await this.query<Row>(request, signal);
    const rows = [...merged.rows];
    let known = blockHashes(merged);
    while (merged.page.next_cursor !== null) {
      const page = { limit: request.page?.limit, cursor: merged.page.next_cursor };
      merged = await this.query<Row>({ ...request, page }, signal);
      const hashes = blockHashes(merged);
      for (const [number, hash] of hashes) {
        const seen = known.get(number);
        if (seen !== undefined && seen !== hash) {
          return undefined;
        }
      }
      known = new Map([...known, ...hashes]);
      rows.push(...merged.rows);
    }
    return { ...merged, rows };
  }

  heads(chain: string, signal?: AbortSignal): Promise<ChainHeads> {
    return this.#call("GET", `/v1/heads/${encodeURIComponent(chain)}`, { signal });
  }

  /** Heads of every chain this application may read. */
  async allHeads(signal?: AbortSignal): Promise<ChainHeads[]> {
    const response = await this.#call<{ chains: ChainHeads[] }>("GET", "/v1/heads", { signal });
    return response.chains;
  }

  /** Resolves once head `kind` passes `after`, or with the unchanged heads after `timeoutMs`.
   * The server caps `timeoutMs` at 60 s. */
  waitHeads(
    chain: string,
    after: number,
    kind: HeadKind,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<ChainHeads> {
    const query = new URLSearchParams({
      after: String(after),
      kind,
      timeout_ms: String(timeoutMs),
    });
    return this.#call("GET", `/v1/heads/${encodeURIComponent(chain)}/wait?${query}`, {
      signal,
      extraTimeoutMs: timeoutMs,
    });
  }

  /** Subscribes the selector for the server's TTL, or renews it. */
  watch(request: WatchRequest, signal?: AbortSignal): Promise<WatchResponse> {
    return this.#call("POST", "/v1/watch", { body: request, signal });
  }

  async unwatch(request: WatchRequest, signal?: AbortSignal): Promise<void> {
    await this.#call("DELETE", "/v1/watch", { body: request, signal, empty: true });
  }

  /** Reads `request.start` on chunk by chunk, then follows the head. A retryable failure
   * comes out as an `error` event followed by a backoff. Any other failure is thrown. */
  follow(request: FollowRequest, options: FollowOptions = {}): AsyncGenerator<FollowEvent> {
    return follow(this, request, options);
  }

  async #call<T>(
    method: string,
    path: string,
    options: { body?: unknown; signal?: AbortSignal; extraTimeoutMs?: number; empty?: boolean },
  ): Promise<T> {
    for (let failures = 1; ; failures++) {
      try {
        return await this.#once<T>(method, path, options);
      } catch (error) {
        if (
          !(error instanceof DatalensError) ||
          !error.isRetryable() ||
          failures >= this.retry.maxAttempts ||
          options.signal?.aborted
        ) {
          throw error;
        }
        await sleep(error.retryAfterMs ?? backoffMs(this.retry, failures), options.signal);
      }
    }
  }

  async #once<T>(
    method: string,
    path: string,
    options: { body?: unknown; signal?: AbortSignal; extraTimeoutMs?: number; empty?: boolean },
  ): Promise<T> {
    const timeout = AbortSignal.timeout(this.#timeoutMs + (options.extraTimeoutMs ?? 0));
    const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
    const headers: Record<string, string> = {
      authorization: `Bearer ${this.#token}`,
      "x-datalens-application": this.#application,
    };
    if (options.body !== undefined) {
      headers["content-type"] = "application/json";
    }
    let response: Response;
    let text: string;
    try {
      response = await this.#fetch(`${this.#endpoint}${path}`, {
        method,
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal,
      });
      text = await response.text();
    } catch (cause) {
      if (options.signal?.aborted) {
        throw cause;
      }
      throw new DatalensError("http", `datalens http: ${String(cause)}`, { cause });
    }
    if (!response.ok) {
      throw apiError(response, text);
    }
    if (options.empty) {
      return undefined as T;
    }
    try {
      assertSafeJsonIntegers(text);
      return JSON.parse(text) as T;
    } catch (cause) {
      if (cause instanceof DatalensError) {
        throw cause;
      }
      throw new DatalensError("decode", `datalens decode: ${String(cause)}`, { cause });
    }
  }
}

function apiError(response: Response, text: string): DatalensError {
  let kind = "http";
  let message = text;
  try {
    const body = JSON.parse(text) as { error?: { kind?: unknown; message?: unknown } };
    if (typeof body.error?.kind === "string" && typeof body.error.message === "string") {
      kind = body.error.kind;
      message = body.error.message;
    }
  } catch {
    // Not a datalens error body, such as a proxy's 502 page.
  }
  const seconds = Number.parseInt(response.headers.get("retry-after") ?? "", 10);
  return new DatalensError("api", `datalens ${response.status} ${kind}: ${message}`, {
    kind,
    status: response.status,
    retryAfterMs: Number.isFinite(seconds) ? seconds * 1000 : undefined,
  });
}

/** Block numbers and hashes named by rows and by segment anchors. */
function blockHashes<Row>(response: QueryResponse<Row>): Map<number, string> {
  const hashes = new Map<number, string>();
  for (const segment of response.segments) {
    if (segment.anchor) {
      hashes.set(segment.anchor.number, segment.anchor.hash);
    }
  }
  for (const row of response.rows) {
    if (row === null || typeof row !== "object") {
      continue;
    }
    const record = row as { block_number?: unknown; block_hash?: unknown; number?: unknown; hash?: unknown };
    if (typeof record.block_number === "number" && typeof record.block_hash === "string") {
      hashes.set(record.block_number, record.block_hash);
    } else if (typeof record.number === "number" && typeof record.hash === "string") {
      hashes.set(record.number, record.hash);
    }
  }
  return hashes;
}

/** JSON.parse rounds integers past `Number.MAX_SAFE_INTEGER`. Reject those JSON numbers
 * first. Digits inside strings, such as a block hash, are left alone. */
export function assertSafeJsonIntegers(text: string): void {
  const withoutStrings = text.replace(/"(?:\\.|[^"\\])*"/g, '""');
  for (const match of withoutStrings.matchAll(/(?<![\d.])-?\d{16,}(?!\.\d)/g)) {
    if (!Number.isSafeInteger(Number(match[0]))) {
      throw new DatalensError(
        "decode",
        `datalens decode: integer ${match[0]} exceeds Number.MAX_SAFE_INTEGER`,
      );
    }
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
