import { backoffMs, sleep, type Client } from "./client.js";
import { DatalensError } from "./errors.js";
import type {
  ChainHeads,
  Dataset,
  HeadKind,
  IncompleteReason,
  LogSelector,
  QueryFinality,
  QueryResponse,
} from "./types.js";

export interface FollowRequest {
  chain: string;
  dataset: Dataset;
  selector?: LogSelector;
  /** `durable_only` (default) follows the safe head. The other two follow latest. */
  finality?: QueryFinality;
  fields?: string[];
  start: number;
  /** Last block to read. Omitted follows the head until the iterator is closed. */
  end?: number;
  /** Most blocks per query, at or below the application's `max_range_blocks`. Default 1000. */
  chunkBlocks?: number;
  pageLimit?: number;
  waitMs?: number;
  /** Long-poll timeout of `heads/wait` once caught up. Default 30 s. */
  headsWaitMs?: number;
  /** Pause after `pending` or `not_followed_yet`. Default 1 s. */
  pollIntervalMs?: number;
}

export interface FollowOptions {
  signal?: AbortSignal;
}

export type FollowEvent =
  /** One query with every page merged. Its rows cover `served`, a prefix of `requested`. */
  | { type: "batch"; response: QueryResponse }
  /** `latest_only` does not serve `blocks`, because they are at or below the safe head. */
  | { type: "skipped"; blocks: { start: number; end: number } }
  /** Everything up to `head` has been read. The iterator long-polls until the head passes it. */
  | { type: "caught_up"; head: number }
  /** The server stopped at `nextStart`. The iterator asks again after `retryInMs`. */
  | {
      type: "stalled";
      nextStart: number;
      reason: IncompleteReason;
      detail: string | null;
      retryInMs: number;
    }
  /** A retryable failure. The iterator retries after `retryInMs`. */
  | { type: "error"; error: DatalensError; retryInMs: number };

function headOf(heads: ChainHeads, kind: HeadKind): number {
  return heads[kind];
}

export async function* follow(
  client: Client,
  request: FollowRequest,
  { signal }: FollowOptions = {},
): AsyncGenerator<FollowEvent> {
  const finality = request.finality ?? "durable_only";
  const kind: HeadKind = finality === "durable_only" ? "safe" : "latest";
  const chunk = Math.max(request.chunkBlocks ?? 1_000, 1);
  const headsWaitMs = request.headsWaitMs ?? 30_000;
  const pollIntervalMs = request.pollIntervalMs ?? 1_000;
  let next = request.start;
  let heads: ChainHeads | undefined;
  let caughtUp = false;
  let failures = 0;

  while (request.end === undefined || next <= request.end) {
    try {
      const current = heads ?? (await client.heads(request.chain, signal));
      heads = undefined;
      if (finality === "latest_only" && next <= current.safe) {
        const last = request.end === undefined ? current.safe : Math.min(request.end, current.safe);
        if (last >= next) {
          yield { type: "skipped", blocks: { start: next, end: last } };
          next = last + 1;
        }
        heads = current;
        continue;
      }
      const tip = headOf(current, kind);
      const last = request.end === undefined ? tip : Math.min(request.end, tip);
      if (next > last) {
        if (!caughtUp) {
          caughtUp = true;
          heads = current;
          yield { type: "caught_up", head: tip };
          continue;
        }
        heads = await client.waitHeads(request.chain, next - 1, kind, headsWaitMs, signal);
        continue;
      }
      caughtUp = false;

      const range = { start: next, end: Math.min(last, next + chunk - 1) };
      const response = await client.queryAll(
        {
          chain: request.chain,
          dataset: request.dataset,
          selector: request.selector,
          range,
          finality,
          fields: request.fields,
          page: request.pageLimit === undefined ? undefined : { limit: request.pageLimit },
          wait_ms: request.waitMs,
        },
        signal,
      );
      const resume = response.next_start ?? range.end + 1;
      if (resume > next) {
        failures = 0;
      }
      const unservedTo = response.served === null ? resume - 1 : response.served.start - 1;
      if (unservedTo >= next) {
        yield { type: "skipped", blocks: { start: next, end: unservedTo } };
      }
      next = resume;
      if (response.served !== null) {
        yield { type: "batch", response };
      }
      const reason = response.incomplete_reason;
      if (reason !== null && reason !== "below_safe") {
        const waiting = reason === "pending" || reason === "not_followed_yet";
        const retryInMs = waiting ? pollIntervalMs : backoffMs(client.retry, ++failures);
        yield {
          type: "stalled",
          nextStart: resume,
          reason,
          detail: response.incomplete_detail ?? null,
          retryInMs,
        };
        await sleep(retryInMs, signal);
      }
    } catch (error) {
      if (!(error instanceof DatalensError) || !error.isRetryable()) {
        throw error;
      }
      const retryInMs = error.retryAfterMs ?? backoffMs(client.retry, ++failures);
      yield { type: "error", error, retryInMs };
      await sleep(retryInMs, signal);
    }
  }
}
