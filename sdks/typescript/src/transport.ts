import {
  DatalensAuthError,
  DatalensGraphQLError,
  DatalensGraphQLAuthError,
  DatalensGraphQLRateLimitError,
  DatalensHttpError,
  DatalensHttpRateLimitError,
  DatalensTimeoutError,
  type GraphQLErrorPayload,
} from "./errors.js";
import type { DatalensClientOptions, FetchLike } from "./types.js";

const defaultUserAgent = "datalens-typescript-sdk/0.1.0";

type GraphQLResponse<TData> = {
  data?: TData | null;
  errors?: GraphQLErrorPayload[];
};

export class GraphQLTransport {
  private readonly endpoint: URL;
  private readonly fetchImpl: FetchLike;
  private readonly token?: string;
  private readonly application?: string;
  private readonly timeoutMs?: number;
  private readonly userAgent: string;

  constructor(options: DatalensClientOptions) {
    this.endpoint = new URL(options.endpoint);
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.token = options.token;
    this.application = options.application;
    this.timeoutMs = options.timeoutMs;
    this.userAgent = options.userAgent ?? defaultUserAgent;
  }

  async graphql<TData>(
    path: string,
    query: string,
    variables?: Record<string, unknown>,
    operationName?: string,
  ): Promise<TData> {
    const response = await this.post(path, {
      query,
      variables,
      operationName,
    });
    const body = (await parseJson(response)) as GraphQLResponse<TData>;

    if (!response.ok) {
      throw httpError(response, body);
    }

    if (body.errors?.length) {
      throw graphqlError(body.errors);
    }

    if (body.data == null) {
      throw new DatalensGraphQLError([
        {
          message: "Datalens GraphQL response did not include data",
          extensions: {
            kind: "invalid_response",
          },
        },
      ]);
    }

    return body.data;
  }

  private async post(path: string, body: unknown): Promise<Response> {
    const url = new URL(path, this.endpoint);
    const controller = this.timeoutMs == null ? undefined : new AbortController();
    const timeout =
      controller == null
        ? undefined
        : setTimeout(() => {
            controller.abort();
          }, this.timeoutMs);

    try {
      return await this.fetchImpl(url, {
        method: "POST",
        headers: this.headers(),
        body: JSON.stringify(body),
        signal: controller?.signal,
      });
    } catch (error) {
      if (controller?.signal.aborted && this.timeoutMs != null) {
        throw new DatalensTimeoutError(this.timeoutMs, error);
      }
      throw error;
    } finally {
      if (timeout != null) {
        clearTimeout(timeout);
      }
    }
  }

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      accept: "application/json",
      "content-type": "application/json",
      "user-agent": this.userAgent,
    };

    if (this.token != null) {
      headers.authorization = `Bearer ${this.token}`;
    }

    if (this.application != null) {
      headers["x-datalens-application"] = this.application;
    }

    return headers;
  }
}

async function parseJson(response: Response): Promise<unknown> {
  const text = await response.text();
  if (text.length === 0) {
    return null;
  }
  return JSON.parse(text);
}

function graphqlError(errors: GraphQLErrorPayload[]): DatalensGraphQLError {
  if (errors.some(isRateLimitedGraphQLError)) {
    return new DatalensGraphQLRateLimitError(errors);
  }
  if (errors.some(isAuthGraphQLError)) {
    return new DatalensGraphQLAuthError(errors);
  }
  return new DatalensGraphQLError(errors);
}

function httpError(response: Response, body: unknown): DatalensHttpError {
  const detail = responseBodyError(body);
  const message = detail.message ?? `Datalens HTTP request failed with status ${response.status}`;
  const kind = detail.kind;

  if (response.status === 429 || normalizedKind(kind) === "rate_limited") {
    return new DatalensHttpRateLimitError(response.status, message, {
      kind,
      body,
      retryAfter: response.headers.get("retry-after"),
    });
  }

  if (
    response.status === 401 ||
    response.status === 403 ||
    normalizedKind(kind) === "unauthorized" ||
    normalizedKind(kind) === "authentication_failed"
  ) {
    return new DatalensAuthError(response.status, message, {
      kind,
      body,
    });
  }

  return new DatalensHttpError(response.status, message, {
    kind,
    body,
  });
}

function isRateLimitedGraphQLError(error: GraphQLErrorPayload): boolean {
  return (
    normalizedKind(error.extensions?.kind) === "rate_limited" ||
    error.extensions?.code === "RATE_LIMITED"
  );
}

function isAuthGraphQLError(error: GraphQLErrorPayload): boolean {
  const kind = normalizedKind(error.extensions?.kind);
  return (
    kind === "authentication_failed" ||
    kind === "unauthorized" ||
    error.extensions?.code === "AUTHENTICATION_FAILED" ||
    error.extensions?.code === "AUTHORIZATION_FAILED"
  );
}

function normalizedKind(kind: string | undefined): string | undefined {
  return kind
    ?.replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/-/g, "_")
    .toLowerCase();
}

function responseBodyError(body: unknown): { kind?: string; message?: string } {
  if (body == null || typeof body !== "object" || !("error" in body)) {
    return {};
  }

  const error = (body as { error?: unknown }).error;
  if (error == null || typeof error !== "object") {
    return {};
  }

  return {
    kind: stringValue((error as { kind?: unknown }).kind),
    message: stringValue((error as { message?: unknown }).message),
  };
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
