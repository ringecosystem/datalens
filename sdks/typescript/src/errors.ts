export type GraphQLErrorPayload = {
  message?: string;
  extensions?: {
    code?: string;
    kind?: string;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

const rateLimitMarker = Symbol("DatalensRateLimitError");
const authMarker = Symbol("DatalensAuthError");

export class DatalensError extends Error {
  readonly kind?: string;

  constructor(message: string, options: { kind?: string; cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = new.target.name;
    this.kind = options.kind;
  }
}

export class DatalensGraphQLError extends DatalensError {
  readonly errors: GraphQLErrorPayload[];

  constructor(errors: GraphQLErrorPayload[]) {
    const first = errors[0];
    super(first?.message ?? "Datalens GraphQL request failed", {
      kind: first?.extensions?.kind,
    });
    this.errors = errors;
  }
}

export class DatalensHttpError extends DatalensError {
  readonly status: number;
  readonly body: unknown;

  constructor(status: number, message: string, options: { kind?: string; body?: unknown } = {}) {
    super(message, { kind: options.kind });
    this.status = status;
    this.body = options.body;
  }
}

export class DatalensAuthError extends DatalensHttpError {
  static [Symbol.hasInstance](instance: unknown): boolean {
    return Boolean(instance && typeof instance === "object" && authMarker in instance);
  }

  constructor(status: number, message: string, options: { kind?: string; body?: unknown } = {}) {
    super(status, message, options);
    Object.defineProperty(this, authMarker, {
      value: true,
    });
  }
}

export class DatalensRateLimitError extends DatalensError {
  readonly retryAfter?: string | null;

  static [Symbol.hasInstance](instance: unknown): boolean {
    return Boolean(instance && typeof instance === "object" && rateLimitMarker in instance);
  }

  constructor(message: string, options: { kind?: string; retryAfter?: string | null } = {}) {
    super(message, { kind: options.kind ?? "rate_limited" });
    this.retryAfter = options.retryAfter;
    Object.defineProperty(this, rateLimitMarker, {
      value: true,
    });
  }
}

export class DatalensTimeoutError extends DatalensError {
  constructor(timeoutMs: number, cause: unknown) {
    super(`Datalens request timed out after ${timeoutMs}ms`, {
      kind: "timeout",
      cause,
    });
  }
}

export class DatalensGraphQLRateLimitError extends DatalensGraphQLError {
  readonly retryAfter?: string | null;

  constructor(errors: GraphQLErrorPayload[], retryAfter?: string | null) {
    super(errors);
    this.retryAfter = retryAfter;
    Object.defineProperty(this, rateLimitMarker, {
      value: true,
    });
  }
}

export class DatalensGraphQLAuthError extends DatalensGraphQLError {
  constructor(errors: GraphQLErrorPayload[]) {
    super(errors);
    Object.defineProperty(this, authMarker, {
      value: true,
    });
  }
}

export class DatalensHttpRateLimitError extends DatalensHttpError {
  readonly retryAfter?: string | null;

  constructor(
    status: number,
    message: string,
    options: { kind?: string; body?: unknown; retryAfter?: string | null } = {},
  ) {
    super(status, message, {
      kind: options.kind ?? "rate_limited",
      body: options.body,
    });
    this.retryAfter = options.retryAfter;
    Object.defineProperty(this, rateLimitMarker, {
      value: true,
    });
  }
}
