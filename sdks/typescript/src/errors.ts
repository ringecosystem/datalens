/** Stable `error.kind` values, or `http` for a body that is not a datalens error. */
export type ApiErrorKind =
  | "unauthorized"
  | "forbidden"
  | "invalid_input"
  | "quota_exceeded"
  | "unsupported"
  | "internal"
  | "http"
  | (string & {});

export type DatalensErrorType = "api" | "http" | "decode" | "inconsistent";

export class DatalensError extends Error {
  /** `api`: the server answered with an error status. `http`: the request did not complete. */
  readonly type: DatalensErrorType;
  readonly kind?: ApiErrorKind;
  readonly status?: number;
  readonly retryAfterMs?: number;

  constructor(
    type: DatalensErrorType,
    message: string,
    details: { kind?: ApiErrorKind; status?: number; retryAfterMs?: number; cause?: unknown } = {},
  ) {
    super(message, { cause: details.cause });
    this.name = "DatalensError";
    this.type = type;
    this.kind = details.kind;
    this.status = details.status;
    this.retryAfterMs = details.retryAfterMs;
  }

  /** Rate limits (429), server errors, transport failures, and a page set that disagreed
   * on a block hash. 400 and 403 are not retried. */
  isRetryable(): boolean {
    switch (this.type) {
      case "api":
        return this.status === 429 || (this.status ?? 0) >= 500;
      case "http":
      case "inconsistent":
        return true;
      case "decode":
        return false;
    }
  }
}
