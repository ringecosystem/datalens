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

export type DatalensErrorType = "api" | "http" | "decode";

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

  /** Rate limits, server errors, and transport failures. */
  isRetryable(): boolean {
    switch (this.type) {
      case "api":
        return this.status === 429 || (this.status ?? 0) >= 500;
      case "http":
        return true;
      case "decode":
        return false;
    }
  }
}
