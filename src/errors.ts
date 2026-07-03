/**
 * Error taxonomy (decision D2/plan §Error taxonomy).
 *
 * LlmError (base, carries provider + requestId)
 * ├── TransportError    DNS, TLS, socket reset            → retryable
 * ├── HttpError         non-2xx; 429/5xx retryable, 4xx not
 * │     └── RateLimitError  429, carries retryAfterMs
 * ├── ParseError        malformed SSE/NDJSON/JSON          → not retryable
 * ├── ProviderError     in-stream error events             → per provider code
 * └── TimeoutError      first-byte vs idle (decision D3)   → per phase
 *
 * AbortError is deliberately absent: unsubscription is silent teardown and
 * must never surface on the error channel (ADR-0005).
 */

export interface LlmErrorOptions {
  provider?: string;
  requestId?: string;
  cause?: unknown;
}

export abstract class LlmError extends Error {
  abstract readonly kind: string;
  readonly provider: string | undefined;
  readonly requestId: string | undefined;

  constructor(message: string, options: LlmErrorOptions = {}) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = new.target.name;
    this.provider = options.provider;
    this.requestId = options.requestId;
  }
}

/** Connection-level failure before or during the response: DNS, TLS, socket reset. */
export class TransportError extends LlmError {
  override readonly kind = 'transport';
}

/** Non-2xx HTTP response. Carries status and (best-effort) response body. */
export class HttpError extends LlmError {
  override readonly kind: string = 'http';
  readonly status: number;
  readonly body: string | undefined;

  constructor(
    message: string,
    status: number,
    options: LlmErrorOptions & { body?: string } = {},
  ) {
    super(message, options);
    this.status = status;
    this.body = options.body;
  }
}

/** HTTP 429. `retryAfterMs` is parsed from the Retry-After header when present. */
export class RateLimitError extends HttpError {
  override readonly kind = 'rate_limit';
  readonly retryAfterMs: number | undefined;

  constructor(
    message: string,
    options: LlmErrorOptions & { body?: string; retryAfterMs?: number } = {},
  ) {
    super(message, 429, options);
    this.retryAfterMs = options.retryAfterMs;
  }
}

/** The provider sent bytes we could not parse (malformed SSE frame, bad JSON). */
export class ParseError extends LlmError {
  override readonly kind = 'parse';
}

/** An error event delivered inside an otherwise-healthy stream. */
export class ProviderError extends LlmError {
  override readonly kind = 'provider';
  /** Provider-specific error code, e.g. Anthropic's `overloaded_error`. */
  readonly code: string | undefined;
  readonly retryable: boolean;

  constructor(
    message: string,
    options: LlmErrorOptions & { code?: string; retryable?: boolean } = {},
  ) {
    super(message, options);
    this.code = options.code;
    this.retryable = options.retryable ?? false;
  }
}

/**
 * Stream timeout (decision D3). Two distinct failure modes:
 * - `first-byte`: connection established but no data — provider overloaded.
 *   Retrying costs nothing; retryable by default.
 * - `idle`: the stream stalled mid-generation. Tokens were already produced;
 *   a blind retry duplicates cost. Not retryable by default.
 */
export class TimeoutError extends LlmError {
  override readonly kind = 'timeout';
  readonly phase: 'first-byte' | 'idle';
  readonly retryable: boolean;

  constructor(
    message: string,
    phase: 'first-byte' | 'idle',
    options: LlmErrorOptions & { retryable?: boolean } = {},
  ) {
    super(message, options);
    this.phase = phase;
    this.retryable = options.retryable ?? phase === 'first-byte';
  }
}

/** The single predicate the retry operator consults. */
export function isRetryable(error: unknown): boolean {
  if (error instanceof RateLimitError) return true;
  if (error instanceof HttpError) return error.status === 429 || error.status >= 500;
  if (error instanceof TransportError) return true;
  if (error instanceof ProviderError) return error.retryable;
  if (error instanceof TimeoutError) return error.retryable;
  return false;
}
