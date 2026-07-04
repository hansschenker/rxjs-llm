import {
  asyncScheduler,
  retry,
  throwError,
  timer,
  type MonoTypeOperatorFunction,
  type SchedulerLike,
} from 'rxjs';
import { isRetryable, RateLimitError } from '../errors.js';

export interface RetryBackoffOptions {
  /** Retry attempts after the first failure. Default 3. */
  maxRetries?: number;
  /** First backoff delay; doubles each retry. Default 500. */
  baseMs?: number;
  /** Backoff ceiling before jitter. Default 30_000. */
  maxDelayMs?: number;
  /** Which errors to retry. Default: the taxonomy's isRetryable predicate. */
  shouldRetry?: (error: unknown) => boolean;
  /** 0..1 random source — injectable so jitter is deterministic in tests. */
  random?: () => number;
  /** Injectable for marble tests. */
  scheduler?: SchedulerLike;
}

/**
 * Jittered exponential backoff over the error taxonomy.
 *
 * - Non-retryable errors (per `shouldRetry`) pass through immediately.
 * - A `RateLimitError` carrying `retryAfterMs` waits exactly what the
 *   provider asked for — Retry-After beats the exponential schedule.
 * - Otherwise the delay is `min(maxDelayMs, baseMs·2^(attempt-1))`, scaled
 *   by equal jitter into [50%, 100%] so retry storms decorrelate.
 * - When retries are exhausted, the LAST error is re-thrown unchanged.
 *
 * Because a ChatModel stream is cold, each retry is a genuinely new HTTP
 * request — resubscription IS the retry.
 */
export function retryWithBackoff<T>(
  options: RetryBackoffOptions = {},
): MonoTypeOperatorFunction<T> {
  const maxRetries = options.maxRetries ?? 3;
  const baseMs = options.baseMs ?? 500;
  const maxDelayMs = options.maxDelayMs ?? 30_000;
  const shouldRetry = options.shouldRetry ?? isRetryable;
  const random = options.random ?? Math.random;
  const scheduler = options.scheduler ?? asyncScheduler;

  return retry({
    count: maxRetries,
    delay: (error: unknown, retryCount: number) => {
      if (!shouldRetry(error)) return throwError(() => error);
      const delayMs =
        error instanceof RateLimitError && error.retryAfterMs !== undefined
          ? error.retryAfterMs
          : Math.min(maxDelayMs, baseMs * 2 ** (retryCount - 1)) * (0.5 + random() / 2);
      return timer(delayMs, scheduler);
    },
  });
}
