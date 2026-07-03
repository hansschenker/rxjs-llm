import {
  asyncScheduler,
  concatMap,
  defer,
  map,
  of,
  timer,
  type MonoTypeOperatorFunction,
  type SchedulerLike,
} from 'rxjs';

export interface RateLimitOptions {
  /** Tokens refilled per interval — the sustained rate. */
  tokensPerInterval: number;
  intervalMs: number;
  /** Burst size: how many values may pass instantly. Default: tokensPerInterval. */
  capacity?: number;
  /** Injectable for marble tests; also supplies the clock via scheduler.now(). */
  scheduler?: SchedulerLike;
}

/**
 * Token-bucket rate limiter. Each value spends one token; the bucket starts
 * full (bursts up to `capacity` pass immediately) and refills continuously
 * at `tokensPerInterval / intervalMs`. When empty, values queue — order
 * preserved via concatMap — and each is released as soon as a whole token
 * has accrued. Downstream request operators (embedding batchers, agents)
 * compose this in front of a ChatModel call without either knowing.
 *
 * Bucket state lives in a defer() closure: per subscription, so the operator
 * stays cold and re-subscription gets a fresh, full bucket.
 */
export function rateLimit<T>(options: RateLimitOptions): MonoTypeOperatorFunction<T> {
  const capacity = options.capacity ?? options.tokensPerInterval;
  const refillPerMs = options.tokensPerInterval / options.intervalMs;
  const scheduler = options.scheduler ?? asyncScheduler;

  return (source) =>
    defer(() => {
      let tokens = capacity;
      let lastRefill = scheduler.now();

      const refill = (): void => {
        const now = scheduler.now();
        tokens = Math.min(capacity, tokens + (now - lastRefill) * refillPerMs);
        lastRefill = now;
      };

      return source.pipe(
        concatMap((value) => {
          refill();
          if (tokens >= 1) {
            tokens -= 1;
            return of(value);
          }
          const waitMs = Math.ceil((1 - tokens) / refillPerMs);
          return timer(waitMs, scheduler).pipe(
            map(() => {
              refill();
              tokens -= 1;
              return value;
            }),
          );
        }),
      );
    });
}
