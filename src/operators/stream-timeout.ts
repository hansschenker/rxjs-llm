import {
  throwError,
  timeout,
  type MonoTypeOperatorFunction,
  type Observable,
  type SchedulerLike,
  type TimeoutConfig,
  type TimeoutInfo,
} from 'rxjs';
import { TimeoutError } from '../errors.js';

export interface StreamTimeoutOptions {
  /** Max wait for the FIRST emission after subscribe (connection up, no data). */
  firstByteMs?: number;
  /** Max gap BETWEEN emissions once the stream is flowing. */
  idleMs?: number;
  /** Tagged onto the TimeoutError. */
  provider?: string;
  /** Injectable for marble tests. */
  scheduler?: SchedulerLike;
}

/**
 * Two timeouts, not one (ADR-0003). First-byte and inter-chunk idle are
 * distinct failure modes with distinct retryability: a first-byte timeout is
 * retryable by default (nothing was generated), an idle timeout is not
 * (tokens were already produced and billed). The emitted TimeoutError carries
 * `phase`, so `isRetryable` — and therefore `retryWithBackoff` — does the
 * right thing with no extra wiring.
 *
 * When only `idleMs` is configured it also bounds the wait for the first
 * emission (RxJS `timeout` semantics); the error still reports the honest
 * phase, `first-byte`, because nothing had been received.
 */
export function streamTimeout<T>(options: StreamTimeoutOptions): MonoTypeOperatorFunction<T> {
  const { firstByteMs, idleMs, provider, scheduler } = options;
  if (firstByteMs === undefined && idleMs === undefined) return (source) => source;

  return (source) => {
    const config: TimeoutConfig<T, Observable<never>> & {
      with: (info: TimeoutInfo<T>) => Observable<never>;
    } = {
      with: (info) => {
        const phase = info.seen === 0 ? 'first-byte' : 'idle';
        const limit = phase === 'first-byte' ? (firstByteMs ?? idleMs) : idleMs;
        return throwError(
          () =>
            new TimeoutError(`${phase} timeout after ${String(limit)}ms`, phase, {
              ...(provider !== undefined && { provider }),
            }),
        );
      },
    };
    if (firstByteMs !== undefined) config.first = firstByteMs;
    if (idleMs !== undefined) config.each = idleMs;
    if (scheduler !== undefined) config.scheduler = scheduler;
    return source.pipe(timeout(config));
  };
}
