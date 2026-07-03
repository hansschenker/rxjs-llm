import { defer, of, throwError } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { describe, expect, it } from 'vitest';
import { HttpError, RateLimitError, TransportError } from '../../src/errors';
import { retryWithBackoff } from '../../src/operators/retry-backoff';

function makeScheduler() {
  return new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
}

// random: () => 1 pins the jitter factor at 1.0, so delays are exactly
// baseMs · 2^(attempt-1) and the marble arithmetic below is deterministic.

describe('retryWithBackoff', () => {
  it('retries retryable errors with exponential delays, then re-throws the last error', () => {
    const scheduler = makeScheduler();
    const error = new TransportError('socket reset');
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('-#', {}, error);
      // fail@1 → wait 2 → fail@4 → wait 4 → fail@9 → retries exhausted
      expectObservable(
        source.pipe(
          retryWithBackoff({ maxRetries: 2, baseMs: 2, random: () => 1, scheduler }),
        ),
      ).toBe('---------#', {}, error);
    });
  });

  it('recovers when a later attempt succeeds', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ expectObservable }) => {
      let attempt = 0;
      const source = defer(() => {
        attempt += 1;
        return attempt < 3 ? throwError(() => new TransportError('flaky')) : of('ok');
      });
      // fail@0 → wait 2 → fail@2 → wait 4 → succeed@6
      expectObservable(
        source.pipe(retryWithBackoff({ baseMs: 2, random: () => 1, scheduler })),
      ).toBe('------(o|)', { o: 'ok' });
    });
  });

  it('does not retry non-retryable errors', () => {
    const scheduler = makeScheduler();
    const error = new HttpError('bad request', 400);
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('--#', {}, error);
      expectObservable(source.pipe(retryWithBackoff({ scheduler }))).toBe('--#', {}, error);
    });
  });

  it('honors Retry-After over the exponential schedule', () => {
    const scheduler = makeScheduler();
    const error = new RateLimitError('429', { retryAfterMs: 7 });
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('#', {}, error);
      // fail@0 → wait 7 (not baseMs·2⁰ = 2) → fail@7 → exhausted
      expectObservable(
        source.pipe(retryWithBackoff({ maxRetries: 1, baseMs: 2, random: () => 1, scheduler })),
      ).toBe('-------#', {}, error);
    });
  });

  it('accepts a custom shouldRetry predicate', () => {
    const scheduler = makeScheduler();
    const error = new Error('not in the taxonomy');
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('-#', {}, error);
      expectObservable(
        source.pipe(
          retryWithBackoff({
            maxRetries: 1,
            baseMs: 1,
            random: () => 1,
            shouldRetry: () => true,
            scheduler,
          }),
        ),
      ).toBe('---#', {}, error); // fail@1 → wait 1 → fail@3
    });
  });
});
