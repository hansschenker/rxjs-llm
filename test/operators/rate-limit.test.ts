import { TestScheduler } from 'rxjs/testing';
import { describe, expect, it } from 'vitest';
import { rateLimit } from '../../src/operators/rate-limit';

function makeScheduler() {
  return new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
}

describe('rateLimit (token bucket)', () => {
  it('lets a burst through instantly, then spaces the queue at the refill rate', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('(abcd|)');
      // capacity 2: a,b spend the burst @0; c waits a full token → @10; d → @20
      expectObservable(
        source.pipe(
          rateLimit({ tokensPerInterval: 1, intervalMs: 10, capacity: 2, scheduler }),
        ),
      ).toBe('(ab)------c---------(d|)');
    });
  });

  it('does not delay a stream already under the rate', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('a---------b---------c|');
      expectObservable(
        source.pipe(rateLimit({ tokensPerInterval: 1, intervalMs: 10, capacity: 1, scheduler })),
      ).toBe('a---------b---------c|');
    });
  });

  it('defaults capacity to tokensPerInterval', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('(ab|)');
      expectObservable(
        source.pipe(rateLimit({ tokensPerInterval: 2, intervalMs: 10, scheduler })),
      ).toBe('(ab|)');
    });
  });

  it('is cold: every subscription gets its own full bucket', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const limited = cold('(ab|)').pipe(
        rateLimit({ tokensPerInterval: 1, intervalMs: 10, capacity: 1, scheduler }),
      );
      // Two independent subscribers see identical timing — no shared bucket.
      expectObservable(limited).toBe('a---------(b|)');
      expectObservable(limited).toBe('a---------(b|)');
    });
  });
});
