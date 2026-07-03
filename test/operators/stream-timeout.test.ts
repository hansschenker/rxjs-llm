import { TestScheduler } from 'rxjs/testing';
import { describe, expect, it } from 'vitest';
import { TimeoutError } from '../../src/errors';
import { retryWithBackoff } from '../../src/operators/retry-backoff';
import { streamTimeout } from '../../src/operators/stream-timeout';

function makeScheduler() {
  return new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
}

describe('streamTimeout', () => {
  it('errors with a first-byte TimeoutError when nothing arrives in time', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('------a|');
      expectObservable(source.pipe(streamTimeout({ firstByteMs: 5, scheduler }))).toBe(
        '-----#',
        undefined,
        new TimeoutError('first-byte timeout after 5ms', 'first-byte'),
      );
    });
  });

  it('errors with an idle TimeoutError when the stream stalls mid-flow', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('a------b|');
      expectObservable(
        source.pipe(streamTimeout({ firstByteMs: 5, idleMs: 3, scheduler })),
      ).toBe('a--#', { a: 'a' }, new TimeoutError('idle timeout after 3ms', 'idle'));
    });
  });

  it('passes a healthy stream through untouched', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('a-b-c|');
      expectObservable(
        source.pipe(streamTimeout({ firstByteMs: 5, idleMs: 3, scheduler })),
      ).toBe('a-b-c|');
    });
  });

  it('idle-only config still guards the first byte, reported as first-byte phase', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('----a|');
      expectObservable(source.pipe(streamTimeout({ idleMs: 3, scheduler }))).toBe(
        '---#',
        undefined,
        new TimeoutError('first-byte timeout after 3ms', 'first-byte'),
      );
    });
  });

  it('is a no-op when neither timeout is configured', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('a----b|');
      expectObservable(source.pipe(streamTimeout({}))).toBe('a----b|');
    });
  });
});

describe('streamTimeout ∘ retryWithBackoff (ADR-0003 in action)', () => {
  it('first-byte timeouts are retried', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('------a|'); // first byte always too late
      // timeout@5 → retryable → wait 2 → resubscribe@7 → timeout@12 → exhausted
      expectObservable(
        source.pipe(
          streamTimeout({ firstByteMs: 5, scheduler }),
          retryWithBackoff({ maxRetries: 1, baseMs: 2, random: () => 1, scheduler }),
        ),
      ).toBe(
        '------------#',
        undefined,
        new TimeoutError('first-byte timeout after 5ms', 'first-byte'),
      );
    });
  });

  it('idle timeouts are not retried — tokens were already produced', () => {
    const scheduler = makeScheduler();
    scheduler.run(({ cold, expectObservable }) => {
      const source = cold('a------b|');
      expectObservable(
        source.pipe(
          streamTimeout({ firstByteMs: 5, idleMs: 3, scheduler }),
          retryWithBackoff({ maxRetries: 3, baseMs: 2, random: () => 1, scheduler }),
        ),
      ).toBe('a--#', { a: 'a' }, new TimeoutError('idle timeout after 3ms', 'idle'));
    });
  });
});
