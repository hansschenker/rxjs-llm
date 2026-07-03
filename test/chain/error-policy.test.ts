import { defer, firstValueFrom, map, of, throwError, timer, type Observable } from 'rxjs';
import { describe, expect, expectTypeOf, it } from 'vitest';
import { chain } from '../../src/chain/chain';
import type { ChainEvent } from '../../src/chain/events';
import { stage, stageOf } from '../../src/chain/stage';
import { collectorSink } from '../../src/chain/trace';
import { TransportError } from '../../src/errors';
import { retryWithBackoff } from '../../src/operators/retry-backoff';

const boom = () => new Error('stage exploded');

describe('onError: "fail" (default)', () => {
  it('propagates the error tagged with the stage name; run_failed carries it', async () => {
    const error = boom();
    const built = chain<{ q: string }>().pipe(
      stage('healthy', (ctx) => of({ a: ctx.q })),
      stage('doomed', () => throwError(() => error)),
    );
    const { result$, progress$ } = built.run({ q: 'x' });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    const caught = await new Promise((resolve) => result$.subscribe({ error: resolve }));

    expect(caught).toBe(error); // identity preserved — no wrapper (ADR-0013)
    expect(stageOf(caught)).toBe('doomed');
    expect(events.at(-1)).toEqual({
      type: 'run_failed',
      message: 'stage exploded',
      stage: 'doomed',
    });
  });

  it('the tag is invisible: enumeration and serialization see a plain Error', async () => {
    const error = boom();
    const built = chain<{ q: string }>().pipe(stage('doomed', () => throwError(() => error)));
    await new Promise((resolve) => built.run({ q: 'x' }).result$.subscribe({ error: resolve }));
    expect(Object.keys(error)).toEqual([]);
    expect(JSON.stringify(error)).toBe('{}');
  });

  it('the innermost stage wins: an outer stage never overwrites the tag', async () => {
    const error = boom();
    const inner = chain<{ q: string }>().pipe(stage('inner', () => throwError(() => error)));
    const outer = chain<{ q: string }>().pipe(
      stage('outer', (ctx) => inner.run({ q: ctx.q }).result$.pipe(map(() => ({ done: true })))),
    );
    const caught = await new Promise((resolve) =>
      outer.run({ q: 'x' }).result$.subscribe({ error: resolve }),
    );
    expect(stageOf(caught)).toBe('inner');
  });
});

describe('onError: "skip"', () => {
  it('drops the patch, the chain completes, and the trace still records the failure', async () => {
    const sink = collectorSink();
    const built = chain<{ q: string }>({ trace: sink }).pipe(
      stage('flaky', (): Observable<{ extra: number }> => throwError(boom), {
        onError: 'skip',
      }),
      stage('after', (ctx) => of({ done: ctx.q })),
    );
    const { result$, progress$ } = built.run({ q: 'x' });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    const final = await firstValueFrom(result$);

    expect(final).toEqual({ q: 'x', done: 'x' }); // flaky's patch absent
    expect(events.at(-1)).toEqual({ type: 'run_complete' }); // a skip is not a failure
    expect(sink.events.map((e) => [e.stage, e.type])).toEqual([
      ['flaky', 'stage_start'],
      ['flaky', 'stage_error'], // ...but the trace tells the truth
      ['after', 'stage_start'],
      ['after', 'stage_complete'],
    ]);
  });

  it('types the skipped patch as optional', () => {
    const built = chain<{ q: string }>().pipe(
      stage('flaky', () => of({ maybe: 1 }), { onError: 'skip' }),
    );
    built.run({ q: 'x' }).result$.subscribe((ctx) => {
      expectTypeOf(ctx.maybe).toEqualTypeOf<number | undefined>();
    });
  });
});

describe('onError: fallback function', () => {
  it('merges the fallback patch as if the stage had succeeded', async () => {
    const built = chain<{ q: string }>().pipe(
      stage('flaky', (): Observable<{ answer: string }> => throwError(boom), {
        onError: (ctx, error) =>
          of({ answer: `fallback for ${ctx.q}: ${(error as Error).message}` }),
      }),
    );
    const final = await firstValueFrom(built.run({ q: 'x' }).result$);
    expect(final).toEqual({ q: 'x', answer: 'fallback for x: stage exploded' });
  });

  it('a failing fallback propagates, tagged with the same stage', async () => {
    const fallbackError = new Error('fallback also exploded');
    const built = chain<{ q: string }>().pipe(
      stage('flaky', (): Observable<{ a: number }> => throwError(boom), {
        onError: () => throwError(() => fallbackError),
      }),
    );
    const caught = await new Promise((resolve) =>
      built.run({ q: 'x' }).result$.subscribe({ error: resolve }),
    );
    expect(caught).toBe(fallbackError);
    expect(stageOf(caught)).toBe('flaky');
  });
});

describe('retry composition (Module 1 inside a stage)', () => {
  it('retryWithBackoff inside the stage body retries the body only — the chain sees one clean success', async () => {
    let attempts = 0;
    const built = chain<{ q: string }>().pipe(
      stage('resilient', (ctx) =>
        defer(() => {
          attempts += 1;
          return attempts < 3
            ? throwError(() => new TransportError('flaky network'))
            : timer(1).pipe(map(() => ({ answer: `${ctx.q}!` })));
        }).pipe(retryWithBackoff({ maxRetries: 3, baseMs: 1 })),
      ),
    );
    const { result$, progress$ } = built.run({ q: 'ok' });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    const final = await firstValueFrom(result$);

    expect(final).toEqual({ q: 'ok', answer: 'ok!' });
    expect(attempts).toBe(3);
    expect(events).toEqual([{ type: 'run_complete' }]); // retries invisible to the run
  });
});
