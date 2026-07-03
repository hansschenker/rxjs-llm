import { from, map, Observable, of, retry, Subject, take, timer } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { chain } from '../../src/chain/chain';
import type { ChainEvent } from '../../src/chain/events';
import { collectText } from '../../src/chain/collect-text';
import { stage } from '../../src/chain/stage';
import type { StreamEvent } from '../../src/types';

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));

const delta = (text: string): StreamEvent => ({ type: 'text_delta', text });

/** A two-stage chain whose executions are counted; stages are async via timer. */
function countingChain() {
  let executions = 0;
  const built = chain<{ q: string }>().pipe(
    stage('first', (ctx, emit) => {
      executions += 1;
      emit(delta('a'));
      return timer(5).pipe(map(() => ({ a: ctx.q + '-a' })));
    }),
    stage('second', (ctx, emit) => {
      emit(delta('b'));
      return timer(5).pipe(map(() => ({ b: ctx.a + '-b' })));
    }),
  );
  return { built, executions: () => executions };
}

describe('D3.3 point 1 — passivity', () => {
  it('subscribing progress$ alone triggers zero stage invocations', async () => {
    const { built, executions } = countingChain();
    const { progress$ } = built.run({ q: 'x' });
    const seen: ChainEvent[] = [];
    progress$.subscribe((e) => seen.push(e));
    await tick();
    expect(executions()).toBe(0);
    expect(seen).toEqual([]);
  });

  it('run() itself is lazy — no execution before the first result$ subscription', async () => {
    const { built, executions } = countingChain();
    built.run({ q: 'x' });
    await tick();
    expect(executions()).toBe(0);
  });

  it('nothing emits in the subscribe call frame, even for synchronous stages (no Zalgo)', async () => {
    const built = chain<{ q: string }>().pipe(stage('sync', () => of({ a: 1 })));
    const { result$ } = built.run({ q: 'x' });
    let value: unknown;
    result$.subscribe((v) => (value = v));
    expect(value).toBeUndefined(); // still inside the subscribe frame
    await vi.waitFor(() => expect(value).toEqual({ q: 'x', a: 1 }));
  });
});

describe('D3.3 point 2 — lifecycle coupling and terminal events', () => {
  it('success: progress$ ends with exactly one run_complete, then completes; no error handlers fire', async () => {
    const { built } = countingChain();
    const { result$, progress$ } = built.run({ q: 'x' });
    const events: ChainEvent[] = [];
    let progressCompleted = false;
    let progressErrored = false;
    progress$.subscribe({
      next: (e) => events.push(e),
      complete: () => (progressCompleted = true),
      error: () => (progressErrored = true),
    });
    const final = await new Promise((resolve) => result$.subscribe(resolve));

    expect(final).toEqual({ q: 'x', a: 'x-a', b: 'x-a-b' });
    const terminals = events.filter((e) => e.type === 'run_complete' || e.type === 'run_failed');
    expect(terminals).toEqual([{ type: 'run_complete' }]);
    expect(events.at(-1)).toEqual({ type: 'run_complete' });
    expect(progressCompleted).toBe(true);
    expect(progressErrored).toBe(false);
    // the stage-tagged model events came through in order
    expect(events.slice(0, 2)).toEqual([
      { type: 'stage_event', stage: 'first', event: delta('a') },
      { type: 'stage_event', stage: 'second', event: delta('b') },
    ]);
  });

  it('failure: the error object travels on result$ only; progress$ gets run_failed as data, then completes', async () => {
    const boom = new Error('stage exploded');
    const built = chain<{ q: string }>().pipe(
      stage('bad', () => timer(5).pipe(map((): { a: number } => { throw boom; }))),
    );
    const { result$, progress$ } = built.run({ q: 'x' });
    const events: ChainEvent[] = [];
    let progressCompleted = false;
    let progressErrored = false;
    progress$.subscribe({
      next: (e) => events.push(e),
      complete: () => (progressCompleted = true),
      error: () => (progressErrored = true),
    });
    const caught = await new Promise((resolve) =>
      result$.subscribe({ error: resolve }),
    );

    expect(caught).toBe(boom); // the object itself, on result$ alone
    expect(events).toEqual([{ type: 'run_failed', message: 'stage exploded' }]);
    expect(progressCompleted).toBe(true);
    expect(progressErrored).toBe(false); // one failure, one error handler
  });

  it('cancellation: unsubscribe aborts the execution and completes both channels silently', async () => {
    let innerTorndown = false;
    const built = chain<{ q: string }>().pipe(
      stage('hang', () =>
        new Observable<{ a: number }>(() => {
          return () => (innerTorndown = true);
        }),
      ),
    );
    const { result$, progress$ } = built.run({ q: 'x' });
    const events: ChainEvent[] = [];
    let progressCompleted = false;
    progress$.subscribe({
      next: (e) => events.push(e),
      complete: () => (progressCompleted = true),
    });
    const subscription = result$.subscribe();
    await tick();
    subscription.unsubscribe();

    expect(innerTorndown).toBe(true); // in-flight work aborted
    expect(progressCompleted).toBe(true);
    expect(events).toEqual([]); // no terminal event — cancellation is silent
    // and the cancelled run is latched: a late subscriber gets an empty completion, no re-run
    let lateCompleted = false;
    let lateValue: unknown;
    result$.subscribe({ next: (v) => (lateValue = v), complete: () => (lateCompleted = true) });
    await tick();
    expect(lateCompleted).toBe(true);
    expect(lateValue).toBeUndefined();
  });
});

describe('D3.3 point 3 — unobserved progress events are dropped', () => {
  it('a late progress$ subscriber sees only what happens after it subscribes', async () => {
    const gate = new Subject<void>();
    const built = chain<{ q: string }>().pipe(
      stage('one', (_ctx, emit) => {
        emit(delta('early'));
        return gate.pipe(take(1), map(() => ({ a: 1 })));
      }),
      stage('two', (_ctx, emit) => {
        emit(delta('late'));
        return of({ b: 2 });
      }),
    );
    const { result$, progress$ } = built.run({ q: 'x' });
    result$.subscribe();
    await tick(); // 'early' fires with nobody listening — dropped

    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    gate.next();
    await tick();

    expect(events).toEqual([
      { type: 'stage_event', stage: 'two', event: delta('late') },
      { type: 'run_complete' },
    ]);
  });
});

describe('D3.3 point 4 — one run() = one execution, outcome latched', () => {
  it('a staggered second result$ subscription joins the same execution', async () => {
    const { built, executions } = countingChain();
    const { result$ } = built.run({ q: 'x' });
    const finals: unknown[] = [];
    result$.subscribe((v) => finals.push(v));
    await tick(2); // mid-flight (stages take ~10ms total)
    result$.subscribe((v) => finals.push(v));
    await vi.waitFor(() => expect(finals).toHaveLength(2));

    expect(executions()).toBe(1);
    expect(finals[0]).toEqual(finals[1]);
  });

  it('after completion, a late subscriber gets the latched final context without re-executing', async () => {
    const { built, executions } = countingChain();
    const { result$ } = built.run({ q: 'x' });
    const first = await new Promise((resolve) => result$.subscribe(resolve));
    const second = await new Promise((resolve) => result$.subscribe(resolve));
    expect(executions()).toBe(1);
    expect(second).toEqual(first);
  });

  it('after a failed run, retry() hits the latched error — same object, no new execution', async () => {
    let executions = 0;
    const boom = new Error('permanent');
    const built = chain<{ q: string }>().pipe(
      stage('bad', () => {
        executions += 1;
        return timer(5).pipe(map((): { a: number } => { throw boom; }));
      }),
    );
    const { result$ } = built.run({ q: 'x' });
    const first = await new Promise((resolve) => result$.subscribe({ error: resolve }));
    const retried = await new Promise((resolve) =>
      result$.pipe(retry(2)).subscribe({ error: resolve }),
    );

    expect(first).toBe(boom);
    expect(retried).toBe(boom); // the SAME latched error object
    expect(executions).toBe(1); // retry(2) caused zero re-runs
  });

  it('separate run() calls are separate executions — re-execution is syntactically visible', async () => {
    const { built, executions } = countingChain();
    await new Promise((resolve) => built.run({ q: 'x' }).result$.subscribe(resolve));
    await new Promise((resolve) => built.run({ q: 'x' }).result$.subscribe(resolve));
    expect(executions()).toBe(2);
  });
});

describe('collectText', () => {
  it('forwards every event to emit while reducing deltas to the final text', async () => {
    const forwarded: StreamEvent[] = [];
    const source = from<StreamEvent[]>([
      { type: 'message_start', model: 'm' },
      delta('Hel'),
      delta('lo'),
      { type: 'message_stop', stopReason: 'end_turn' },
    ]);
    const text = await new Promise((resolve) =>
      source.pipe(collectText((e) => forwarded.push(e))).subscribe(resolve),
    );
    expect(text).toBe('Hello');
    expect(forwarded).toHaveLength(4);
  });
});
