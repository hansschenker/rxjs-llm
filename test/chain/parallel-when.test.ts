import { firstValueFrom, map, Observable, of, Subject, take, timer } from 'rxjs';
import { describe, expect, expectTypeOf, it } from 'vitest';
import { chain } from '../../src/chain/chain';
import type { ChainEvent } from '../../src/chain/events';
import { stage, stages } from '../../src/chain/stage';
import { collectorSink } from '../../src/chain/trace';
import type { StreamEvent } from '../../src/types';

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));
const delta = (text: string): StreamEvent => ({ type: 'text_delta', text });

describe('stages.parallel', () => {
  it('runs branches against the same pre-join context and merges all patches', async () => {
    const built = chain<{ q: string }>().pipe(
      stages.parallel({
        upper: (ctx) => of({ upper: ctx.q.toUpperCase() }),
        len: (ctx) => timer(5).pipe(map(() => ({ len: ctx.q.length }))),
      }),
      stage('after', (ctx) => of({ combined: `${ctx.upper}:${ctx.len}` })),
    );
    const final = await firstValueFrom(built.run({ q: 'abc' }).result$);
    expect(final).toEqual({ q: 'abc', upper: 'ABC', len: 3, combined: 'ABC:3' });
  });

  it('branches genuinely run concurrently — the join waits for the slowest', async () => {
    const gates = { a: new Subject<void>(), b: new Subject<void>() };
    const startedOrder: string[] = [];
    const built = chain<{ q: string }>().pipe(
      stages.parallel({
        a: () => {
          startedOrder.push('a');
          return gates.a.pipe(take(1), map(() => ({ a: 1 })));
        },
        b: () => {
          startedOrder.push('b');
          return gates.b.pipe(take(1), map(() => ({ b: 2 })));
        },
      }),
    );
    const finals: unknown[] = [];
    built.run({ q: 'x' }).result$.subscribe((v) => finals.push(v));
    await tick(5);
    expect(startedOrder).toEqual(['a', 'b']); // both started before either finished

    gates.b.next(); // resolve in reverse declaration order
    await tick(5);
    expect(finals).toHaveLength(0); // join still waiting on a
    gates.a.next();
    await tick(5);
    expect(finals).toEqual([{ q: 'x', a: 1, b: 2 }]);
  });

  it('tags progress events and traces with the branch key as the stage name', async () => {
    const sink = collectorSink();
    let t = 0;
    const built = chain<{ q: string }>({ trace: sink, now: () => (t += 1) }).pipe(
      stages.parallel({
        left: (_ctx, emit) => {
          emit(delta('L'));
          return of({ left: true });
        },
        right: (_ctx, emit) => {
          emit(delta('R'));
          return of({ right: true });
        },
      }),
    );
    const { result$, progress$ } = built.run({ q: 'x' });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    await firstValueFrom(result$);

    const tagged = events.filter(
      (e): e is Extract<ChainEvent, { type: 'stage_event' }> => e.type === 'stage_event',
    );
    expect(tagged.map((e) => e.stage).sort()).toEqual(['left', 'right']);
    const traceStages = new Set(sink.events.map((e) => e.stage));
    expect(traceStages).toEqual(new Set(['left', 'right']));
  });

  it('unsubscribing mid-parallel tears down every branch', async () => {
    const torndown: string[] = [];
    const hang = (name: string) =>
      new Observable<{ x: number }>(() => () => torndown.push(name));
    const built = chain<{ q: string }>().pipe(
      stages.parallel({ one: () => hang('one'), two: () => hang('two') }),
    );
    const subscription = built.run({ q: 'x' }).result$.subscribe();
    await tick(5);
    subscription.unsubscribe();
    await tick(5);
    expect(torndown.sort()).toEqual(['one', 'two']);
  });

  it('an empty branches object passes the context through', async () => {
    const built = chain<{ q: string }>().pipe(stages.parallel({}));
    expect(await firstValueFrom(built.run({ q: 'x' }).result$)).toEqual({ q: 'x' });
  });
});

describe('stage.when', () => {
  const built = chain<{ n: number }>().pipe(
    stage.when(
      'bigify',
      (ctx) => ctx.n > 10,
      (ctx) => of({ big: ctx.n * 100 }),
    ),
  );

  it('applies the stage when the predicate is true', async () => {
    expect(await firstValueFrom(built.run({ n: 50 }).result$)).toEqual({ n: 50, big: 5000 });
  });

  it('passes the context through untouched when false — no progress, no trace', async () => {
    const sink = collectorSink();
    const traced = chain<{ n: number }>({ trace: sink }).pipe(
      stage.when(
        'bigify',
        (ctx) => ctx.n > 10,
        (ctx, emit) => {
          emit(delta('ran'));
          return of({ big: ctx.n * 100 });
        },
      ),
    );
    const { result$, progress$ } = traced.run({ n: 3 });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    const final = await firstValueFrom(result$);

    expect(final).toEqual({ n: 3 });
    expect(events.filter((e) => e.type === 'stage_event')).toEqual([]);
    expect(sink.events).toEqual([]); // nothing ran, nothing traced
  });
});

describe('type level (D3.1 across parallel/when)', () => {
  it('a branch cannot see a sibling branch key before the join; after the join both are visible', () => {
    chain<{ q: string }>().pipe(
      stages.parallel({
        a: (ctx) => of({ fromA: ctx.q.length }),
        // @ts-expect-error — sibling branch key is not visible before the join
        b: (ctx) => of({ fromB: String(ctx.fromA) }),
      }),
    );

    const joined = chain<{ q: string }>().pipe(
      stages.parallel({
        a: (ctx) => of({ fromA: ctx.q.length }),
        b: (ctx) => of({ fromB: ctx.q.toUpperCase() }),
      }),
      stage('after', (ctx) => of({ both: `${ctx.fromA}${ctx.fromB}` })),
    );
    joined.run({ q: 'x' }).result$.subscribe((ctx) => {
      expectTypeOf(ctx.fromA).toBeNumber();
      expectTypeOf(ctx.fromB).toBeString();
      expectTypeOf(ctx.both).toBeString();
    });
  });

  it('a skipped when-stage types its patch as optional', () => {
    const built = chain<{ n: number }>().pipe(
      stage.when(
        'maybe',
        (ctx) => ctx.n > 0,
        () => of({ extra: 'yes' }),
      ),
    );
    built.run({ n: 1 }).result$.subscribe((ctx) => {
      expectTypeOf(ctx.extra).toEqualTypeOf<string | undefined>();
      // @ts-expect-error — extra may be absent; unguarded use must not compile
      ctx.extra.length;
    });
  });
});
