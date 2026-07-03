import { firstValueFrom, lastValueFrom, map, of, timer } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { chain } from '../../src/chain/chain';
import { stage } from '../../src/chain/stage';
import { collectorSink, traced, type TraceContext } from '../../src/chain/trace';

/** Deterministic clock: every reading advances by 5ms. */
function fakeClock() {
  let t = 0;
  return () => (t += 5);
}

describe('traced() (D3.4)', () => {
  it('stamps start at subscription and complete with elapsed time', async () => {
    const sink = collectorSink();
    const trace: TraceContext = { sink, runId: 'r1', now: fakeClock() };
    const source = of(1).pipe(traced('lonely', trace));
    expect(sink.events).toEqual([]); // assembly stamps nothing — defer matters

    await lastValueFrom(source); // last: completion must be observed for the tap to fire
    expect(sink.events.map((e) => e.type)).toEqual(['stage_start', 'stage_complete']);
    expect(sink.events[0]).toEqual({ type: 'stage_start', stage: 'lonely', runId: 'r1', at: 5 });
    expect(sink.events[1]).toMatchObject({ type: 'stage_complete', elapsedMs: 5 });
  });

  it('reports errors with message and elapsed time', async () => {
    const sink = collectorSink();
    const trace: TraceContext = { sink, runId: 'r1', now: fakeClock() };
    await lastValueFrom(
      timer(1).pipe(
        map(() => {
          throw new Error('kaput');
        }),
        traced('doomed', trace),
      ),
    ).catch(() => undefined);
    expect(sink.events[1]).toMatchObject({
      type: 'stage_error',
      stage: 'doomed',
      message: 'kaput',
      elapsedMs: 5,
    });
  });
});

describe('chain tracing', () => {
  const build = (sink: ReturnType<typeof collectorSink>, runId?: () => string) =>
    chain<{ q: string }>({ trace: sink, now: fakeClock(), ...(runId && { runId }) }).pipe(
      stage('first', (ctx) => timer(2).pipe(map(() => ({ a: ctx.q + '-a' })))),
      stage('second', (ctx) => of({ b: ctx.a + '-b' })),
    );

  it('every stage reports start/complete in order, with timing fields present', async () => {
    const sink = collectorSink();
    await firstValueFrom(build(sink).run({ q: 'x' }).result$);
    expect(sink.events.map((e) => [e.stage, e.type])).toEqual([
      ['first', 'stage_start'],
      ['first', 'stage_complete'],
      ['second', 'stage_start'],
      ['second', 'stage_complete'],
    ]);
    for (const event of sink.events) {
      expect(event.at).toBeGreaterThan(0);
      if (event.type !== 'stage_start') expect(event.elapsedMs).toBeGreaterThanOrEqual(0);
    }
  });

  it('the correlation id is stable across stages and distinct across runs', async () => {
    const sink = collectorSink();
    let n = 0;
    const built = build(sink, () => `corr_${++n}`);
    await firstValueFrom(built.run({ q: 'x' }).result$);
    await firstValueFrom(built.run({ q: 'y' }).result$);

    const runIds = sink.events.map((e) => e.runId);
    expect(new Set(runIds.slice(0, 4)).size).toBe(1); // stable within run 1
    expect(new Set(runIds.slice(4)).size).toBe(1); // stable within run 2
    expect(runIds[0]).not.toBe(runIds[4]); // distinct across runs
  });

  it('a failing stage reports stage_error to the sink', async () => {
    const sink = collectorSink();
    const boom = new Error('fell over');
    const built = chain<{ q: string }>({ trace: sink, now: fakeClock() }).pipe(
      stage('bad', () => {
        throw boom;
      }),
    );
    await firstValueFrom(built.run({ q: 'x' }).result$).catch(() => undefined);
    expect(sink.events.map((e) => e.type)).toEqual(['stage_start', 'stage_error']);
    expect(sink.events[1]?.message).toBe('fell over');
  });

  it('an untraced chain calls no sink and strips nothing extra', async () => {
    const built = chain<{ q: string }>().pipe(stage('only', (ctx) => of({ a: ctx.q })));
    const final = await firstValueFrom(built.run({ q: 'x' }).result$);
    expect(final).toEqual({ q: 'x', a: 'x' });
    expect(Object.getOwnPropertySymbols(final)).toEqual([]);
  });

  it('the trace context never leaks into result$', async () => {
    const sink = collectorSink();
    const built = chain<{ q: string }>({ trace: sink }).pipe(
      stage('only', (ctx) => of({ a: ctx.q })),
    );
    const final = await firstValueFrom(built.run({ q: 'x' }).result$);
    expect(final).toEqual({ q: 'x', a: 'x' });
    expect(Object.getOwnPropertySymbols(final)).toEqual([]);
  });
});
