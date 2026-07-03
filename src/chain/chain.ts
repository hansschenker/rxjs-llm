import {
  asapScheduler,
  Observable,
  of,
  ReplaySubject,
  Subject,
  subscribeOn,
  type OperatorFunction,
  type Subscription,
} from 'rxjs';
import type { ChainEvent } from './events';
import { CHAIN_EMIT, type InternalEmit } from './stage';

/** One run(): a passive progress channel and the result that drives the work. */
export interface ChainRun<Out extends object> {
  /**
   * The run's outcome. Lazy — execution starts at the FIRST subscription —
   * then multicast: further subscriptions join the same execution, and the
   * outcome (final context, error, or cancellation) is latched permanently.
   * `retry()` here can never re-execute the chain; whole-chain retry is a
   * new run() call: `defer(() => chain.run(input).result$).pipe(retry(n))`.
   */
  result$: Observable<Out>;
  /**
   * Passive observation channel. Subscribing triggers nothing; events with
   * no subscriber are dropped (plain Subject — a UI channel, stated in
   * ADR-0006). Ends with exactly one terminal event (run_complete |
   * run_failed) then completion — except cancellation, which completes it
   * silently (ADR-0005).
   */
  progress$: Observable<ChainEvent>;
}

export interface RunnableChain<In extends object, Out extends object> {
  /** One call = one logical execution (ADR-0006). */
  run(input: In): ChainRun<Out>;
}

export interface ChainBuilder<In extends object> {
  pipe<A extends object>(op1: OperatorFunction<In, A>): RunnableChain<In, A>;
  pipe<A extends object, B extends object>(
    op1: OperatorFunction<In, A>,
    op2: OperatorFunction<A, B>,
  ): RunnableChain<In, B>;
  pipe<A extends object, B extends object, C extends object>(
    op1: OperatorFunction<In, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
  ): RunnableChain<In, C>;
  pipe<A extends object, B extends object, C extends object, D extends object>(
    op1: OperatorFunction<In, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
  ): RunnableChain<In, D>;
  pipe<
    A extends object,
    B extends object,
    C extends object,
    D extends object,
    E extends object,
  >(
    op1: OperatorFunction<In, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
  ): RunnableChain<In, E>;
  pipe<
    A extends object,
    B extends object,
    C extends object,
    D extends object,
    E extends object,
    F extends object,
  >(
    op1: OperatorFunction<In, A>,
    op2: OperatorFunction<A, B>,
    op3: OperatorFunction<B, C>,
    op4: OperatorFunction<C, D>,
    op5: OperatorFunction<D, E>,
    op6: OperatorFunction<E, F>,
  ): RunnableChain<In, F>;
}

/**
 * Chains ARE pipes (decision D3.2): `chain<In>().pipe(stage(...), ...)`
 * merely remembers the operator list; nothing executes until a run's
 * result$ is first subscribed.
 */
export function chain<In extends object>(): ChainBuilder<In> {
  const builder = {
    pipe: (...operators: OperatorFunction<object, object>[]) => ({
      run: (input: In) => runChain(input, operators),
    }),
  };
  return builder as unknown as ChainBuilder<In>;
}

/**
 * The dual-channel contract (D3.3, ADR-0006), point by point:
 *
 * 1. Passivity — progress$ is fed by whatever execution result$ drives;
 *    subscribing to it alone triggers nothing.
 * 2. Lifecycle — natural termination pushes exactly one terminal event
 *    (run_complete | run_failed) onto progress$ and then completes it. The
 *    error OBJECT travels on result$ only. Cancellation (last result$
 *    subscriber leaves mid-flight) aborts the execution and completes both
 *    channels silently — no terminal event, no error (ADR-0005).
 * 3. Unobserved progress events are dropped: plain Subject, no replay.
 * 4. One run() = one logical execution, outcome latched. This is a
 *    hand-rolled multicast rather than share({...latched}) because no
 *    combination of share's reset flags expresses it: with
 *    resetOnRefCountZero:false an abandoned run keeps executing (nothing
 *    aborts the in-flight provider call — a teardown-law violation), and
 *    with true, resubscribing after abandonment silently re-executes. Here
 *    refcount-zero-before-terminal aborts AND latches: late subscribers to
 *    a cancelled run get an immediate empty completion, never a re-run.
 *    The latch is first-writer-wins: `settled` guards both directions, and
 *    straggler emissions after an abort die against two rxjs walls — a
 *    closed subscription discards next/complete, and a completed Subject
 *    ignores next().
 *
 * Execution is subscribed on the asap scheduler so a fully-synchronous
 * stage still cannot emit in the first subscriber's call frame (no Zalgo).
 */
function runChain<In extends object, Out extends object>(
  input: In,
  operators: OperatorFunction<object, object>[],
): ChainRun<Out> {
  const progress = new Subject<ChainEvent>();
  const output = new ReplaySubject<Out>(1);
  let subscriberCount = 0;
  let started = false;
  let settled = false;
  let execution: Subscription | undefined;

  const start = (): void => {
    started = true;
    const internalEmit: InternalEmit = (stage, event) => {
      progress.next({ type: 'stage_event', stage, event });
    };
    const seeded: object = Object.assign({}, input, { [CHAIN_EMIT]: internalEmit });
    let source: Observable<object> = of(seeded);
    for (const op of operators) source = op(source);

    execution = source.pipe(subscribeOn(asapScheduler)).subscribe({
      next: (ctx) => {
        const finalCtx = Object.assign({}, ctx) as Record<PropertyKey, unknown>;
        delete finalCtx[CHAIN_EMIT];
        output.next(finalCtx as Out);
      },
      error: (error: unknown) => {
        settled = true;
        progress.next({
          type: 'run_failed',
          message: error instanceof Error ? error.message : String(error),
        });
        progress.complete();
        output.error(error);
      },
      complete: () => {
        settled = true;
        progress.next({ type: 'run_complete' });
        progress.complete();
        output.complete();
      },
    });
  };

  const result$ = new Observable<Out>((subscriber) => {
    subscriberCount += 1;
    const delivery = output.subscribe(subscriber);
    if (!started) start();
    return () => {
      subscriberCount -= 1;
      delivery.unsubscribe();
      if (subscriberCount === 0 && !settled) {
        settled = true; // cancelled: latch so nothing ever re-executes
        execution?.unsubscribe(); // aborts in-flight provider requests
        progress.complete(); // silent — no terminal event (ADR-0005)
        output.complete(); // late subscribers: immediate empty completion
      }
    };
  });

  return { result$, progress$: progress.asObservable() };
}
