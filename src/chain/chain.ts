import { map, of, type Observable, type OperatorFunction } from 'rxjs';
import { dualChannel } from './dual-channel';
import type { ChainEvent } from './events';
import { CHAIN_EMIT, CHAIN_TRACE, stageOf, type InternalEmit } from './stage';
import type { TraceContext, TraceSink } from './trace';

export interface ChainOptions {
  /** Trace sink; when set, every stage reports start/complete/error to it. */
  trace?: TraceSink;
  /** Injectable clock for deterministic trace tests. Default: Date.now. */
  now?: () => number;
  /** Correlation-id factory, called once per run(). Default: a counter. */
  runId?: () => string;
}

let runCounter = 0;

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
export function chain<In extends object>(options: ChainOptions = {}): ChainBuilder<In> {
  const builder = {
    pipe: (...operators: OperatorFunction<object, object>[]) => ({
      run: (input: In) => runChain(input, operators, options),
    }),
  };
  return builder as unknown as ChainBuilder<In>;
}

/**
 * The dual-channel contract (D3.3, ADR-0006) lives in dual-channel.ts —
 * one audited implementation shared with the agent loop (D6.4, ADR-0026).
 * This function contributes only what is chain-specific: the plumbing
 * symbols (emit + trace context), the operator pipe, and the final-context
 * symbol strip.
 */
function runChain<In extends object, Out extends object>(
  input: In,
  operators: OperatorFunction<object, object>[],
  options: ChainOptions,
): ChainRun<Out> {
  return dualChannel<Out, ChainEvent>({
    terminal: {
      complete: () => ({ type: 'run_complete' }),
      error: (error: unknown) => {
        const stage = stageOf(error);
        return {
          type: 'run_failed',
          message: error instanceof Error ? error.message : String(error),
          ...(stage !== undefined && { stage }),
        };
      },
    },
    work: (emit) => {
      const internalEmit: InternalEmit = (stage, event) => {
        emit({ type: 'stage_event', stage, event });
      };
      const plumbing: Record<PropertyKey, unknown> = { [CHAIN_EMIT]: internalEmit };
      if (options.trace !== undefined) {
        const traceContext: TraceContext = {
          sink: options.trace,
          runId: options.runId?.() ?? `run_${++runCounter}`,
          now: options.now ?? Date.now,
        };
        plumbing[CHAIN_TRACE] = traceContext;
      }
      const seeded: object = Object.assign({}, input, plumbing);
      let source: Observable<object> = of(seeded);
      for (const op of operators) source = op(source);
      return source.pipe(
        map((ctx) => {
          const finalCtx = Object.assign({}, ctx) as Record<PropertyKey, unknown>;
          delete finalCtx[CHAIN_EMIT];
          delete finalCtx[CHAIN_TRACE];
          return finalCtx as Out;
        }),
      );
    },
  });
}
