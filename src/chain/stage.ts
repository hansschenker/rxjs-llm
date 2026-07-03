import { concatMap, from, map, type ObservableInput, type OperatorFunction } from 'rxjs';
import type { StreamEvent } from '../types';

/**
 * Hidden context key carrying the run's progress emitter. Enumerable on
 * purpose: stages merge contexts with Object.assign, and the emitter must
 * survive every merge to reach downstream stages. chain() strips it before
 * anything is delivered on result$.
 */
export const CHAIN_EMIT: unique symbol = Symbol('rxjs-llm.chain.emit');

/** What a stage body sees: forward a model event to the run's progress$. */
export type EmitFn = (event: StreamEvent) => void;

/** Internal signature — the chain tags each event with the emitting stage. */
export type InternalEmit = (stage: string, event: StreamEvent) => void;

/**
 * A stage IS an operator (decision D3.2): sequential composition is just
 * `pipe(stage(...), stage(...))`. The stage body returns a *patch*; the
 * operator merges it into the context, so the output type accumulates —
 * downstream stages can only reference keys upstream stages provably
 * produced (decision D3.1).
 *
 * Used outside a chain, `emit` is a no-op; inside, it feeds progress$.
 */
export function stage<Ctx extends object, P extends object>(
  name: string,
  fn: (ctx: Ctx, emit: EmitFn) => ObservableInput<P>,
): OperatorFunction<Ctx, Ctx & P> {
  return concatMap((ctx: Ctx) => {
    const internal = (ctx as Record<PropertyKey, unknown>)[CHAIN_EMIT] as
      | InternalEmit
      | undefined;
    const emit: EmitFn = internal === undefined ? () => {} : (event) => internal(name, event);
    return from(fn(ctx, emit)).pipe(map((patch) => Object.assign({}, ctx, patch)));
  });
}
