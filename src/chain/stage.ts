import {
  concatMap,
  defer,
  mergeMap,
  toArray,
  type Observable,
  type ObservableInput,
  type OperatorFunction,
} from 'rxjs';
import type { StreamEvent } from '../types';
import { traced, type TraceContext } from './trace';

/**
 * Hidden context keys carrying the run's plumbing. Enumerable on purpose:
 * stages merge contexts with Object.assign, and the plumbing must survive
 * every merge to reach downstream stages. chain() strips them before
 * anything is delivered on result$.
 */
export const CHAIN_EMIT: unique symbol = Symbol('rxjs-llm.chain.emit');
export const CHAIN_TRACE: unique symbol = Symbol('rxjs-llm.chain.trace');

/** What a stage body sees: forward a model event to the run's progress$. */
export type EmitFn = (event: StreamEvent) => void;

/** Internal signature — the chain tags each event with the emitting stage. */
export type InternalEmit = (stage: string, event: StreamEvent) => void;

export type StageFn<Ctx extends object, P extends object> = (
  ctx: Ctx,
  emit: EmitFn,
) => ObservableInput<P>;

function readEmit(ctx: object): InternalEmit | undefined {
  return (ctx as Record<PropertyKey, unknown>)[CHAIN_EMIT] as InternalEmit | undefined;
}

function readTrace(ctx: object): TraceContext | undefined {
  return (ctx as Record<PropertyKey, unknown>)[CHAIN_TRACE] as TraceContext | undefined;
}

/** Run one stage body against a context: emit wiring + tracing, no merge. */
function runBody<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
  ctx: Ctx,
): Observable<P> {
  const internal = readEmit(ctx);
  const emit: EmitFn = internal === undefined ? () => {} : (event) => internal(name, event);
  // defer, not from: a synchronous throw in fn becomes an error
  // NOTIFICATION inside the traced pipeline, so stage_error still fires.
  const body = defer(() => fn(ctx, emit));
  const trace = readTrace(ctx);
  return trace === undefined ? body : body.pipe(traced(name, trace));
}

/**
 * Merge the body's patches into the context, AFTER the body completes.
 * Buffering to completion keeps the stage lifecycle honest: without it, a
 * stage's value propagates through downstream stages before its own
 * complete notification fires, and traces interleave as
 * first:start → second:start → second:complete → first:complete.
 */
function mergePatches<Ctx extends object, P extends object>(
  body: Observable<P>,
  ctx: Ctx,
): Observable<Ctx & P> {
  return body.pipe(
    toArray(),
    mergeMap((patches) => patches.map((patch) => Object.assign({}, ctx, patch))),
  );
}

/**
 * A stage IS an operator (decision D3.2, ADR-0011): sequential composition
 * is just `pipe(stage(...), stage(...))`. The stage body returns a *patch*;
 * the operator merges it into the context, so the output type accumulates —
 * downstream stages can only reference keys upstream stages provably
 * produced (decision D3.1, ADR-0010).
 *
 * Used outside a chain, `emit` is a no-op and nothing is traced; inside,
 * both ride the context under hidden symbol keys.
 */
export function stage<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
): OperatorFunction<Ctx, Ctx & P> {
  return concatMap((ctx: Ctx) => mergePatches(runBody(name, fn, ctx), ctx));
}
