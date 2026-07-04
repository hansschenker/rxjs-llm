import {
  catchError,
  concatMap,
  defer,
  forkJoin,
  map,
  mergeMap,
  of,
  throwError,
  toArray,
  type Observable,
  type ObservableInput,
  type OperatorFunction,
} from 'rxjs';
import type { StreamEvent } from '../types.js';
import { traced, type TraceContext } from './trace.js';

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
const STAGE_TAG: unique symbol = Symbol('rxjs-llm.chain.stage-tag');

/**
 * Which stage an error escaped from. The name rides the error object as a
 * non-enumerable symbol property instead of a wrapper class, because the
 * latch contract (ADR-0006) re-delivers errors BY IDENTITY and consumers
 * match with instanceof / isRetryable — wrapping would break all three.
 * The innermost (actually failing) stage wins; outer stages never overwrite.
 */
export function stageOf(error: unknown): string | undefined {
  if (error === null || (typeof error !== 'object' && typeof error !== 'function')) {
    return undefined;
  }
  return (error as Record<PropertyKey, unknown>)[STAGE_TAG] as string | undefined;
}

function tagStage(error: unknown, name: string): void {
  if (error === null || (typeof error !== 'object' && typeof error !== 'function')) return;
  const record = error as Record<PropertyKey, unknown>;
  if (record[STAGE_TAG] !== undefined) return; // innermost stage wins
  try {
    Object.defineProperty(record, STAGE_TAG, { value: name, enumerable: false });
  } catch {
    /* frozen error object — the tag is best-effort */
  }
}

/**
 * Per-stage error policy (Module 3, Phase 5; ADR-0013):
 * - `'fail'` (default): the error propagates, tagged with the stage name.
 * - `'skip'`: the stage's patch is dropped and the context flows on —
 *   the output type becomes `Ctx & Partial<P>`, like a false `when()`.
 * - a function: fallback — invoked with (ctx, error), its patch merges
 *   as if the stage had succeeded. Errors thrown by the fallback itself
 *   propagate, tagged with the same stage name.
 */
export type StageErrorPolicy<Ctx extends object, P extends object> =
  | 'fail'
  | 'skip'
  | ((ctx: Ctx, error: unknown) => ObservableInput<P>);

export interface StageOptions<Ctx extends object, P extends object> {
  onError?: StageErrorPolicy<Ctx, P>;
}

function applyPolicy<Ctx extends object, P extends object>(
  name: string,
  body: Observable<Ctx & P>,
  ctx: Ctx,
  policy: StageErrorPolicy<Ctx, P>,
): Observable<Ctx & P> {
  return body.pipe(
    catchError((error: unknown) => {
      tagStage(error, name);
      if (policy === 'fail') return throwError(() => error);
      if (policy === 'skip') return of(ctx as Ctx & P);
      return mergePatches(
        defer(() => policy(ctx, error)).pipe(
          catchError((fallbackError: unknown) => {
            tagStage(fallbackError, name);
            return throwError(() => fallbackError);
          }),
        ),
        ctx,
      );
    }),
  );
}

function stageFn<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
): OperatorFunction<Ctx, Ctx & P>;
function stageFn<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
  options: { onError: 'skip' },
): OperatorFunction<Ctx, Ctx & Partial<P>>;
function stageFn<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
  options: StageOptions<Ctx, P>,
): OperatorFunction<Ctx, Ctx & P>;
function stageFn<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
  options?: StageOptions<Ctx, P>,
): OperatorFunction<Ctx, Ctx & P> {
  const policy = options?.onError ?? 'fail';
  return concatMap((ctx: Ctx) =>
    applyPolicy(name, mergePatches(runBody(name, fn, ctx), ctx), ctx, policy),
  );
}

/**
 * Conditional stage — an `if` inside concatMap, not a new abstraction
 * (ADR-0011). When the predicate is false the context passes through
 * untouched, so the output type is honestly `Ctx & Partial<P>`:
 * downstream must handle the keys' absence. A skipped stage emits no
 * progress and no trace events — nothing ran.
 */
function when<Ctx extends object, P extends object>(
  name: string,
  predicate: (ctx: Ctx) => boolean,
  fn: StageFn<Ctx, P>,
): OperatorFunction<Ctx, Ctx & Partial<P>> {
  return concatMap((ctx: Ctx) =>
    predicate(ctx)
      ? mergePatches(runBody(name, fn, ctx), ctx)
      : of(ctx as Ctx & Partial<P>),
  );
}

export const stage = Object.assign(stageFn, { when });

type UnionToIntersection<U> = (U extends unknown ? (x: U) => void : never) extends (
  x: infer I,
) => void
  ? I
  : never;

// eslint-style note: `any[]` is required for contravariant parameter inference.
type PatchOf<F> = F extends (...args: any[]) => ObservableInput<infer P> ? P : never;

export type ParallelBranches<Ctx extends object> = Record<string, StageFn<Ctx, object>>;

export type MergedPatch<B> = UnionToIntersection<PatchOf<B[keyof B]>>;

/**
 * Parallel fan-out — forkJoin inside one concatMap (ADR-0011). Every
 * branch runs concurrently against the SAME pre-join context: a branch
 * cannot see a sibling's patch (enforced at the type level). The joined
 * patch is the intersection of all branches' patches; on runtime key
 * collision the later branch (declaration order) wins. Each branch emits
 * progress and traces under its own key as the stage name; forkJoin takes
 * each branch's LAST value as its patch.
 */
function parallel<Ctx extends object, B extends ParallelBranches<Ctx>>(
  branches: B,
): OperatorFunction<Ctx, Ctx & MergedPatch<B>> {
  return concatMap((ctx: Ctx) => {
    const keys = Object.keys(branches);
    if (keys.length === 0) return of(ctx as Ctx & MergedPatch<B>);
    return forkJoin(keys.map((key) => runBody(key, branches[key]!, ctx))).pipe(
      map((patches) => Object.assign({}, ctx, ...patches) as Ctx & MergedPatch<B>),
    );
  });
}

export const stages = { parallel };
