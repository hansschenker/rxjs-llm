# ADR-0011: A stage is an operator; chains are pipes

**Status:** accepted · **Decision:** D3.2

## Context

LangChain's `Runnable` reinvents composition: sequence, parallel, branch,
retry — each a bespoke class. RxJS already has a composition algebra;
NON_GOALS forbids reinventing it.

## Decision

`stage(name, fn)` where `fn: (ctx, emit) => ObservableInput<Patch>` returns
an ordinary `OperatorFunction<Ctx, Ctx & Patch>`, implemented with
`concatMap`. Everything else follows from operator algebra:

- **Sequence** is `pipe(stage(...), stage(...))` — chains ARE pipes.
  `chain<In>().pipe(...)` merely remembers the operator list and adds the
  dual-channel `run()` (ADR-0006).
- **Parallel fan-out** is `stages.parallel({ a: fnA, b: fnB })` — a
  `forkJoin` inside one `concatMap`. Branches run concurrently against the
  SAME pre-join context; the joined patch is the intersection of the
  branches' patches. A branch cannot see a sibling's keys — enforced at the
  type level and pinned by test.
- **Branching** is `stage.when(name, predicate, fn)` — an `if` inside
  `concatMap`, not a new abstraction. A skipped stage passes the context
  through untouched, so its output type is `Ctx & Partial<Patch>`: the
  honest type — downstream must handle the keys' absence.
- A stage used OUTSIDE a chain is still just an operator over any
  `Observable<Ctx>`; `emit` degrades to a no-op and tracing to nothing.

Because stage bodies return `ObservableInput`, Module 1's operators compose
inside a stage unchanged: `model.stream(...).pipe(retryWithBackoff(...),
collectText(emit))` — resilience is the stage author's one-liner, not a
framework feature.

## Consequences

- Associativity for free: `pipe(stage(f), stage(g))` ≡ `pipe(stage(g ∘ f))`
  up to trace events, because `concatMap` composition is associative.
- No scheduler, cancellation, or error semantics of our own invention —
  a chain inherits RxJS's, which Module 1 already tests against.
