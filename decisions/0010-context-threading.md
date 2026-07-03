# ADR-0010: Context threading via accumulating record

**Status:** accepted · **Decision:** D3.1

## Context

A raw Observable pipeline carries only its latest value. LLM workflows need
earlier intermediate results downstream — the answer stage wants both the
original question and the summary produced two stages back. LangChain
solves this with runnable-bound input/output mappings; we want the typed,
boring answer.

## Decision

The unit of flow through a chain is `Ctx extends Record<string, unknown>`.
Each stage reads what it needs from the context and returns a **patch**;
the stage operator merges the patch (`Object.assign({}, ctx, patch)`) so
the value flowing on is `Ctx & Patch`.

Type-level: each stage *widens* the context type. Downstream stages can
only reference keys that upstream stages provably produced — a reference to
a missing key is a compile error, pinned by `@ts-expect-error` tests. The
run's input keys flow through untouched and are part of the final context.

Runtime collision policy is last-writer-wins (patch over context, later
parallel branch over earlier). The type level discourages collisions
naturally: colliding keys intersect, and incompatible intersections
collapse toward `never` at the point of use.

Merges are shallow and non-mutating: every stage output is a fresh object;
no stage can observe another's in-flight mutation.

## Consequences

- The context is the only data channel between stages; there is no side
  registry, so a chain's behavior is fully determined by its input.
- Hidden plumbing (the progress emitter, the trace context) rides the same
  record under symbol keys — enumerable so `Object.assign` merges carry
  them forward, stripped before anything reaches `result$` (ADR-0006).
