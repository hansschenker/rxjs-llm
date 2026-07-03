# ADR-0013: Per-stage error policy; stage tagging without wrapping

**Status:** accepted · **Decision:** Module 3, Phase 5

## Context

A chain needs to say, per stage, whether a failure kills the run, is
tolerable, or has a substitute. And when a run dies, the consumer needs to
know *which stage* — but the D3.3 latch re-delivers errors **by identity**
(`retry()` after a failed run surfaces the very same object, pinned by
test), and Module 1 consumers match errors with `instanceof` and
`isRetryable`. Any wrapper class breaks all three.

## Decision

### Policy: `onError: 'fail' | 'skip' | fallback`

- **`'fail'`** (default) — the error propagates; the run fails.
- **`'skip'`** — the patch is dropped, the context flows on. The output
  type becomes `Ctx & Partial<P>` (same honesty as a false `stage.when`),
  and the *run* completes — but the trace still records `stage_error`:
  policy decides flow, never observability.
- **fallback function** — `(ctx, error) => ObservableInput<P>`; its patch
  merges as if the stage had succeeded. A failing fallback propagates.

The policy sits OUTSIDE the stage body, so Module 1's `retryWithBackoff`
composes INSIDE it: retries happen first (invisible to the run — the trace
shows one stage execution, progress shows one clean outcome), the policy
judges only the final failure.

### Stage attribution: a symbol tag, not a wrapper

The failing stage's name rides the error object as a **non-enumerable
symbol property**, read via `stageOf(error)`. The innermost stage wins;
outer stages never overwrite (a chain nested inside a stage keeps its own
attribution). Enumeration, spread, and `JSON.stringify` see an untouched
error. `run_failed` on progress$ carries the stage name as data.

Rejected: `StageError extends Error { cause }` — breaks error identity
(latch), `instanceof HttpError` (consumers), and `isRetryable`
(retry operator) in one move. Mutating a foreign error object is the
lesser evil, and it is best-effort: frozen or primitive errors simply go
untagged.

## Consequences

- `stageOf()` is the only sanctioned way to read attribution; the symbol
  itself is not exported.
- `'skip'` on a stage whose keys downstream stages *require* is a compile
  error at the point of use — the type system enforces that skippable
  stages produce optional context.
