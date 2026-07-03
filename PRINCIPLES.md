# PRINCIPLES

Shared conventions for every module. These are law — most are enforced by tests.

1. **Cold, lazy, unicast, teardown-complete.** Every Observable-returning API:
   each subscribe is one unit of work (one HTTP request for model calls); no
   side effects before subscribe; subscribers never share; unsubscribe tears
   down the underlying work (asserted via AbortSignal in tests).
2. **No Zalgo.** Nothing emits synchronously in the subscribe call frame.
3. **Cancellation is silent teardown.** It never surfaces on the error
   channel. There is no `AbortError` in the taxonomy (ADR-0005).
4. **Errors are typed and classified.** Everything crossing the error channel
   is an `LlmError` subtype; `isRetryable` is the single retry predicate.
5. **Each phase = one commit.** The git history reads as a tutorial.
6. **Marble tests for every custom operator** via injected `TestScheduler`;
   law/property tests for anything claiming algebraic structure.
7. **Governance:** keep `STATUS.md` current; file an ADR in `decisions/` for
   every design decision; respect `NON_GOALS.md`; no new runtime dependencies
   without an ADR.
8. **Runtime/tooling:** Bun, Vitest, `rxjs@7.8`, strict TypeScript, ESM only.
