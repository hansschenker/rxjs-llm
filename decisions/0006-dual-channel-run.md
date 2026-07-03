# ADR-0006: The dual-channel run() — { result$, progress$ }

**Status:** accepted · **Decision:** D3.3 (implemented ahead of the rest of
Module 3; reviewed point-by-point before implementation)

## Context

A chain stage that streams from a model must surface `text_delta` events to a
UI while contributing only the reduced final text to the context. One channel
cannot serve both consumers: the result wants exactly one value, the UI wants
every delta. This is where LangChain's callback system lives.

## Decision

`chain.run(input)` returns `{ result$, progress$ }`, governed by four rules:

### 1. Passivity

`progress$` never triggers execution. It is a plain Subject fed by whatever
execution `result$` drives; subscribing to it alone does nothing. Stages
receive an `emit` function (threaded through the context under a hidden
symbol key) that pushes stage-tagged events into the Subject.

### 2. Lifecycle coupling

`progress$` completes when the run ends — all three ways:

- **Complete:** one terminal `run_complete` event, then completion.
- **Error:** one terminal `run_failed` event carrying only the *message*,
  then completion. The error **object** travels on `result$` alone — one
  failure, one error handler. A progress-only UI still learns the run died
  (vs. a slow stage) from the terminal event, and both outcomes give
  consumers a uniform "this run is over" signal.
- **Cancellation** (last `result$` subscriber unsubscribes mid-flight):
  the execution is aborted — in-flight provider requests included — and
  both channels complete silently. No terminal event, no error (ADR-0005).

Invariant, pinned by test: every `progress$` stream that terminates on its
own ends with exactly one terminal event followed by completion.

### 3. Unobserved events are dropped

Plain Subject semantics — no buffering, no replay. This is a UI channel;
subscribe to `progress$` before `result$` to see everything. A stated
decision, not an accident.

### 4. One run() = one logical execution

Forced by passivity: `result$` cold-multi would pump a second execution's
events into the *same* Subject — correlation-id tagging would be damage
control. So `run()` is lazy (nothing before the first `result$`
subscription — laziness is the law worth keeping) and multicast after it,
with the outcome (final context, error, or cancellation) **latched
permanently**:

- `retry()`/`repeat()` on `result$` re-deliver the latched outcome — the
  same error object, zero new provider requests. Whole-chain retry is a
  *syntactically visible* new call:
  `defer(() => chain.run(input).result$).pipe(retry(n))`.
  Per-stage retry belongs inside the stage via Module 1's `retryWithBackoff`.
- Late subscribers to a completed run get the final context (ReplaySubject);
  to a failed run, the latched error; to a cancelled run, an immediate empty
  completion.

**Why the latch is hand-rolled rather than `share({ resetOnError: false,
resetOnComplete: false, resetOnRefCountZero: false })`:** no combination of
share's reset flags expresses the whole contract. `resetOnError`/
`resetOnComplete` default to `true`, so a bare refcount latch silently
re-executes under `retry()` — but latching `resetOnRefCountZero` to `false`
means an *abandoned* run keeps executing with nobody subscribed: nothing
aborts the in-flight provider call, violating the teardown law. Setting it
`true` instead re-executes on resubscription after abandonment — the same
back door. The contract needs refcount-zero-before-terminal to mean *abort
and latch as cancelled*, which is ~20 lines of explicit refcounting in
`runChain` (src/chain/chain.ts).

### Reconciliation with the Module 1 cold/unicast law

Module 1's law applies where the Observable *is* the request and
resubscription is the retry mechanism. `run(input)` is an application of
arguments to a workflow; re-execution of that size must be syntactically
visible — a second `run()` call — never a silent consequence of a second
subscription. Unicast was a consequence in Module 1, not the point; laziness
is the point, and it survives.

## Consequences

- The pinning test (test/chain/pinning.test.ts) asserts the contract over
  real HTTP: staggered double subscription → one provider request, both
  subscribers get the final context, one terminal event; failed run +
  `retry()` → request counter unmoved, same error object; unsubscribe →
  server observes the abort, both channels silent.
- Agents (Module 6, D6.4) reuse this contract unchanged.
- Execution is subscribed on the asap scheduler so fully-synchronous stages
  cannot emit in the first subscriber's call frame (no Zalgo).
