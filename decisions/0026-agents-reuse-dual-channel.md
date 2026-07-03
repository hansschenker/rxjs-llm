# ADR-0026: Agents reuse the chain contract — by extraction, not imitation

**Status:** accepted · **Decision:** D6.4

## Context

D6.4 says the agent exposes `{ result$, progress$ }` with D3.3's
semantics. Those semantics took three audited latch races to get right
(ADR-0006, the plans-file checklist). Re-implementing them in the agent
would mean re-earning that audit; imitating them loosely would mean two
subtly different contracts under one name.

## Decision

The dual-channel machinery moved OUT of `chain.ts` into
`src/chain/dual-channel.ts`: a taxonomy-agnostic
`dualChannel({ work, terminal })` that owns passivity, the terminal-event
protocol, dropped-when-unobserved, the hand-rolled outcome latch, the
microtask-deferred cancellation decision, and the no-Zalgo subscribe-on.
Chains contribute only what is chain-specific (plumbing symbols, the
operator pipe, symbol stripping); agents contribute the expand() loop and
the AgentEvent taxonomy. The existing chain test suite — latch races,
stragglers, firstValueFrom — verified the refactor unchanged.

Consequences of sharing one implementation:

- One `runAgent()` call = one execution; the outcome (including
  `max_iterations`) latches; `retry()` on `result$` re-delivers, never
  re-runs the loop's N model calls.
- Cancellation aborts the in-flight model call AND every in-flight tool
  (their AbortSignals fire), both channels complete silently.
- `progress$` interleaves model deltas (`model_event`, iteration-tagged)
  with tool lifecycle (`tool_start`/`tool_result`), ending in exactly one
  `agent_complete | agent_failed`.
- **An agent is a chain stage from the outside**: its result$ is an
  ObservableInput a stage body returns; its model deltas forward to the
  stage's `emit` with no adapter. The capstone test composes it exactly
  that way, with no special casing in either module.
