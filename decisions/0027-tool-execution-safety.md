# ADR-0027: Tool execution safety — a result is always produced

**Status:** accepted · **Decision:** D6.3

## Context

The loop appends one tool_result per tool_use and recurses. Anything that
prevents a result — a hung tool, a thrown error, a missing catch — stalls
the loop forever or kills the run for a recoverable condition. And when
the consumer walks away, everything in flight must stop: model call and
tools alike.

## Decision

- **A tool result is ALWAYS produced.** `executeToolCall` never errors:
  success, validation error, execution failure, or timeout notice — each
  becomes a `{ role: 'tool' }` message. The loop cannot stall waiting for
  a missing tool_result, by construction; the property test (Phase 4)
  verifies the 1:1 pairing.
- **Timeout and retries map onto Module 1's operators.** `timeoutMs`
  bounds the whole execution via `streamTimeout` (both phases set, so a
  tool that emits then hangs before completing is still caught);
  `retries` wraps in `retryWithBackoff` with an always-retry predicate —
  tool errors are arbitrary, the author opted in. Each retry gets a fresh
  AbortController.
- **Cancellation reaches everything.** Tools receive an `AbortSignal`
  wired to Observable teardown; unsubscribing from the agent aborts the
  in-flight model call AND every in-flight tool execution (mergeMap
  teardown propagates to all active inners). The cancellation matrix —
  unsubscribe during (a) model streaming, (b) tool execution, (c) between
  iterations — is pinned with multi-AbortSignal assertions and silence on
  both channels.
- **Post-abort promise rejections are swallowed** (ADR-0024 detail,
  load-bearing here): a rejection arriving after our own abort would hit
  rxjs's closed-subscriber path and become an uncaught exception — the
  "no unhandled rejections" half of the matrix.

## Consequences

- Concurrency-capped parallel tool execution (`mergeMap`, default 4) with
  results appended in call order — transcripts stay deterministic under
  any completion interleaving.
- A tool author who does nothing gets safe defaults; `timeoutMs` and
  `retries` are opt-in per tool, not loop-wide policy.
