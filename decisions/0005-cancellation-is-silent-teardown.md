# ADR-0005: Cancellation is silent teardown, never an error

**Status:** accepted

## Context

Most HTTP client stacks surface cancellation as an `AbortError` rejection.
If adapters forwarded that to subscribers, every consumer would need to
filter "errors" that are really just its own unsubscribe — and retry
operators would treat user cancellation as a retryable failure.

## Decision

Unsubscribing is the one and only cancellation mechanism, and it is silent:

- Teardown aborts the underlying `fetch` via `AbortController`.
- The resulting `AbortError` from the fetch promise/reader is swallowed by
  the transport layer; nothing is delivered to the (already unsubscribed)
  observer, and no unhandled rejection escapes.
- There is deliberately **no `AbortError` in the error taxonomy** and no
  external `AbortSignal` parameter on `ChatModel` methods: callers who need
  signal-driven cancellation wrap the subscription (`takeUntil(fromEvent(
  signal, 'abort'))`), keeping one cancellation path instead of two.

## Consequences

- The error channel means one thing: the operation failed. Retry logic and
  UI error states never see cancellations.
- Every transport/adapter test asserts both directions: unsubscribe fires the
  AbortSignal, and no error/next is delivered afterwards.
