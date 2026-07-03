# ADR-0003: Two stream timeouts, not one

**Status:** accepted · **Decision:** D3

## Context

A single timeout conflates two distinct failure modes: the connection is
established but no data ever arrives (provider overloaded), and the stream
stalls mid-generation.

## Decision

`streamTimeout({ firstByteMs, idleMs })` maps onto RxJS
`timeout({ first, each })`. The two phases produce `TimeoutError` with a
`phase` discriminator and **distinct default retryability**:

- `first-byte` — nothing was generated; a retry costs nothing extra.
  Retryable by default.
- `idle` — tokens were already produced and billed; a blind retry duplicates
  cost and may duplicate side effects upstream. Not retryable by default,
  overridable per call.

## Consequences

`isRetryable` consults `TimeoutError.retryable`, so retry policy composes with
`retryWithBackoff` without special-casing timeouts.
