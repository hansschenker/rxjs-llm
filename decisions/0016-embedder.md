# ADR-0016: Embeddings mirror the ChatModel pattern

**Status:** accepted · **Decision:** D4.3

## Context

Embedding APIs are the simple sibling of chat APIs: same auth, same error
surface, same rate limits — but one JSON response instead of a stream.
They should cost one small interface, not a second framework.

## Decision

`Embedder = { embed(texts): Observable<Float32Array[]> }` — cold, lazy,
unicast, one HTTP request per subscribe, teardown aborts, index-aligned
results. Adapters reuse Module 1 wholesale:

- **`fetchJson`** joins the transport layer as the non-streaming sibling
  of `fetchStream`: identical laws, identical error mapping (HttpError /
  RateLimitError with Retry-After / TransportError / ParseError), one
  JSON body instead of framed bytes.
- Shipped adapters: **openai** (`/v1/embeddings`, the reference shape,
  optional `dimensions` truncation) and **ollama** (`/api/embed`, local
  and keyless). Voyage (Anthropic's partner) is deliberately omitted —
  it is the OpenAI shape with a different host and auth header, a
  ~20-line variation that adds nothing to the design; keeping the module
  bounded wins. Both adapters reject count mismatches as ParseError
  rather than silently mis-aligning vectors with texts.

**`embedBatched(embedder, opts)`** is the composition showcase the plan
asked for — nothing but Module 1 operators: `bufferCount` forms batches,
`rateLimit` (token bucket, injectable scheduler, marble-tested) spaces
requests, `concatMap` keeps one request in flight and preserves order,
and vectors zip back onto their chunks. A count mismatch inside a batch
is a loud RangeError. Cancellation mid-batch aborts the in-flight HTTP
request because the Embedder contract says so — the operator adds no
cancellation machinery of its own.

## Consequences

- `retryWithBackoff` composes around `embed()` calls unchanged — same
  taxonomy, same `isRetryable`.
- Vectors are `Float32Array` end to end: half the memory of number[],
  and the exact type the store's cosine scan wants.
