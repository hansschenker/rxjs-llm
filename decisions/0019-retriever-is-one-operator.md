# ADR-0019: The retriever is one operator

**Status:** accepted · **Decision:** D4.5

## Context

LangChain retrievers are a class hierarchy with their own invocation
protocol. Here, retrieval is a transformation from a query string to a
context block — which is what operators are for.

## Decision

`retrieveContext(store, embedder, k, opts?)` is an
`OperatorFunction<string, RetrievedContext>`: embed the query, top-k the
store (optional metadata filter), optionally rerank, format. It drops into
a chain stage unchanged — `of(ctx.question).pipe(retrieveContext(...))` —
and inherits chain cancellation because every constituent is
teardown-complete.

- **Formatting:** one `[source: metadata.source ?? id]` block per match,
  blank-line separated, trimmed to `tokenBudget` in WHOLE blocks. The top
  match is always included: a tight budget degrades to "best chunk",
  never to an empty context. `matches` on the result lists exactly what
  the context contains, in order — no silent divergence between the
  string and the structure.
- **Rerank is a hook**, `(query, matches) => ObservableInput<QueryMatch[]>`,
  not an abstraction: an LLM-judge reranker is one stage-shaped function
  away, and the default is identity.
- **Ingestion plumbing** (`ingest.ts`): `upsertInto(store)` buffers
  embedded chunks and emits per-batch counts; `ingest(docs$, opts)` is the
  whole load → split → embed → upsert pipe emitting cumulative progress.
  Chunk provenance (docId, start, end) folds into entry metadata so
  retrieved contexts can cite exact source spans.

## Consequences

- The Module 4 e2e proves relevance with a REAL embedding (deterministic
  bag-of-words), not a mocked ranking: shared vocabulary genuinely drives
  cosine order, so "the espresso question retrieves the espresso chunk"
  tests the pipeline, not the test author's wiring.
- Cancellation mid-ingest aborts the in-flight embedder call and stops
  upserts — pinned, per the Module 1 ethos.
