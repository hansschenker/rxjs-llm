# ADR-0017: Loaders emit documents as a stream

**Status:** accepted · **Decision:** D4.1

## Context

Directory loads, paginated APIs, and crawls are naturally incremental. A
promised array forces the consumer to wait for the last document before
processing the first, and makes cancellation meaningless.

## Decision

`Loader = (source, opts) => Observable<Doc>` with
`Doc = { id, text, metadata }`. Three loaders only:

- **`textFileLoader(root)`** — recursive, deterministic (sorted) walk; one
  Doc per file, emitted as read; a RegExp or predicate filters by relative
  path. Unsubscribing stops the walk at the next file boundary. `node:fs`
  arrives via **dynamic import**, so the package's static import graph
  stays web-standard (ADR-0004) and only this loader is Node/Bun-bound.
- **`webLoader(urls)`** — fetch via Module 1's `fetchStream` (GET), so
  teardown-aborts is inherited, not re-implemented. Sequential across URLs
  (polite to single hosts). Extraction is deliberately crude — strip
  script/style/head, break at block tags, decode common entities, collapse
  whitespace. Full readability/boilerplate removal is out of scope.
- **`jsonLoader(arrayOrJson, { text, id? })`** — one Doc per record;
  non-mapped fields become metadata; pure apart from lazy parsing.

Docling/OCR/PDF pipelines are explicitly NOT here — `NON_GOALS.md` already
assigns document understanding to `rxjs-rag`.

## Consequences

- `loader.pipe(splitDocs, embedBatched, upsertInto)` starts embedding
  before the directory walk finishes — ingestion is a single streaming
  pipe with backpressure by construction (concatMap in the batcher).
- Cancellation semantics come from the Observable contract, identical to
  every other module: unsubscribe stops file reads and aborts in-flight
  HTTP.
