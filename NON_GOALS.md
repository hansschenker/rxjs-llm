# NON_GOALS

Things this repo deliberately does not do. Additions require an ADR.

- **No `Runnable` reinvention.** Chains are RxJS pipes; stages are operators.
  If a feature needs a new abstraction where an operator would do, it's out.
- **No ReAct prompt scaffolding.** Providers reason natively over tool-use
  events; prompt-engineered agent loops are legacy.
- **No document-understanding integrations** (Docling, OCR, PDF pipelines).
  Loaders here are text-file, web, and JSON only; the rest belongs to `rxjs-rag`.
- **No external vector databases / HNSW.** In-memory brute-force cosine
  (bounded ~50k vectors) and PGlite via Drizzle only.
- **No memory persistence layer.** `snapshot()/restore()` only; hosts persist
  however they like.
- **No API keys in CI.** All integration tests run against the local mock
  provider server.
- **No new runtime dependencies without an ADR.** Currently sanctioned:
  `rxjs`. Pre-approved when their module lands: `zod` + `zod-to-json-schema`
  (Module 6), optional `tiktoken` behind a dynamic import (Module 4).
