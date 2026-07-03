# STATUS

**Current module:** 4 — Indexes / RAG — **complete, tagged v0.4.0**
**Next:** Module 5 — Memory (small), then Module 6 — Agents (the capstone).
See `rxjs-llm-module-plans.md`.

## Module 4 phase checklist

- [x] Phase 1 — Splitter: pure, lossless, offset-carrying; property tests
      for partition/budget/surrogates. Tokenizer interface, chars/4
      default, no tiktoken. ADR-0014.
- [x] Phase 2 — In-memory vector store + the shared contract suite (the
      store law tests). ADR-0015.
- [x] Phase 3 — Embedder interface (openai + ollama adapters), fetchJson
      transport sibling, embedBatched (bufferCount + rateLimit +
      concatMap, marble-tested). ADR-0016.
- [x] Phase 4 — Loaders: text-file (dynamic node:fs), web (fetchStream +
      crude extraction), json. Fixture corpus added. ADR-0017.
- [x] Phase 5 — PGlite + pgvector via Drizzle, opt-in `rxjs-llm/pglite`
      subpath, contract suite generalized to fixed dimensions. ADR-0018.
- [x] Phase 6 — retrieveContext (one operator), upsertInto/ingest,
      end-to-end retrieval over the fixture corpus with a real
      (deterministic bag-of-words) embedding. ADR-0019.

230 tests, all green; strict `tsc` clean. Core runtime dependency: `rxjs`
only (pglite/drizzle are opt-in via subpath, dev-installed here).

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | **done — v0.1.0** |
| 2 | Prompts | **done — v0.2.0** |
| 3 | Chains | **done — v0.3.0** |
| 4 | Indexes / RAG | **done — v0.4.0** |
| 5 | Memory | planned |
| 6 | Agents / tool use | planned |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/` (19 ADRs).
