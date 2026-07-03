# STATUS

**Current module:** 1 — Uniform Model Interface
**Current phase:** 3 of 6 (Anthropic adapter)

## Module 1 phase checklist

- [x] Phase 1 — Scaffold, `types.ts`, `errors.ts`, governance docs. Taxonomy tests pass.
- [x] Phase 2 — Transport: `fetchStream` (teardown-tested), SSE parser (adversarial fixtures).
- [ ] Phase 3 — Anthropic adapter, full event mapping incl. tool-use and in-stream errors.
- [ ] Phase 4 — OpenAI + Ollama adapters; NDJSON framing joins the transport layer.
- [ ] Phase 5 — Resilience operators: `retryWithBackoff`, `streamTimeout`, `rateLimit`.
- [ ] Phase 6 — Mock provider server integration tests, README with per-provider examples, tag `v0.1.0`.

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | in progress |
| 2 | Prompts | planned |
| 3 | Chains | planned |
| 4 | Indexes / RAG | planned |
| 5 | Memory | planned |
| 6 | Agents / tool use | planned |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/`.
