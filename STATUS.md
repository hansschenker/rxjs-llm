# STATUS

**Current module:** 1 — Uniform Model Interface — **complete, tagged v0.1.0**
**Pulled forward:** Module 3's D3.3 (dual-channel `run()`) — implemented with
ADR-0006 after full design review; the rest of Module 3 remains planned.
**Next:** Module 2 — Prompts (see `rxjs-llm-module-plans.md`)

## Module 1 phase checklist

- [x] Phase 1 — Scaffold, `types.ts`, `errors.ts`, governance docs. Taxonomy tests pass.
- [x] Phase 2 — Transport: `fetchStream` (teardown-tested), SSE parser (adversarial fixtures).
- [x] Phase 3 — Anthropic adapter, full event mapping incl. tool-use and in-stream errors.
- [x] Phase 4 — OpenAI + Ollama adapters; NDJSON framing joins the transport layer.
- [x] Phase 5 — Resilience operators: `retryWithBackoff`, `streamTimeout`, `rateLimit`.
- [x] Phase 6 — Mock provider server integration tests, README with per-provider examples.

111 tests, all green; strict `tsc` clean. Sole runtime dependency: `rxjs`.

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | **done — v0.1.0** |
| 2 | Prompts | planned |
| 3 | Chains | D3.3 core shipped early (dual-channel `run()`, ADR-0006); phases 1–2, 4–6 planned |
| 4 | Indexes / RAG | planned |
| 5 | Memory | planned |
| 6 | Agents / tool use | planned |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/`.
