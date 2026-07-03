# STATUS

**Current module:** 2 — Prompts — **complete, tagged v0.2.0**
**Pulled forward:** Module 3's D3.3 (dual-channel `run()`) — implemented with
ADR-0006 after full design review, latch race audited; the rest of Module 3
remains planned.
**Next:** Module 3 — Chains, remaining phases (stage/context types are
partially in place from the D3.3 work; see `rxjs-llm-module-plans.md`).

## Module 2 phase checklist

- [x] Phase 1 — Placeholder type machinery: `ExtractVars` ({{-escape before
      variable branch), `promptTemplate` + tagged `prompt` as two exports,
      exact-match type tests, fast-check round-trip property test. ADR-0007.
- [x] Phase 2 — Message builders, `messagePrompt`, `withHistory()` slot
      between few-shot and the final user turn. ADR-0008.
- [x] Phase 3 — Format helpers: `asBullets`, `noJargon`, `asJson` (plain
      JSON Schema; zod deferred to Module 6). ADR-0009.
- [x] Phase 4 — README section, tag v0.2.0.

147 tests, all green; strict `tsc` clean. Sole runtime dependency: `rxjs`.

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | **done — v0.1.0** |
| 2 | Prompts | **done — v0.2.0** |
| 3 | Chains | D3.3 core shipped early (dual-channel `run()`, ADR-0006); phases 1–2, 4–6 planned |
| 4 | Indexes / RAG | planned |
| 5 | Memory | planned |
| 6 | Agents / tool use | planned |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/`.
