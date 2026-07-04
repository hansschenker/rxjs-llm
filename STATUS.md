# STATUS

**All six modules complete.** The capstone test — retrieve → agent with a
retrieval tool → memory record, composed as a chain over real HTTP against
the mock server — is green: the repo's definition of done, met at v0.6.0.

## Module 6 phase checklist

- [x] Phase 1 — Zod-defined tools: validation → self-correction messages,
      abortable execution, timeout/retries on Module 1's operators.
      ADR-0024 (D6.2; zod v4 eliminated zod-to-json-schema).
- [x] Phase 2 — The expand() loop (max-iterations as an OUTCOME) and the
      dual-channel machinery extracted from chain.ts so agents get the
      audited D3.3 contract verbatim. ADR-0025 (D6.1), ADR-0026 (D6.4).
- [x] Phase 3 — Safety rails: budget, per-tool timeout, concurrency cap,
      and the full cancellation matrix (mid-stream / mid-tool / between
      iterations, multi-AbortSignal). ADR-0027 (D6.3).
- [x] Phase 4 — Mock-server scenario DSL, THE CAPSTONE (six modules, one
      pipeline, two wire requests, tool result = Module 4's retrieval),
      loop-state property tests.
- [x] Phase 5 — README walkthrough, governance updates, v0.6.0.

280 tests, all green; strict `tsc` clean. Runtime dependencies: `rxjs` +
`zod` (pglite/drizzle opt-in via subpath). ~3,100 non-blank source lines.

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | **done — v0.1.0** |
| 2 | Prompts | **done — v0.2.0** |
| 3 | Chains | **done — v0.3.0** |
| 4 | Indexes / RAG | **done — v0.4.0** |
| 5 | Memory | **done — v0.5.0** |
| 6 | Agents / tool use | **done — v0.6.0** |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/` (27 ADRs).
The book's first two chapters live in `book/` (`chapter-1.md`: the model
interface + the dual channel; `chapter-2.md`: Modules 2–6 + the capstone),
built from the commit history (`git log --reverse --oneline`) as their
skeleton. Possible next steps: chapter revisions, a v1.0.0 cut, npm publish.
