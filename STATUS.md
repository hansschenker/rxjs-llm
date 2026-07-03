# STATUS

**Current module:** 3 — Chains — **complete, tagged v0.3.0**
**Next:** Module 4 — Indexes / RAG, or Module 5 — Memory (small; pairs well
with 4). See `rxjs-llm-module-plans.md`.

## Module 3 phase checklist

- [x] Phase 1 — `stage()` + context merge types (shipped with the D3.3
      pull-forward); ADR-0010 (D3.1), ADR-0011 (D3.2).
- [x] Phase 2 — `traced()` + TraceSink (ADR-0012, D3.4). Surfaced and fixed
      the third latch race (firstValueFrom-style consumers) and the stage
      lifecycle ordering bug (bodies now buffer to completion).
- [x] Phase 3 — `progress$` dual channel (pulled forward earlier;
      ADR-0006, D3.3; latch race audited).
- [x] Phase 4 — `stages.parallel` (forkJoin) + `stage.when`; sibling-branch
      type isolation pinned.
- [x] Phase 5 — Error policies `fail | skip | fallback`; `stageOf()`
      attribution without wrapping (ADR-0013); retryWithBackoff composes
      inside stage bodies.
- [x] Phase 6 — End-to-end test: prompts → adapter → chain with every
      feature, over real HTTP against the mock server. README section.

173 tests, all green; strict `tsc` clean. Sole runtime dependency: `rxjs`.

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | **done — v0.1.0** |
| 2 | Prompts | **done — v0.2.0** |
| 3 | Chains | **done — v0.3.0** |
| 4 | Indexes / RAG | planned |
| 5 | Memory | planned |
| 6 | Agents / tool use | planned |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/` (13 ADRs).
