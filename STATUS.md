# STATUS

**Current module:** 5 — Memory — **complete, tagged v0.5.0**
**Next:** Module 6 — Agents / tool use — the capstone. Its final test
(retrieve → agent → memory, composed as a chain, against the mock server)
is the repo's definition of done. See `rxjs-llm-module-plans.md`.

## Module 5 phase checklist

- [x] Phases 1+2 — Core reducer (scan over the turn stream) + full/window/
      token-budget views; reactivity and suffix/budget/pairing properties.
      ADR-0020 (D5.1), ADR-0021 (D5.3).
- [x] Phase 3 — summaryView: the async fold — eventually consistent,
      exhaustMap-non-overlapping (with the observeOn re-trigger fix),
      failure-tolerant, abortable via dispose(). ADR-0022 (D5.2).
- [x] Phase 4 — snapshot/restore with the equivalence property; turns are
      truth, derived state is not snapshotted. ADR-0023 (D5.4).

252 tests, all green; strict `tsc` clean. Core runtime dependency: `rxjs`
only (pglite/drizzle remain opt-in via subpath).

## Modules

| # | Module | Status |
|---|--------|--------|
| 1 | Uniform Model Interface | **done — v0.1.0** |
| 2 | Prompts | **done — v0.2.0** |
| 3 | Chains | **done — v0.3.0** |
| 4 | Indexes / RAG | **done — v0.4.0** |
| 5 | Memory | **done — v0.5.0** |
| 6 | Agents / tool use | planned |

Full plans: `rxjs-llm-module-plans.md`. Decisions: `decisions/` (23 ADRs).
