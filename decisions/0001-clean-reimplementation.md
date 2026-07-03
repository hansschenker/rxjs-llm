# ADR-0001: Clean re-implementation instead of depending on llm-stream-adapter

**Status:** accepted · **Decision:** D1

## Context

`llm-stream-adapter` already contains SSE frame-boundary parsing, an error
taxonomy, and cancellation work. Options were: (a) depend on it as a package,
(b) fold its code into this repo, (c) re-implement cleanly here.

## Decision

(c) — clean re-implementation. This repo is the reference implementation and
the book artifact; a fresh, audited implementation with its own test suite is
worth more than a dependency edge, and "pure TypeScript and RxJS" is the
project's stated identity. `rxjs-llm` becomes the canonical home for this code.

## Consequences

- Zero runtime dependencies beyond `rxjs`.
- The SSE parser and error taxonomy are re-derived from scratch against
  adversarial fixtures, not inherited.
- `llm-stream-adapter` may later be deprecated in favor of this package.
