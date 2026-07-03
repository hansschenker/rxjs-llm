# ADR-0015: VectorStore is a small interface, verified by a contract suite

**Status:** accepted · **Decision:** D4.4

## Context

Vector-database abstractions grow filters-as-DSL, namespaces, hybrid
search, and driver registries. This repo needs exactly three operations,
and it needs two implementations (in-memory; PGlite) to be provably
interchangeable.

## Decision

Three Observable-returning methods — `upsert` (replaces on id collision,
returns count), `query` (cosine top-k, optional metadata predicate),
`delete` (returns removed count) — all cold and lazy per the Module 1
laws. Scores are cosine similarity in [-1, 1].

**The contract IS a test file** (`test/index/store-contract.ts`): one
parameterized suite of law tests — ranking against hand-computed cosine
fixtures, replace-on-upsert, filter-before-k, metadata round-trip,
laziness, and the fast-check property that querying with any stored
vector returns its own id first. Every implementation runs the identical
suite; passing it is what "is a VectorStore" means.

- **In-memory:** brute-force cosine with precomputed norms. Fine to ~50k
  vectors (a full scan of 50k×1536 dims is single-digit ms); beyond that
  you want a real index, which is a NON_GOAL (no HNSW, no services).
  Dimension mismatches throw loudly; zero vectors score 0, never NaN.
- **Filters are predicates** (`(metadata, id) => boolean`), not a query
  DSL. The in-memory store applies them pre-k in the scan; SQL-backed
  stores may over-fetch and filter post-ordering — the contract only
  fixes observable behavior.

## Consequences

- Adding a store implementation = one factory + one line invoking the
  contract suite.
- Predicate filters cannot be pushed down to SQL; acceptable at the
  bounded scale this repo states.
