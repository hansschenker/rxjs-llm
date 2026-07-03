# ADR-0018: PGlite + Drizzle store as an opt-in module

**Status:** accepted · **Decision:** D4.4 (second implementation) + the
dependency exception NON_GOALS requires an ADR for

## Context

The store contract needs a second, durable implementation to prove the
interface is real — and it must run in CI with no external services
(NON_GOALS). PGlite is WASM Postgres with pgvector; Drizzle is the query
layer, mirroring the Fitness Assistant port.

## Decision

`pgliteStore({ dimensions, dataDir?, tableName? })` in
`src/index/store/pglite.ts`, exported ONLY via the `rxjs-llm/pglite`
subpath — never from the package root — with every dependency import
dynamic. Consequence: `rxjs` remains the sole dependency of the core;
`@electric-sql/pglite`, `@electric-sql/pglite-pgvector`, and `drizzle-orm`
are devDependencies here and become the consumer's own dependencies the
moment they import the subpath. Type-only imports are erased, so
type-checking the package never needs them at runtime.

Implementation notes:

- **pgvector via `@electric-sql/pglite-pgvector`** — PGlite ≥0.5 no longer
  bundles the vector extension under `@electric-sql/pglite/vector`; it
  moved to its own package with the same `vector` export.
- **DDL is raw SQL; queries are Drizzle** — drizzle-kit is a migration
  tool, not runtime DDL. The `pgTable` schema object is what queries
  type-check against; `cosineDistance` orders, `1 - distance` is the
  score.
- **Fixed dimensions.** A pgvector column has a declared dimension, so the
  store does too — and this forced an honest change to the CONTRACT: the
  factory now takes `dimensions`, because the suite exercises 2/3/4-dim
  vectors and only the in-memory store could pretend not to care.
- **Predicate filters scan post-ordering** (no SQL push-down for a JS
  predicate); without a filter the database LIMITs. Acceptable at the
  stated ~50k bound (ADR-0015).

## Consequences

- The identical contract suite passes against both stores; the WASM
  instance-per-test costs ~1.3s each — the price of service-free CI.
- Persistent use is one `dataDir` away; this repo only tests in-memory.
