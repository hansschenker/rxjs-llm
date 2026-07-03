# ADR-0004: Runtime and tooling

**Status:** accepted · **Decision:** D4

## Decision

Bun (package manager / runtime), Vitest (test runner), `rxjs@7.8`, strict
TypeScript (`strict` plus `noUncheckedIndexedAccess`,
`exactOptionalPropertyTypes`, `verbatimModuleSyntax`), ESM only. Matches the
`rxjs-remix` setup.

## Notes

- Tests run via `bun run test` → `vitest run`. Runtime APIs used by the
  library itself are web-standard only (`fetch`, `ReadableStream`,
  `TextDecoder`, `AbortController`) so the package runs unmodified on Bun,
  Node ≥ 18, Deno, and browsers.
- The mock provider server for integration tests uses `node:http` rather than
  `Bun.serve` so the test suite also runs under plain Node CI runners; this is
  a test-only concern and does not add a runtime dependency.
- Sole runtime dependency: `rxjs@^7.8.1`.
