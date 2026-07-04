# ADR-0028: v1.0.0 publish shape — compiled ESM, explicit specifiers, optional peers

**Status:** accepted · **Decision:** post-plan (the plan ends at v0.6.0; this
governs how the finished library ships to npm)

## Context

Through v0.6.0 the package exported raw `.ts` sources — fine for this repo's
Bun/Vitest world, useless for plain Node consumers and for anyone not
compiling their dependencies. A 1.0 on npm must load under `node` with no
tooling. Node ESM resolution does not do extension guessing: every relative
import in emitted JS must carry its `.js` extension, and TypeScript's
`NodeNext` mode is the only emit mode that enforces this at compile time.

## Decision

- **Source specifiers gain explicit `.js` extensions** (32 files, mechanical
  codemod). TypeScript's extension substitution resolves `./x.js` → `x.ts`
  in both the dev config (`bundler` resolution) and Vitest, so tests and
  typecheck are unaffected; the emitted JS is Node-ESM-correct verbatim.
- **`tsconfig.build.json`** extends the dev config with
  `module: NodeNext` + declarations, emitting to `dist/`. The dev config
  stays `noEmit` — two configs, one source of truth for strictness flags.
- **`exports` maps both entries to `dist/`** with `types` conditions:
  `.` → `dist/index.js`, `./pglite` → `dist/index/store/pglite.js`.
  `files: ["dist"]` — the tarball is 89 files, ~58 kB packed.
- **PGlite/Drizzle become optional `peerDependencies`** (with
  `peerDependenciesMeta`), formalizing ADR-0018's "the consumer's own
  dependencies the moment they import the subpath": installs never pull
  them, but importing `rxjs-llm/pglite` without them fails loudly at the
  dynamic import with the package manager's own missing-peer diagnostics.
- **`prepublishOnly` runs typecheck + full suite + build** — nothing reaches
  the registry that the repo's own definition of done wouldn't accept.
- `sideEffects: false` (everything is cold and lazy by law — the bundler
  claim is the Module 1 contract restated), LICENSE file added (MIT).

## Consequences

- Runtime dependencies remain `rxjs` + `zod`; the core install stays two
  packages deep.
- Verified by a Node-ESM smoke test: both entries import under plain
  `node`, and a prompt-render + memory round-trip executes from `dist/`.
- The published artifact contains no TypeScript sources; consumers who want
  the tutorial read the repo, which is the point of the repo.
