# ADR-0007: Compile-time placeholder extraction

**Status:** accepted · **Decision:** D2.2

## Context

The module's headline feature: `promptTemplate('Summarize {doc} in {n}
bullets')` must return a function whose parameter type is exactly
`{ doc: string | number; n: string | number }`, with missing and extra keys
as compile errors.

## Decision

`ExtractVars<T>` recurses over the template literal type with one
non-negotiable ordering rule: **the `{{` escape is checked before the
variable branch**, so `{{foo}}` contributes no key. The scan is positional
(find the first `{`, then decide) — a naive "handle all `{{` first" type
would swallow variables that precede a later escape. An unclosed `{` is
literal tail: no key, mirrored at runtime.

Signature discipline (from review):

- `promptTemplate<T extends string>(template: T)` — the generic preserves
  the literal type without `as const`. A plain `string` parameter would
  silently degrade every template to `Record<string, string | number>`.
- Type tests use `expectTypeOf(...).toEqualTypeOf<...>()` — exact match
  only. `toMatchTypeOf` would accept the degraded type and never fail.
- Zero-placeholder templates build `() => string`, not
  `(vars: {}) => string`.

**Runtime/type symmetry is a contract:** `renderTemplate` and
`templateVars` implement exactly the grammar `ExtractVars` describes
(`{name}`, `{{`→`{`, `}}`→`}`, unclosed `{` literal, values never
re-scanned). The property test — random templates, interpolation then
extraction round-trips — pins the symmetry; `fast-check` joins as a
dev-only dependency (test-side, so outside the runtime-dependency ADR rule,
noted here anyway).

## Consequences

- Placeholder names may be any text not containing `}` — including unicode.
  The type level cannot practically restrict the character set, and runtime
  must match the type level exactly, so neither restricts it.
- All values render via `String()`: `string | number` uniformly (the plan's
  `{ doc: string }` vs `{ n: string | number }` distinction was not
  derivable from the template and is dropped).
