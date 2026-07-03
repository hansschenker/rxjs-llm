# ADR-0009: Prompts are pure; format helpers are string transformers

**Status:** accepted · **Decision:** D2.3

## Context

LangChain's prompt layer entangles templating with runnables, callbacks,
and partial application machinery. The cost is that nothing in it can be
tested without the framework.

## Decision

Module 2 contains no I/O and no Observables. Every export is a pure
function:

- A template is `(vars) => string | AppliedMessages`. Applying it twice
  with the same vars yields equal results; there is no hidden state.
- `withHistory` returns a new array; the applied messages are never mutated.
- Format helpers are `(text: string) => string` transformers that append
  instructions. Composition is function nesting — no combinator framework.

Streams enter only when a chain stage (Module 3) feeds the result to a
`ChatModel`. This boundary is what makes the module trivially testable and
lawful: the property tests in `test/prompt/` need no scheduler, no mock
server, no teardown assertions.

### Deviation from the plan: `asJson` takes plain JSON Schema, not zod

The plan sketched `asJson(zodSchema)`. zod is sanctioned for Module 6 by a
future ADR, not for Module 2, and NON_GOALS forbids new runtime
dependencies without one. `asJson(schema: Record<string, unknown>)` keeps
this module dependency-free; zod users pass `zodToJsonSchema(z)` output
unchanged, and Module 6 can layer a zod-aware wrapper where zod is already
justified. The transformer carries `.schema` so downstream parsing
(Module 6's structured output) validates against exactly what was rendered
into the prompt.

## Consequences

- Module 2 adds zero runtime dependencies.
- Format helpers compose with both prompt forms and with plain strings —
  they know nothing about templates.
