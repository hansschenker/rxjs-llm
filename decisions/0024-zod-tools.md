# ADR-0024: Tools are Zod-defined; zod is the one new dependency

**Status:** accepted · **Decision:** D6.2

## Context

A tool needs three things from one definition: a provider-facing JSON
Schema, runtime validation of model-produced arguments, and a typed
`execute` handler. Hand-writing JSON Schema next to a TypeScript type
next to a validator means three sources of truth that drift.

## Decision

`tool({ name, description, input: zodSchema, execute, timeoutMs?, retries? })`.
The zod schema is the single source of truth:

- **JSON Schema** derives via zod v4's native `z.toJSONSchema()` — the
  plan budgeted for zod + zod-to-json-schema, but v4 absorbed the
  conversion, so the budget is spent on ONE dependency (`zod@^4`,
  runtime, zero transitive deps). `$schema` is stripped: noise to an LLM
  API.
- **Runtime validation** via `safeParse` before execute — and this is the
  key robustness trick: invalid arguments produce a tool-result message
  RETURNED TO THE MODEL (`Error: invalid arguments for 'x': city: …`),
  never a thrown error. The model reads the issue list and self-corrects
  on the next iteration. Same treatment for malformed JSON args and
  unknown tool names (which list the available tools).
- **Types** flow from the same schema: `execute` receives `z.infer<S>`.
  The registry-facing `Tool` type takes `unknown` and the factory casts —
  sound because the loop only calls execute with schema-parsed data.

Empty argument strings parse as `{}` (providers send `""` for no-arg
tools). Duplicate registry names throw at construction — a config bug,
not a runtime condition.

## Consequences

- The core's dependency list grows to `rxjs` + `zod`. NON_GOALS'
  pre-approval anticipated this module; the ADR is the required paper.
- Consumers on zod v3 cannot pass their schemas directly (v4 API);
  acceptable for a reference implementation tracking current zod.
