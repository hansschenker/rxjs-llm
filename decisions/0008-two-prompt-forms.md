# ADR-0008: Two prompt forms, two exports, and the history slot

**Status:** accepted · **Decision:** D2.1 (+ review notes 3 and 4)

## Context

LangChain conflates single-turn string prompts with full message-list
prompts; they compose differently and deserve distinct types. Separately,
Module 5's memory needs a defined place to splice conversation history into
a message prompt — and Module 3's plan examples already assume the shape
`qa(vars).withHistory(history)`.

## Decision

### Two forms

- **String prompt** (`promptTemplate` / tagged `prompt`): `(vars) => string`
  — a single user turn, composable as plain text.
- **Message prompt** (`messagePrompt`): `(vars) => AppliedMessages` — a full
  `ChatMessage[]` assembled from optional system turn, optional few-shot
  pairs, the history slot, and the final user turn. Placeholders are
  compile-checked across system + user templates (union). Few-shot examples
  are deliberately NOT templated: examples are static by nature, and typing
  them would put every example's text into the type system for no benefit.

### Two exports, never one overload

The parsed form (`promptTemplate`) and the tagged form (`prompt`) are
separate functions. A shared overload was rejected on precedent: the
`streamTimeout` overload incident (Phase 5) showed how quickly TS overload
resolution degrades around generic conditional returns. The two forms also
have different escape semantics — parsed needs `{{`/`}}`, tagged has no
escapes at all — which a single name would blur.

### The history slot is `withHistory()` on the applied prompt

`messagePrompt(...)(vars)` returns `AppliedMessages`: a **real**
`ChatMessage[]` (directly usable by `ChatModel.stream`) carrying one
non-enumerable method. `withHistory(history)` returns a NEW plain array
with the history spliced **between the few-shot block and the final user
turn** — few-shot examples stay pinned to the system prompt, and the actual
question is always last, which is what providers' caching and attention
patterns want. The method is pure and the applied array is never mutated.

Non-enumerable matters: iteration, spread, `Object.keys`, and
`JSON.stringify` all see a plain 4-message array; only an explicit
`.withHistory(...)` call sees the method.

## Consequences

- Module 5's memory does `qa(vars).withHistory(await view())` with no
  adapter changes — history is ordinary messages by the time a model sees it.
- A host that never uses memory pays nothing: the applied array IS the
  message list.
