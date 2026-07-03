# ADR-0020: Memory is a reducer plus a view

**Status:** accepted · **Decision:** D5.1

## Context

LangChain ships a memory class per strategy (buffer, window, summary...),
each owning its own storage. But the strategies differ only in how history
is PROJECTED into messages — the accumulation is identical. Conflating
storage with projection is why LangChain memories can't be swapped
mid-conversation.

## Decision

One reducer, many views:

- **The reducer** is a `scan` over the turn stream
  (`(turns, turn) => [...turns, turn]`), materialized in a BehaviorSubject
  so `view()` is reactive: every `record()` pushes an updated projection
  to existing subscribers.
- **A view is a stream transform**:
  `MemoryView = (turns$: Observable<Turn[]>) => Observable<ChatMessage[]>`.
  Pure strategies (full, window, token-budget) are a single `map`; the
  summary strategy builds an async fold on the same input (ADR-0022).
  Strategies are swappable because they share the reducer — two memories
  restored from one snapshot with different views project the same truth
  differently (pinned by test).
- **The view pipeline is primed at creation** with an internal keep-alive
  subscription, so async views make progress even while nothing observes
  them. That requires a lifecycle end: `dispose()` completes the view and
  aborts any in-flight fold — deterministic teardown, per the repo ethos.
  Recording after dispose is ignored, not an error (a completed Subject).

The output shape is exactly what Module 2's history slot consumes:
`qa(vars).withHistory(await view())`.

## Consequences

- `view()` is `shareReplay(1)`-backed: late subscribers get the current
  projection, never a replay of intermediate states.
- The reducer state (`Turn[]`) is the single serialization boundary
  (ADR-0023); views never persist anything.
