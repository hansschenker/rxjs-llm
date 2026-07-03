# ADR-0025: The agent loop is expand(); exceeding the budget is an answer

**Status:** accepted · **Decision:** D6.1

## Context

An agent is a recursion: model turn → maybe tools → model turn… LangChain
builds this as an AgentExecutor class with pluggable "agent types" and
ReAct prompt scaffolding. Providers now reason natively over tool-use
events; the scaffolding is legacy, and the recursion is exactly what
RxJS's `expand()` is for.

## Decision

State is `{ messages, iteration, usage, outcome? }`. Each expansion:

1. Stream one model turn — deltas tap to progress$, then reduce via
   `collectCompletion()` (semantically Module 1's `complete()`; streaming
   first is what makes D6.4's delta interleaving possible).
2. No tool calls → terminate with a `complete` outcome carrying text,
   final transcript, iteration count, and summed usage.
3. Tool calls → execute them under `mergeMap`'s concurrency cap (parallel
   tool calls are real), append the assistant message + tool results
   **in call order** regardless of completion order (deterministic
   transcripts), recurse.

The plan's `pendingToolCalls` state field proved unnecessary: tools
execute within the same expansion step, so no pending state ever crosses
an expansion boundary.

**Max iterations is a result variant, not an error.**
`{ type: 'max_iterations', messages, iterations, usage }` — the transcript
is intact and the caller decides what the budget exhaustion means.
Errors on `result$` are reserved for real failures (transport, provider).
A budget is policy; policy outcomes are data.

## Consequences

- `expand(step, 1)` — the recursion is linear; the explicit concurrency
  of 1 documents it.
- The final emitted state always carries the outcome (`last()` + map),
  including at `maxIterations: 0`.
- Iteration = completed model calls, 1-based in progress events.
