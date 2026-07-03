# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

`rxjs-llm` is a reference implementation of LLM orchestration primitives built directly on RxJS — "LangChain in ~2,500 lines" — and doubles as a book artifact. The full design lives in `rxjs-llm-module-plans.md`; read the relevant module section there before implementing anything. The repo is currently pre-scaffold: no code exists yet.

Six modules, all in this one repo:

1. **Uniform Model Interface** — `ChatModel` with `stream()`/`complete()`, transport layer (fetch → SSE/NDJSON framing), provider adapters (Anthropic, OpenAI, Ollama), normalized `StreamEvent` union, error taxonomy, resilience operators (`retryWithBackoff`, `streamTimeout`, `rateLimit`).
2. **Prompts** — pure, synchronous typed prompt templates; placeholder names extracted via template-literal types so missing/extra variables are compile errors.
3. **Chains** — `stage()` returns an `OperatorFunction`; chains ARE pipes. Typed accumulating context (each stage merges a patch, widening the type). `{ result$, progress$ }` split for UI streaming; `traced()` tap-based tracing seam.
4. **Indexes/RAG** — loaders as `Observable<Doc>`, pure text splitter + operator wrapper, `Embedder` mirroring the ChatModel pattern, `VectorStore` interface (in-memory + PGlite/Drizzle), `retrieveContext` operator.
5. **Memory** — reducer (`scan` over turns) + swappable views (full, window, token-budget, async summary fold). ~150 lines target.
6. **Agents** — tool loop via `expand()`, Zod-defined tools, safety rails (max iterations as a result variant not an error, per-tool timeout, full cancellation matrix).

## Tooling and commands

Bun + Vitest + `rxjs@7.8` + strict TypeScript, ESM only.

- Install: `bun install`
- All tests: `bun run test` (Vitest)
- Single test file: `bunx vitest run test/<path>.test.ts`
- Type-level tests use Vitest's `expectTypeOf` and `@ts-expect-error`; they live in `*.types.test.ts` files and are part of the suite.

Integration tests run against a mock provider server (plain `Bun.serve`) — CI must never need API keys.

## Non-negotiable contracts ("law tests")

These are enforced by tests, not just convention:

- Every Observable-returning API is **cold, lazy, unicast, teardown-complete**. Each subscribe = one HTTP request; no fetch before subscribe; unsubscribe aborts the underlying work (asserted via AbortSignal). No Zalgo: nothing emits synchronously in the subscribe call frame.
- **Cancellation is silent teardown** — it must never surface on the error channel. `AbortError` is not an error.
- Error taxonomy: `LlmError` base carrying provider + requestId; subtypes `TransportError` (retryable), `HttpError` (429/5xx retryable, other 4xx not; `RateLimitError` carries `retryAfterMs`), `ParseError` (not retryable), `ProviderError` (per provider code). An `isRetryable` predicate drives the retry operator.
- Marble tests via injected `TestScheduler` for every custom operator; property tests (fast-check) for anything claiming algebraic structure (splitter invariants, memory snapshot/restore round-trip, agent loop state).

## Workflow and governance

- **Each phase = one commit** — the git history is meant to read as a tutorial. Follow the phase breakdown in the module plan.
- Keep `STATUS.md` current; file an ADR in `decisions/` for every design decision marked **D-n** in the plan; respect `NON_GOALS.md`.
- **No new runtime dependencies without an ADR** justifying them. Known-sanctioned ones: `zod` + `zod-to-json-schema` (Module 6), optional `tiktoken` behind a dynamic import (Module 4).
- After each module: update `STATUS.md`, file the module's ADRs, tag `v0.N.0`.
- Session pattern: one module per session. Start by reading `STATUS.md`, `NON_GOALS.md`, and `decisions/`, then implement the module per its plan section. Modules 2 and 5 are small enough to pair; Module 4 may split at the phase 3/4 boundary.

## Architecture notes that span modules

- The `StreamEvent` discriminated union (`message_start`, `text_delta`, `tool_call_delta`, `usage`, `message_stop`) is the normalization boundary — all provider adapters emit it, and Modules 3–6 consume only it. Tool-call events are in the taxonomy from day one because Anthropic and OpenAI encode tool deltas incompatibly.
- Two distinct timeouts in Module 1: first-byte (connection up, no data) vs. inter-chunk idle (stall mid-generation) — different failure modes, different retryability.
- The transport layer supports two framing strategies: SSE (Anthropic/OpenAI) and NDJSON (Ollama) — this split validates the layering.
- Module 2 prompts are pure functions (no I/O, no Observables); streams enter only when a chain feeds a prompt result to a `ChatModel`. The message-prompt form has a history slot where Module 5 memory splices in.
- The `{ result$, progress$ }` pattern (D3.3) is shared by chains and agents: `progress$` carries tagged stream/lifecycle events for UI, `result$` the final value. An agent is just a chain stage from the outside.
- Adversarial SSE chunk-split fixtures (frames split mid-`data:`, mid-UTF-8-codepoint, CRLF vs LF, multi-line `data:`) are the transport parser's main test set.
- The Module 6 capstone test — retrieve → agent with tools → memory record, composed as a chain against the mock server — is the repo's definition of done.
