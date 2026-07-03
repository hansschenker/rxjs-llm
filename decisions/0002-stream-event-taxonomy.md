# ADR-0002: Normalized StreamEvent taxonomy

**Status:** accepted · **Decision:** D2

## Context

Every provider streams a different wire format (Anthropic SSE event types,
OpenAI chat-completion chunks with a `[DONE]` sentinel, Ollama NDJSON).
Modules 3–6 need one shape to consume.

## Decision

All adapters emit a discriminated union:

```ts
type StreamEvent =
  | { type: 'message_start'; model: string }
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_delta'; id: string; name?: string; argsDelta: string }
  | { type: 'usage'; input: number; output: number }
  | { type: 'message_stop'; stopReason: StopReason };
```

- **Tool-call events are included from day one** even though agents are
  Module 6: Anthropic and OpenAI encode tool deltas completely differently
  (content blocks vs. indexed delta fragments), and retrofitting a common
  shape later would ripple through every consumer.
- **`thinking_delta` is included** (the plan's open question, answered yes):
  Anthropic extended thinking streams are real today, the variant costs one
  union member, and `rxjs-full` integration wants to render thinking
  distinctly from answer text.
- `tool_call_delta.argsDelta` is a raw JSON *fragment*; assembly into a
  parseable string is the consumer's fold (see `foldEvents`).

## Consequences

Adapters own all provider-specific statefulness (e.g. mapping Anthropic block
indexes to tool-call ids, OpenAI tool-call index bookkeeping). Consumers see
only this union.
