# ADR-0012: Tracing as an operator, not a framework

**Status:** accepted · **Decision:** D3.4

## Context

This is where LangSmith lives in LangChain: a callback manager threaded
through every runnable, with its own event taxonomy, batching, and vendor
coupling. The observability seam is worth keeping; the framework is not.

## Decision

`traced(stage, { sink, runId, now })` is a `defer` + `tap` operator emitting
three event types — `stage_start`, `stage_complete`, `stage_error` — with a
correlation `runId` stable across every stage of one `run()`, wall-clock
`at`, and `elapsedMs` on completion/error. The `defer` is load-bearing:
start is stamped at *subscription*, so timings measure execution, not
pipeline assembly.

- **Sinks are pluggable and dumb:** `TraceSink` is one method. Provided:
  `consoleSink` (dev) and `collectorSink()` (tests assert on its array).
  An OpenTelemetry adapter is a sink implementation outside this repo.
- **Injection is via chain options** (`chain({ trace, now?, runId? })`),
  and every `stage()` applies `traced()` automatically by reading the trace
  context off the hidden context symbol — stage authors write nothing.
  `now` and `runId` are injectable for deterministic tests; defaults are
  `Date.now` and a process-wide counter.
- **No backpressure, by design:** sinks are called synchronously and must
  not throw. Tracing is observation, never flow control — the same stance
  progress$ takes on unobserved events (ADR-0006 §3).

## Consequences

- A traced chain and an untraced chain differ by one option; stages are
  byte-identical in both.
- Trace events and progress$ events are separate channels with separate
  jobs: progress$ carries *model* events for UIs; trace carries *stage
  lifecycle* for operators. Conflating them would force every UI to filter
  ops noise and every ops sink to buffer token deltas.
