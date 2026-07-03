import { defer, tap, type MonoTypeOperatorFunction } from 'rxjs';

/**
 * Tracing as an operator, not a framework (decision D3.4, ADR-0012).
 * `traced()` taps stage lifecycle into a pluggable sink; every stage
 * applies it automatically when the chain was built with a `trace` option.
 * This is the LangSmith-replacement seam: an OpenTelemetry adapter is just
 * another TraceSink implementation, outside this repo.
 */

export interface TraceEvent {
  type: 'stage_start' | 'stage_complete' | 'stage_error';
  stage: string;
  /** Correlation id — stable across every stage of one run(). */
  runId: string;
  /** Sink-clock timestamp (ms). */
  at: number;
  /** Present on stage_complete and stage_error. */
  elapsedMs?: number;
  /** Present on stage_error. */
  message?: string;
}

export interface TraceSink {
  event(event: TraceEvent): void;
}

/** Everything traced() needs; the chain builds this per run. */
export interface TraceContext {
  sink: TraceSink;
  runId: string;
  now: () => number;
}

export const consoleSink: TraceSink = {
  event: (e) => {
    const timing = e.elapsedMs !== undefined ? ` (${e.elapsedMs}ms)` : '';
    const detail = e.message !== undefined ? ` — ${e.message}` : '';
    console.log(`[${e.runId}] ${e.stage}: ${e.type}${timing}${detail}`);
  },
};

export interface CollectorSink extends TraceSink {
  events: TraceEvent[];
}

/** Test sink: records everything, in order. */
export function collectorSink(): CollectorSink {
  const events: TraceEvent[] = [];
  return { events, event: (e) => events.push(e) };
}

/**
 * Wrap a source with start/complete/error trace events. The defer matters:
 * `stage_start` is stamped at SUBSCRIPTION, not at pipeline assembly, so
 * timings measure execution. Sinks are called synchronously and must not
 * throw; there is deliberately no backpressure — tracing is observation,
 * never flow control.
 */
export function traced<T>(stage: string, trace: TraceContext): MonoTypeOperatorFunction<T> {
  return (source) =>
    defer(() => {
      const startedAt = trace.now();
      trace.sink.event({ type: 'stage_start', stage, runId: trace.runId, at: startedAt });
      return source.pipe(
        tap({
          complete: () => {
            const at = trace.now();
            trace.sink.event({
              type: 'stage_complete',
              stage,
              runId: trace.runId,
              at,
              elapsedMs: at - startedAt,
            });
          },
          error: (error: unknown) => {
            const at = trace.now();
            trace.sink.event({
              type: 'stage_error',
              stage,
              runId: trace.runId,
              at,
              elapsedMs: at - startedAt,
              message: error instanceof Error ? error.message : String(error),
            });
          },
        }),
      );
    });
}
