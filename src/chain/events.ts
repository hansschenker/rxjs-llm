import type { StreamEvent } from '../types';

/**
 * The progress$ channel's event taxonomy (decision D3.3, ADR-0006).
 *
 * `stage_event` forwards a model StreamEvent tagged with the emitting stage.
 * Every run that terminates on its own ends with exactly one terminal event
 * — `run_complete` or `run_failed` — followed by completion. `run_failed`
 * carries only the message: the error *object* travels on result$ alone, so
 * one failure never fires two error handlers. Cancellation emits no terminal
 * event at all (silent teardown, ADR-0005).
 */
export type ChainEvent =
  | { type: 'stage_event'; stage: string; event: StreamEvent }
  | { type: 'run_complete' }
  | { type: 'run_failed'; message: string };
