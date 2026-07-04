import { reduce, tap, type OperatorFunction } from 'rxjs';
import type { StreamEvent } from '../types.js';
import type { EmitFn } from './stage.js';

/**
 * The D3.3 helper for streaming stages: forward every StreamEvent to the
 * run's progress$ while reducing the text deltas to the final string the
 * stage contributes to the context.
 *
 *   stage('answer', (ctx, emit) =>
 *     model.stream(...).pipe(collectText(emit), map(answer => ({ answer }))))
 */
export function collectText(emit: EmitFn): OperatorFunction<StreamEvent, string> {
  return (source) =>
    source.pipe(
      tap(emit),
      reduce((text, event) => (event.type === 'text_delta' ? text + event.text : text), ''),
    );
}
