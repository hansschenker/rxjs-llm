import { Observable, type OperatorFunction } from 'rxjs';
import { ParseError } from '../errors';

/**
 * NDJSON framing: `Observable<Uint8Array>` → one parsed JSON value per line.
 * Ollama streams this instead of SSE — the transport layer's second framing
 * strategy, and the proof that framing belongs below the adapters.
 *
 * Chunk boundaries are arbitrary here too: lines split across chunks are
 * buffered, and `TextDecoder` in streaming mode handles split codepoints.
 * A trailing line without a newline is parsed at end-of-stream (unlike SSE,
 * NDJSON has no event-boundary semantics to protect).
 */
export function parseNdjson(): OperatorFunction<Uint8Array, unknown> {
  return (source) =>
    new Observable<unknown>((subscriber) => {
      const decoder = new TextDecoder('utf-8');
      let buffer = '';

      const processLine = (line: string): void => {
        const trimmed = line.trim();
        if (trimmed === '') return;
        let value: unknown;
        try {
          value = JSON.parse(trimmed);
        } catch (cause) {
          subscriber.error(
            new ParseError(`malformed NDJSON line: ${trimmed.slice(0, 200)}`, { cause }),
          );
          return;
        }
        subscriber.next(value);
      };

      const subscription = source.subscribe({
        next: (chunk) => {
          buffer += decoder.decode(chunk, { stream: true });
          let newline = buffer.indexOf('\n');
          while (newline !== -1) {
            processLine(buffer.slice(0, newline));
            buffer = buffer.slice(newline + 1);
            newline = buffer.indexOf('\n');
          }
        },
        error: (error: unknown) => subscriber.error(error),
        complete: () => {
          buffer += decoder.decode();
          processLine(buffer);
          subscriber.complete();
        },
      });

      return () => subscription.unsubscribe();
    });
}
