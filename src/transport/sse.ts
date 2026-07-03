import { Observable, type OperatorFunction } from 'rxjs';

/** One dispatched server-sent event. `event` defaults to `'message'` per spec. */
export interface SseEvent {
  event: string;
  data: string;
  id?: string;
  retry?: number;
}

/**
 * WHATWG-conformant SSE parser: `Observable<Uint8Array>` → `Observable<SseEvent>`.
 *
 * The hard part is that chunk boundaries are arbitrary: frames split
 * mid-`data:`, mid-UTF-8-codepoint, or between the CR and LF of a CRLF pair.
 * `TextDecoder` in streaming mode handles codepoint splits; the line scanner
 * holds back a trailing CR until it can see whether an LF follows.
 *
 * Per spec: comment lines (`:`) are ignored, multiple `data:` lines join with
 * `\n`, an event with an empty data buffer is not dispatched, and an
 * incomplete event at end-of-stream (no terminating blank line) is discarded.
 *
 * All parser state lives inside the per-subscription closure, so the
 * resulting Observable stays safely cold.
 */
export function parseSse(): OperatorFunction<Uint8Array, SseEvent> {
  return (source) =>
    new Observable<SseEvent>((subscriber) => {
      const decoder = new TextDecoder('utf-8');
      let buffer = '';
      let dataLines: string[] = [];
      let eventType = '';
      let lastEventId: string | undefined;
      let retry: number | undefined;

      const dispatch = (): void => {
        if (dataLines.length === 0) {
          eventType = '';
          return;
        }
        const event: SseEvent = {
          event: eventType === '' ? 'message' : eventType,
          data: dataLines.join('\n'),
        };
        if (lastEventId !== undefined) event.id = lastEventId;
        if (retry !== undefined) event.retry = retry;
        subscriber.next(event);
        dataLines = [];
        eventType = '';
        retry = undefined;
      };

      const processLine = (line: string): void => {
        if (line === '') {
          dispatch();
          return;
        }
        if (line.startsWith(':')) return;

        const colon = line.indexOf(':');
        let field: string;
        let value: string;
        if (colon === -1) {
          field = line;
          value = '';
        } else {
          field = line.slice(0, colon);
          value = line.slice(colon + 1);
          if (value.startsWith(' ')) value = value.slice(1);
        }

        switch (field) {
          case 'data':
            dataLines.push(value);
            break;
          case 'event':
            eventType = value;
            break;
          case 'id':
            if (!value.includes('\0')) lastEventId = value;
            break;
          case 'retry': {
            const ms = Number(value);
            if (Number.isInteger(ms) && ms >= 0) retry = ms;
            break;
          }
          default:
            break; // unknown fields are ignored per spec
        }
      };

      const processBuffer = (atEnd: boolean): void => {
        let start = 0;
        let i = 0;
        while (i < buffer.length) {
          const ch = buffer.charCodeAt(i);
          if (ch === 0x0a /* \n */) {
            processLine(buffer.slice(start, i));
            i += 1;
            start = i;
          } else if (ch === 0x0d /* \r */) {
            if (i === buffer.length - 1 && !atEnd) break; // CR|LF may straddle chunks
            processLine(buffer.slice(start, i));
            i += buffer.charCodeAt(i + 1) === 0x0a ? 2 : 1;
            start = i;
          } else {
            i += 1;
          }
        }
        buffer = buffer.slice(start);
      };

      const subscription = source.subscribe({
        next: (chunk) => {
          buffer += decoder.decode(chunk, { stream: true });
          processBuffer(false);
        },
        error: (error: unknown) => subscriber.error(error),
        complete: () => {
          buffer += decoder.decode();
          processBuffer(true);
          if (buffer !== '') processLine(buffer);
          // pending, un-terminated event is discarded per spec — no dispatch()
          subscriber.complete();
        },
      });

      return () => subscription.unsubscribe();
    });
}
