import { Observable } from 'rxjs';
import { HttpError, LlmError, RateLimitError, TransportError } from '../errors.js';

export interface FetchStreamInit {
  /** Defaults to POST — every LLM API this package targets is POST-only. */
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  /** Injectable for tests. Defaults to `globalThis.fetch`. */
  fetchFn?: typeof fetch;
  /** Attached to every error this stream produces. */
  provider?: string;
}

/** Parses a Retry-After header: delta-seconds or an HTTP-date. */
export function parseRetryAfter(header: string | null): number | undefined {
  if (header === null || header.trim() === '') return undefined;
  const seconds = Number(header);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const date = Date.parse(header);
  if (Number.isNaN(date)) return undefined;
  return Math.max(0, date - Date.now());
}

const BODY_SNIPPET_LIMIT = 2000;

/**
 * `fetch` as a cold byte stream. Each subscribe issues one request; the
 * request starts at subscribe (lazy) and emission is always asynchronous
 * (no Zalgo). Teardown aborts the request; the resulting AbortError is
 * swallowed — cancellation never reaches the error channel (ADR-0005).
 */
export function fetchStream(url: string, init: FetchStreamInit = {}): Observable<Uint8Array> {
  return new Observable<Uint8Array>((subscriber) => {
    const controller = new AbortController();
    const { signal } = controller;
    const fetchFn = init.fetchFn ?? fetch;

    void (async () => {
      const requestInit: RequestInit = {
        method: init.method ?? 'POST',
        signal,
      };
      if (init.headers !== undefined) requestInit.headers = init.headers;
      if (init.body !== undefined) requestInit.body = init.body;

      const response = await fetchFn(url, requestInit);

      if (!response.ok) {
        const requestId =
          response.headers.get('request-id') ??
          response.headers.get('x-request-id') ??
          undefined;
        const bodyText = await response.text().catch(() => '');
        const body = bodyText.slice(0, BODY_SNIPPET_LIMIT);
        const errorOptions = {
          ...(init.provider !== undefined && { provider: init.provider }),
          ...(requestId !== undefined && { requestId }),
          body,
        };
        if (response.status === 429) {
          const retryAfterMs = parseRetryAfter(response.headers.get('retry-after'));
          throw new RateLimitError(`HTTP 429 from ${url}`, {
            ...errorOptions,
            ...(retryAfterMs !== undefined && { retryAfterMs }),
          });
        }
        throw new HttpError(`HTTP ${response.status} from ${url}`, response.status, errorOptions);
      }

      if (response.body === null) {
        subscriber.complete();
        return;
      }

      const reader = response.body.getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          subscriber.next(value);
        }
      } finally {
        reader.releaseLock();
      }
      subscriber.complete();
    })().catch((error: unknown) => {
      // Teardown already happened: the rejection is our own abort. Silence it.
      if (signal.aborted) return;
      if (error instanceof LlmError) {
        subscriber.error(error);
      } else {
        const message = error instanceof Error ? error.message : String(error);
        subscriber.error(
          new TransportError(`fetch failed for ${url}: ${message}`, {
            ...(init.provider !== undefined && { provider: init.provider }),
            cause: error,
          }),
        );
      }
    });

    return () => controller.abort();
  });
}
