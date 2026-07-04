import { Observable } from 'rxjs';
import { HttpError, LlmError, ParseError, RateLimitError, TransportError } from '../errors.js';
import { parseRetryAfter, type FetchStreamInit } from './fetch-stream.js';

const BODY_SNIPPET_LIMIT = 2000;

/**
 * `fetch` as a cold single-JSON-response Observable — the non-streaming
 * sibling of fetchStream, for APIs like embeddings that answer in one
 * body. Same laws (lazy, cold, unicast, teardown aborts, cancellation
 * silent) and the same error mapping as fetchStream.
 */
export function fetchJson(url: string, init: FetchStreamInit = {}): Observable<unknown> {
  return new Observable<unknown>((subscriber) => {
    const controller = new AbortController();
    const { signal } = controller;
    const fetchFn = init.fetchFn ?? fetch;

    void (async () => {
      const requestInit: RequestInit = { method: init.method ?? 'POST', signal };
      if (init.headers !== undefined) requestInit.headers = init.headers;
      if (init.body !== undefined) requestInit.body = init.body;

      const response = await fetchFn(url, requestInit);
      const requestId =
        response.headers.get('request-id') ?? response.headers.get('x-request-id') ?? undefined;
      const errorOptions = {
        ...(init.provider !== undefined && { provider: init.provider }),
        ...(requestId !== undefined && { requestId }),
      };

      if (!response.ok) {
        const body = (await response.text().catch(() => '')).slice(0, BODY_SNIPPET_LIMIT);
        if (response.status === 429) {
          const retryAfterMs = parseRetryAfter(response.headers.get('retry-after'));
          throw new RateLimitError(`HTTP 429 from ${url}`, {
            ...errorOptions,
            body,
            ...(retryAfterMs !== undefined && { retryAfterMs }),
          });
        }
        throw new HttpError(`HTTP ${response.status} from ${url}`, response.status, {
          ...errorOptions,
          body,
        });
      }

      const text = await response.text();
      let value: unknown;
      try {
        value = JSON.parse(text);
      } catch (cause) {
        throw new ParseError(`malformed JSON from ${url}: ${text.slice(0, 200)}`, {
          ...errorOptions,
          cause,
        });
      }
      subscriber.next(value);
      subscriber.complete();
    })().catch((error: unknown) => {
      if (signal.aborted) return; // our own teardown — silent (ADR-0005)
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
