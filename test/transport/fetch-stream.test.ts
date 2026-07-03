import { firstValueFrom, toArray } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { HttpError, isRetryable, RateLimitError, TransportError } from '../../src/errors';
import { fetchStream, parseRetryAfter } from '../../src/transport/fetch-stream';

const enc = new TextEncoder();

/** A 200 Response streaming the given chunks. Wires the request's AbortSignal
 * to the stream like a real fetch would, so aborts reject pending reads. */
function mockFetch(
  chunks: string[],
  options: { status?: number; headers?: Record<string, string>; body?: string; hang?: boolean } = {},
) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fetchFn = vi.fn((url: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(url), init: init ?? {} });
    const status = options.status ?? 200;
    if (status !== 200) {
      return Promise.resolve(
        new Response(options.body ?? '', { status, headers: options.headers ?? {} }),
      );
    }
    const signal = init?.signal;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(enc.encode(chunk));
        if (!options.hang) controller.close();
        signal?.addEventListener('abort', () => {
          try {
            controller.error(new DOMException('The operation was aborted.', 'AbortError'));
          } catch {
            /* already closed */
          }
        });
      },
    });
    return Promise.resolve(new Response(stream, { status: 200 }));
  });
  return { fetchFn: fetchFn as unknown as typeof fetch, calls };
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('fetchStream laws', () => {
  it('is lazy: no fetch before subscribe', async () => {
    const { fetchFn, calls } = mockFetch(['hi']);
    const stream$ = fetchStream('https://api.test/v1', { fetchFn });
    await tick();
    expect(calls).toHaveLength(0);
    await firstValueFrom(stream$.pipe(toArray()));
    expect(calls).toHaveLength(1);
  });

  it('is cold and unicast: each subscribe issues its own request', async () => {
    const { fetchFn, calls } = mockFetch(['hi']);
    const stream$ = fetchStream('https://api.test/v1', { fetchFn });
    await firstValueFrom(stream$.pipe(toArray()));
    await firstValueFrom(stream$.pipe(toArray()));
    expect(calls).toHaveLength(2);
  });

  it('emits nothing in the subscribe call frame (no Zalgo)', async () => {
    const { fetchFn } = mockFetch(['sync-enqueued']);
    const received: Uint8Array[] = [];
    let completed = false;
    fetchStream('https://api.test/v1', { fetchFn }).subscribe({
      next: (chunk) => received.push(chunk),
      complete: () => {
        completed = true;
      },
    });
    // Still inside the frame that called subscribe:
    expect(received).toHaveLength(0);
    expect(completed).toBe(false);
    await vi.waitFor(() => expect(completed).toBe(true));
    expect(received).toHaveLength(1);
  });

  it('streams the body chunks then completes', async () => {
    const { fetchFn } = mockFetch(['one', 'two']);
    const chunks = await firstValueFrom(
      fetchStream('https://api.test/v1', { fetchFn }).pipe(toArray()),
    );
    expect(chunks.map((c) => new TextDecoder().decode(c))).toEqual(['one', 'two']);
  });

  it('teardown aborts the request signal, silently (ADR-0005)', async () => {
    const { fetchFn, calls } = mockFetch(['first'], { hang: true });
    const errors: unknown[] = [];
    const received: Uint8Array[] = [];
    const subscription = fetchStream('https://api.test/v1', { fetchFn }).subscribe({
      next: (chunk) => received.push(chunk),
      error: (error) => errors.push(error),
    });
    await vi.waitFor(() => expect(received).toHaveLength(1));

    subscription.unsubscribe();
    expect(calls[0]?.init.signal?.aborted).toBe(true);

    await tick();
    await tick();
    expect(errors).toHaveLength(0); // cancellation never reaches the error channel
  });

  it('sends method, headers and body through to fetch', async () => {
    const { fetchFn, calls } = mockFetch(['ok']);
    await firstValueFrom(
      fetchStream('https://api.test/v1', {
        fetchFn,
        headers: { 'x-api-key': 'k' },
        body: '{"stream":true}',
      }).pipe(toArray()),
    );
    expect(calls[0]?.init.method).toBe('POST');
    expect(calls[0]?.init.headers).toEqual({ 'x-api-key': 'k' });
    expect(calls[0]?.init.body).toBe('{"stream":true}');
  });
});

describe('fetchStream error mapping', () => {
  it('maps 5xx to a retryable HttpError with body and requestId', async () => {
    const { fetchFn } = mockFetch([], {
      status: 503,
      body: '{"error":"overloaded"}',
      headers: { 'request-id': 'req_9' },
    });
    const error = await firstValueFrom(
      fetchStream('https://api.test/v1', { fetchFn, provider: 'anthropic' }),
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(HttpError);
    const httpError = error as HttpError;
    expect(httpError.status).toBe(503);
    expect(httpError.body).toBe('{"error":"overloaded"}');
    expect(httpError.provider).toBe('anthropic');
    expect(httpError.requestId).toBe('req_9');
    expect(isRetryable(httpError)).toBe(true);
  });

  it('maps 429 to RateLimitError carrying retryAfterMs', async () => {
    const { fetchFn } = mockFetch([], { status: 429, headers: { 'retry-after': '3' } });
    const error = await firstValueFrom(fetchStream('https://api.test/v1', { fetchFn })).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(RateLimitError);
    expect((error as RateLimitError).retryAfterMs).toBe(3000);
  });

  it('maps 4xx to a non-retryable HttpError', async () => {
    const { fetchFn } = mockFetch([], { status: 400, body: 'bad request' });
    const error = await firstValueFrom(fetchStream('https://api.test/v1', { fetchFn })).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(HttpError);
    expect(isRetryable(error)).toBe(false);
  });

  it('maps network failure to TransportError preserving the cause', async () => {
    const cause = new TypeError('fetch failed');
    const fetchFn = (() => Promise.reject(cause)) as unknown as typeof fetch;
    const error = await firstValueFrom(fetchStream('https://api.test/v1', { fetchFn })).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(TransportError);
    expect((error as TransportError).cause).toBe(cause);
  });
});

describe('parseRetryAfter', () => {
  it('parses delta-seconds to milliseconds', () => {
    expect(parseRetryAfter('2')).toBe(2000);
    expect(parseRetryAfter('0')).toBe(0);
  });

  it('parses an HTTP-date relative to now', () => {
    const inFive = new Date(Date.now() + 5000).toUTCString();
    const ms = parseRetryAfter(inFive)!;
    expect(ms).toBeGreaterThan(3000);
    expect(ms).toBeLessThanOrEqual(5000);
  });

  it('returns undefined for absent or garbage headers', () => {
    expect(parseRetryAfter(null)).toBeUndefined();
    expect(parseRetryAfter('soon')).toBeUndefined();
  });
});
