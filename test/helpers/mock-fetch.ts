import { vi } from 'vitest';

const enc = new TextEncoder();

export interface RecordedCall {
  url: string;
  init: RequestInit;
  /** The request body parsed as JSON (LLM APIs are JSON-in). */
  bodyJson: () => unknown;
}

export interface MockFetchOptions {
  status?: number;
  headers?: Record<string, string>;
  /** Response body for non-200 responses. */
  errorBody?: string;
  /** Keep the stream open after the scripted chunks (for teardown tests). */
  hang?: boolean;
}

/**
 * A fetch double that streams scripted chunks and wires the request's
 * AbortSignal to the response stream, like a real fetch: aborting rejects
 * any pending read with an AbortError.
 */
export function mockFetch(chunks: string[], options: MockFetchOptions = {}) {
  const calls: RecordedCall[] = [];
  const fetchFn = vi.fn((url: string | URL | Request, init?: RequestInit) => {
    const recorded = init ?? {};
    calls.push({
      url: String(url),
      init: recorded,
      bodyJson: () => JSON.parse(String(recorded.body)),
    });
    const status = options.status ?? 200;
    if (status !== 200) {
      return Promise.resolve(
        new Response(options.errorBody ?? '', { status, headers: options.headers ?? {} }),
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
    return Promise.resolve(new Response(stream, { status: 200 })) as Promise<Response>;
  });
  return { fetchFn: fetchFn as unknown as typeof fetch, calls };
}

/** Renders `event:`/`data:` frames the way Anthropic and OpenAI send them. */
export function sseFrames(events: { event?: string; data: unknown }[]): string {
  return events
    .map((e) => {
      const data = typeof e.data === 'string' ? e.data : JSON.stringify(e.data);
      return (e.event !== undefined ? `event: ${e.event}\n` : '') + `data: ${data}\n\n`;
    })
    .join('');
}
