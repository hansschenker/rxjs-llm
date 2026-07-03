import { map, retry } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { anthropic } from '../../src/adapters/anthropic';
import { chain } from '../../src/chain/chain';
import type { ChainEvent } from '../../src/chain/events';
import { collectText } from '../../src/chain/collect-text';
import { stage } from '../../src/chain/stage';
import { HttpError } from '../../src/errors';
import { startMockServer, type MockServer } from '../helpers/mock-server';

// THE pinning test (D3.3's counterpart of Module 1's AbortSignal teardown
// test): a real chain, a real adapter, real HTTP against the mock server's
// request counter. Fresh server per test so counters are per-scenario.

let server: MockServer;

beforeEach(async () => {
  server = await startMockServer();
});

afterEach(async () => {
  await server.close();
});

function answerChain(baseUrl: string) {
  const model = anthropic({ apiKey: 'test', model: 'mock-model', baseUrl });
  return chain<{ question: string }>().pipe(
    stage('answer', (ctx, emit) =>
      model
        .stream([{ role: 'user', content: ctx.question }])
        .pipe(collectText(emit), map((answer) => ({ answer }))),
    ),
  );
}

const messagesUrl = '/anthropic/v1/messages';
const count = (url: string) => server.requests.filter((u) => u === url).length;

describe('D3.3 pinning test', () => {
  it('staggered double subscription: one set of provider requests, both get the final context, one execution on progress$', async () => {
    const { result$, progress$ } = answerChain(`${server.url}/anthropic`).run({
      question: 'hello',
    });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));

    const finals: unknown[] = [];
    result$.subscribe((v) => finals.push(v));
    // second subscriber joins mid-flight: after the request went out, before completion
    await vi.waitFor(() => expect(count(messagesUrl)).toBe(1));
    result$.subscribe((v) => finals.push(v));
    await vi.waitFor(() => expect(finals).toHaveLength(2));

    expect(count(messagesUrl)).toBe(1); // exactly one set of provider requests
    expect(finals[0]).toEqual({ question: 'hello', answer: 'echo: hello' });
    expect(finals[1]).toEqual(finals[0]); // both subscribers got the final context

    // progress$ carried events from exactly one execution, one terminal event
    const terminals = events.filter((e) => e.type === 'run_complete' || e.type === 'run_failed');
    expect(terminals).toEqual([{ type: 'run_complete' }]);
    const streamed = events
      .filter((e): e is Extract<ChainEvent, { type: 'stage_event' }> => e.type === 'stage_event')
      .filter((e) => e.event.type === 'message_start');
    expect(streamed).toHaveLength(1); // one message_start = one model call observed
  });

  it('latch assertion: after a failed run, resubscription and retry() surface the same error without moving the request counter', async () => {
    // /missing/* is unrouted → 404 → non-retryable HttpError from the adapter stack
    const failingUrl = '/missing/v1/messages';
    const { result$ } = answerChain(`${server.url}/missing`).run({ question: 'boom' });

    const first = await new Promise((resolve) => result$.subscribe({ error: resolve }));
    expect(first).toBeInstanceOf(HttpError);
    expect(count(failingUrl)).toBe(1);

    const second = await new Promise((resolve) => result$.subscribe({ error: resolve }));
    const retried = await new Promise((resolve) =>
      result$.pipe(retry(1)).subscribe({ error: resolve }),
    );

    expect(second).toBe(first); // the same latched error object
    expect(retried).toBe(first);
    expect(count(failingUrl)).toBe(1); // the counter has not moved
  });

  it('cancellation crosses the wire: unsubscribing the chain aborts the provider request, both channels stay silent', async () => {
    const { result$, progress$ } = answerChain(`${server.url}/anthropic`).run({
      question: '[slow] take your time',
    });
    const events: ChainEvent[] = [];
    let progressCompleted = false;
    let anyError = false;
    progress$.subscribe({
      next: (e) => events.push(e),
      complete: () => (progressCompleted = true),
      error: () => (anyError = true),
    });
    const subscription = result$.subscribe({ error: () => (anyError = true) });

    await vi.waitFor(() => expect(events.length).toBeGreaterThan(0)); // stream is flowing
    subscription.unsubscribe();

    await vi.waitFor(() => expect(server.aborted).toContain(messagesUrl));
    expect(progressCompleted).toBe(true);
    expect(events.some((e) => e.type === 'run_complete' || e.type === 'run_failed')).toBe(false);
    expect(anyError).toBe(false); // cancellation is silent on both channels
  });
});
