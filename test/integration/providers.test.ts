import { firstValueFrom, toArray } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { anthropic } from '../../src/adapters/anthropic';
import { ollama } from '../../src/adapters/ollama';
import { openai } from '../../src/adapters/openai';
import type { ChatMessage, ChatModel, StreamEvent } from '../../src/types';
import { startMockServer, type MockServer } from '../helpers/mock-server';

// Real HTTP round-trips through the genuine global fetch — no injected
// doubles. This is the "CI needs no API keys" guarantee from NON_GOALS.md.

let server: MockServer;

beforeAll(async () => {
  server = await startMockServer();
});

afterAll(async () => {
  await server.close();
});

const user = (content: string): ChatMessage => ({ role: 'user', content });

function models(): [string, () => ChatModel][] {
  return [
    [
      'anthropic',
      () => anthropic({ apiKey: 'test', model: 'mock-model', baseUrl: `${server.url}/anthropic` }),
    ],
    [
      'openai',
      () => openai({ apiKey: 'test', model: 'mock-model', baseUrl: `${server.url}/openai` }),
    ],
    ['ollama', () => ollama({ model: 'mock-model', baseUrl: `${server.url}/ollama` })],
  ];
}

describe('integration: three wire formats, one event taxonomy', () => {
  it.each(models())('%s streams normalized events over real HTTP', async (_name, make) => {
    const events = await firstValueFrom(make().stream([user('hello')]).pipe(toArray()));

    expect(events[0]).toEqual({ type: 'message_start', model: 'mock-model' });
    const text = events
      .filter((e): e is Extract<StreamEvent, { type: 'text_delta' }> => e.type === 'text_delta')
      .map((e) => e.text)
      .join('');
    expect(text).toBe('echo: hello');
    expect(events).toContainEqual({ type: 'usage', input: 5, output: 7 });
    expect(events.at(-1)).toEqual({ type: 'message_stop', stopReason: 'end_turn' });
  });

  it.each(models())('%s complete() folds the stream', async (_name, make) => {
    const completion = await firstValueFrom(make().complete([user('fold me')]));
    expect(completion.text).toBe('echo: fold me');
    expect(completion.usage).toEqual({ input: 5, output: 7 });
    expect(completion.stopReason).toBe('end_turn');
  });

  it('unsubscribing tears the request down across the real network stack', async () => {
    const model = anthropic({
      apiKey: 'test',
      model: 'mock-model',
      baseUrl: `${server.url}/anthropic`,
    });
    const received: StreamEvent[] = [];
    const errors: unknown[] = [];
    const subscription = model.stream([user('[slow] take your time')]).subscribe({
      next: (event) => received.push(event),
      error: (error) => errors.push(error),
    });

    // Wait for the first delta, then walk away mid-generation.
    await vi.waitFor(() => expect(received.some((e) => e.type === 'text_delta')).toBe(true));
    subscription.unsubscribe();

    // The server must observe the disconnect — teardown crossed the wire.
    await vi.waitFor(() => expect(server.aborted).toContain('/anthropic/v1/messages'));
    expect(errors).toHaveLength(0); // and the error channel stayed silent
  });
});
