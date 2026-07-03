import { firstValueFrom, toArray } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { anthropic } from '../../src/adapters/anthropic';
import { isRetryable, ProviderError } from '../../src/errors';
import type { ChatMessage, StreamEvent } from '../../src/types';
import { mockFetch, sseFrames } from '../helpers/mock-fetch';

const user = (content: string): ChatMessage => ({ role: 'user', content });

function model(fetchFn: typeof fetch) {
  return anthropic({ apiKey: 'sk-test', model: 'claude-fable-5', fetchFn });
}

const textFixture = sseFrames([
  {
    event: 'message_start',
    data: {
      type: 'message_start',
      message: { model: 'claude-fable-5', usage: { input_tokens: 12 } },
    },
  },
  {
    event: 'content_block_start',
    data: { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
  },
  { event: 'ping', data: { type: 'ping' } },
  {
    event: 'content_block_delta',
    data: { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Hello' } },
  },
  {
    event: 'content_block_delta',
    data: { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: ' world' } },
  },
  { event: 'content_block_stop', data: { type: 'content_block_stop', index: 0 } },
  {
    event: 'message_delta',
    data: {
      type: 'message_delta',
      delta: { stop_reason: 'end_turn' },
      usage: { output_tokens: 5 },
    },
  },
  { event: 'message_stop', data: { type: 'message_stop' } },
]);

const toolFixture = sseFrames([
  {
    event: 'message_start',
    data: {
      type: 'message_start',
      message: { model: 'claude-fable-5', usage: { input_tokens: 30 } },
    },
  },
  {
    event: 'content_block_start',
    data: {
      type: 'content_block_start',
      index: 0,
      content_block: { type: 'thinking', thinking: '' },
    },
  },
  {
    event: 'content_block_delta',
    data: {
      type: 'content_block_delta',
      index: 0,
      delta: { type: 'thinking_delta', thinking: 'Needs the weather tool.' },
    },
  },
  { event: 'content_block_stop', data: { type: 'content_block_stop', index: 0 } },
  {
    event: 'content_block_start',
    data: {
      type: 'content_block_start',
      index: 1,
      content_block: { type: 'tool_use', id: 'toolu_1', name: 'get_weather' },
    },
  },
  {
    event: 'content_block_delta',
    data: {
      type: 'content_block_delta',
      index: 1,
      delta: { type: 'input_json_delta', partial_json: '{"city":' },
    },
  },
  {
    event: 'content_block_delta',
    data: {
      type: 'content_block_delta',
      index: 1,
      delta: { type: 'input_json_delta', partial_json: '"Rapperswil"}' },
    },
  },
  { event: 'content_block_stop', data: { type: 'content_block_stop', index: 1 } },
  {
    event: 'message_delta',
    data: {
      type: 'message_delta',
      delta: { stop_reason: 'tool_use' },
      usage: { output_tokens: 20 },
    },
  },
  { event: 'message_stop', data: { type: 'message_stop' } },
]);

describe('anthropic adapter — event mapping', () => {
  it('maps a text stream to the normalized taxonomy', async () => {
    const { fetchFn } = mockFetch([textFixture]);
    const events = await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(events).toEqual<StreamEvent[]>([
      { type: 'message_start', model: 'claude-fable-5' },
      { type: 'text_delta', text: 'Hello' },
      { type: 'text_delta', text: ' world' },
      { type: 'usage', input: 12, output: 5 },
      { type: 'message_stop', stopReason: 'end_turn' },
    ]);
  });

  it('maps thinking blocks and re-attaches tool arg fragments to their call id', async () => {
    const { fetchFn } = mockFetch([toolFixture]);
    const events = await firstValueFrom(model(fetchFn).stream([user('umbrella?')]).pipe(toArray()));
    expect(events).toEqual<StreamEvent[]>([
      { type: 'message_start', model: 'claude-fable-5' },
      { type: 'thinking_delta', text: 'Needs the weather tool.' },
      { type: 'tool_call_delta', id: 'toolu_1', name: 'get_weather', argsDelta: '' },
      { type: 'tool_call_delta', id: 'toolu_1', argsDelta: '{"city":' },
      { type: 'tool_call_delta', id: 'toolu_1', argsDelta: '"Rapperswil"}' },
      { type: 'usage', input: 30, output: 20 },
      { type: 'message_stop', stopReason: 'tool_use' },
    ]);
  });

  it('complete() folds the stream into a ChatCompletion', async () => {
    const { fetchFn } = mockFetch([toolFixture]);
    const completion = await firstValueFrom(model(fetchFn).complete([user('umbrella?')]));
    expect(completion.thinking).toBe('Needs the weather tool.');
    expect(completion.toolCalls).toEqual([
      { id: 'toolu_1', name: 'get_weather', args: '{"city":"Rapperswil"}' },
    ]);
    expect(completion.stopReason).toBe('tool_use');
    expect(completion.usage).toEqual({ input: 30, output: 20 });
  });

  it('surfaces in-stream error events as ProviderError with per-code retryability', async () => {
    const { fetchFn } = mockFetch([
      sseFrames([
        {
          event: 'error',
          data: { type: 'error', error: { type: 'overloaded_error', message: 'Overloaded' } },
        },
      ]),
    ]);
    const error = await firstValueFrom(model(fetchFn).stream([user('hi')])).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ProviderError);
    const providerError = error as ProviderError;
    expect(providerError.code).toBe('overloaded_error');
    expect(providerError.provider).toBe('anthropic');
    expect(isRetryable(providerError)).toBe(true);
  });
});

describe('anthropic adapter — request shape', () => {
  it('targets /v1/messages with auth and version headers', async () => {
    const { fetchFn, calls } = mockFetch([textFixture]);
    await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(calls[0]?.url).toBe('https://api.anthropic.com/v1/messages');
    expect(calls[0]?.init.headers).toMatchObject({
      'x-api-key': 'sk-test',
      'anthropic-version': '2023-06-01',
    });
  });

  it('hoists system messages, maps tool results and assistant tool calls', async () => {
    const { fetchFn, calls } = mockFetch([textFixture]);
    const messages: ChatMessage[] = [
      { role: 'system', content: 'Be terse.' },
      user('weather?'),
      {
        role: 'assistant',
        content: '',
        toolCalls: [{ id: 'toolu_1', name: 'get_weather', args: '{"city":"Rapperswil"}' }],
      },
      { role: 'tool', content: 'rainy, 14°C', toolCallId: 'toolu_1' },
    ];
    await firstValueFrom(
      model(fetchFn)
        .stream(messages, {
          temperature: 0.2,
          tools: [{ name: 'get_weather', inputSchema: { type: 'object' } }],
        })
        .pipe(toArray()),
    );
    expect(calls[0]?.bodyJson()).toEqual({
      model: 'claude-fable-5',
      max_tokens: 4096,
      stream: true,
      system: 'Be terse.',
      temperature: 0.2,
      tools: [{ name: 'get_weather', input_schema: { type: 'object' } }],
      messages: [
        { role: 'user', content: 'weather?' },
        {
          role: 'assistant',
          content: [
            {
              type: 'tool_use',
              id: 'toolu_1',
              name: 'get_weather',
              input: { city: 'Rapperswil' },
            },
          ],
        },
        {
          role: 'user',
          content: [{ type: 'tool_result', tool_use_id: 'toolu_1', content: 'rainy, 14°C' }],
        },
      ],
    });
  });

  it('per-call options override adapter defaults', async () => {
    const { fetchFn, calls } = mockFetch([textFixture]);
    await firstValueFrom(
      model(fetchFn)
        .stream([user('hi')], { model: 'claude-haiku-4-5-20251001', maxTokens: 64 })
        .pipe(toArray()),
    );
    expect(calls[0]?.bodyJson()).toMatchObject({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 64,
    });
  });
});

describe('anthropic adapter — laws', () => {
  it('is lazy and cold: no request until subscribe, one request per subscriber', async () => {
    const { fetchFn, calls } = mockFetch([textFixture]);
    const stream$ = model(fetchFn).stream([user('hi')]);
    expect(calls).toHaveLength(0);
    await firstValueFrom(stream$.pipe(toArray()));
    await firstValueFrom(stream$.pipe(toArray()));
    expect(calls).toHaveLength(2);
  });

  it('unsubscribe aborts the request without touching the error channel', async () => {
    const { fetchFn, calls } = mockFetch([textFixture], { hang: true });
    const errors: unknown[] = [];
    const received: StreamEvent[] = [];
    const subscription = model(fetchFn)
      .stream([user('hi')])
      .subscribe({ next: (e) => received.push(e), error: (e) => errors.push(e) });
    await vi.waitFor(() => expect(received.length).toBeGreaterThan(0));

    subscription.unsubscribe();
    expect(calls[0]?.init.signal?.aborted).toBe(true);
    await new Promise((r) => setTimeout(r, 0));
    expect(errors).toHaveLength(0);
  });
});
