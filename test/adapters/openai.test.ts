import { firstValueFrom, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { openai } from '../../src/adapters/openai';
import { ProviderError } from '../../src/errors';
import type { ChatMessage, StreamEvent } from '../../src/types';
import { mockFetch, sseFrames } from '../helpers/mock-fetch';

const user = (content: string): ChatMessage => ({ role: 'user', content });

function model(fetchFn: typeof fetch) {
  return openai({ apiKey: 'sk-test', model: 'gpt-4o', fetchFn });
}

const textFixture = sseFrames([
  {
    data: {
      model: 'gpt-4o',
      choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }],
    },
  },
  { data: { model: 'gpt-4o', choices: [{ index: 0, delta: { content: 'Hello' } }] } },
  { data: { model: 'gpt-4o', choices: [{ index: 0, delta: { content: ' world' } }] } },
  { data: { model: 'gpt-4o', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] } },
  { data: { model: 'gpt-4o', choices: [], usage: { prompt_tokens: 9, completion_tokens: 2 } } },
  { data: '[DONE]' },
]);

const toolFixture = sseFrames([
  {
    data: {
      model: 'gpt-4o',
      choices: [
        {
          index: 0,
          delta: {
            role: 'assistant',
            tool_calls: [
              {
                index: 0,
                id: 'call_A',
                type: 'function',
                function: { name: 'get_weather', arguments: '' },
              },
            ],
          },
        },
      ],
    },
  },
  {
    data: {
      model: 'gpt-4o',
      choices: [
        { index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '{"city":' } }] } },
      ],
    },
  },
  {
    data: {
      model: 'gpt-4o',
      choices: [
        {
          index: 0,
          delta: { tool_calls: [{ index: 0, function: { arguments: '"Rapperswil"}' } }] },
        },
      ],
    },
  },
  { data: { model: 'gpt-4o', choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] } },
  { data: { model: 'gpt-4o', choices: [], usage: { prompt_tokens: 40, completion_tokens: 15 } } },
  { data: '[DONE]' },
]);

describe('openai adapter — event mapping', () => {
  it('maps a text stream, ending on the [DONE] sentinel', async () => {
    const { fetchFn } = mockFetch([textFixture]);
    const events = await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(events).toEqual<StreamEvent[]>([
      { type: 'message_start', model: 'gpt-4o' },
      { type: 'text_delta', text: 'Hello' },
      { type: 'text_delta', text: ' world' },
      { type: 'usage', input: 9, output: 2 },
      { type: 'message_stop', stopReason: 'end_turn' },
    ]);
  });

  it('re-attaches indexed tool-call fragments to the id from the first fragment', async () => {
    const { fetchFn } = mockFetch([toolFixture]);
    const events = await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(events).toEqual<StreamEvent[]>([
      { type: 'message_start', model: 'gpt-4o' },
      { type: 'tool_call_delta', id: 'call_A', name: 'get_weather', argsDelta: '' },
      { type: 'tool_call_delta', id: 'call_A', argsDelta: '{"city":' },
      { type: 'tool_call_delta', id: 'call_A', argsDelta: '"Rapperswil"}' },
      { type: 'usage', input: 40, output: 15 },
      { type: 'message_stop', stopReason: 'tool_use' },
    ]);
  });

  it('complete() assembles the full tool call', async () => {
    const { fetchFn } = mockFetch([toolFixture]);
    const completion = await firstValueFrom(model(fetchFn).complete([user('hi')]));
    expect(completion.toolCalls).toEqual([
      { id: 'call_A', name: 'get_weather', args: '{"city":"Rapperswil"}' },
    ]);
    expect(completion.stopReason).toBe('tool_use');
  });

  it('surfaces in-stream error payloads as ProviderError', async () => {
    const { fetchFn } = mockFetch([
      sseFrames([
        { data: { error: { message: 'The server is overloaded', type: 'server_error' } } },
      ]),
    ]);
    const error = await firstValueFrom(model(fetchFn).stream([user('hi')])).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ProviderError);
    expect((error as ProviderError).provider).toBe('openai');
    expect((error as ProviderError).retryable).toBe(true);
  });
});

describe('openai adapter — request shape', () => {
  it('targets /v1/chat/completions with bearer auth and usage reporting on', async () => {
    const { fetchFn, calls } = mockFetch([textFixture]);
    await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(calls[0]?.url).toBe('https://api.openai.com/v1/chat/completions');
    expect(calls[0]?.init.headers).toMatchObject({ authorization: 'Bearer sk-test' });
    expect(calls[0]?.bodyJson()).toMatchObject({
      stream: true,
      stream_options: { include_usage: true },
    });
  });

  it('maps tools, assistant tool calls, and tool results to OpenAI shapes', async () => {
    const { fetchFn, calls } = mockFetch([textFixture]);
    const messages: ChatMessage[] = [
      user('weather?'),
      {
        role: 'assistant',
        content: '',
        toolCalls: [{ id: 'call_A', name: 'get_weather', args: '{"city":"Rapperswil"}' }],
      },
      { role: 'tool', content: 'rainy', toolCallId: 'call_A' },
    ];
    await firstValueFrom(
      model(fetchFn)
        .stream(messages, {
          system: 'Be terse.',
          tools: [{ name: 'get_weather', inputSchema: { type: 'object' } }],
        })
        .pipe(toArray()),
    );
    expect(calls[0]?.bodyJson()).toMatchObject({
      messages: [
        { role: 'system', content: 'Be terse.' },
        { role: 'user', content: 'weather?' },
        {
          role: 'assistant',
          content: null,
          tool_calls: [
            {
              id: 'call_A',
              type: 'function',
              function: { name: 'get_weather', arguments: '{"city":"Rapperswil"}' },
            },
          ],
        },
        { role: 'tool', tool_call_id: 'call_A', content: 'rainy' },
      ],
      tools: [
        { type: 'function', function: { name: 'get_weather', parameters: { type: 'object' } } },
      ],
    });
  });
});
