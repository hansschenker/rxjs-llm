import { firstValueFrom, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { ollama } from '../../src/adapters/ollama';
import { ProviderError } from '../../src/errors';
import type { ChatMessage, StreamEvent } from '../../src/types';
import { mockFetch } from '../helpers/mock-fetch';

const user = (content: string): ChatMessage => ({ role: 'user', content });

function model(fetchFn: typeof fetch) {
  return ollama({ model: 'llama3.2', fetchFn });
}

function ndjson(lines: unknown[]): string {
  return lines.map((line) => `${JSON.stringify(line)}\n`).join('');
}

const textLines = [
  { model: 'llama3.2', message: { role: 'assistant', content: 'Hel' }, done: false },
  { model: 'llama3.2', message: { role: 'assistant', content: 'lo' }, done: false },
  {
    model: 'llama3.2',
    message: { role: 'assistant', content: '' },
    done: true,
    done_reason: 'stop',
    prompt_eval_count: 7,
    eval_count: 3,
  },
];

describe('ollama adapter — event mapping', () => {
  it('maps an NDJSON text stream to the normalized taxonomy', async () => {
    const { fetchFn } = mockFetch([ndjson(textLines)]);
    const events = await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(events).toEqual<StreamEvent[]>([
      { type: 'message_start', model: 'llama3.2' },
      { type: 'text_delta', text: 'Hel' },
      { type: 'text_delta', text: 'lo' },
      { type: 'usage', input: 7, output: 3 },
      { type: 'message_stop', stopReason: 'end_turn' },
    ]);
  });

  it('parses identically when chunk boundaries fall mid-line', async () => {
    const whole = ndjson(textLines);
    const { fetchFn } = mockFetch([whole.slice(0, 25), whole.slice(25, 60), whole.slice(60)]);
    const events = await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(events).toHaveLength(5);
    expect(events[1]).toEqual({ type: 'text_delta', text: 'Hel' });
  });

  it('emits whole tool calls as single fragments with synthetic ids', async () => {
    const { fetchFn } = mockFetch([
      ndjson([
        {
          model: 'llama3.2',
          message: {
            role: 'assistant',
            content: '',
            tool_calls: [
              { function: { name: 'get_weather', arguments: { city: 'Rapperswil' } } },
            ],
          },
          done: false,
        },
        {
          model: 'llama3.2',
          message: { role: 'assistant', content: '' },
          done: true,
          done_reason: 'stop',
          prompt_eval_count: 20,
          eval_count: 12,
        },
      ]),
    ]);
    const events = await firstValueFrom(model(fetchFn).stream([user('weather?')]).pipe(toArray()));
    expect(events).toEqual<StreamEvent[]>([
      { type: 'message_start', model: 'llama3.2' },
      {
        type: 'tool_call_delta',
        id: 'ollama_call_1',
        name: 'get_weather',
        argsDelta: '{"city":"Rapperswil"}',
      },
      { type: 'usage', input: 20, output: 12 },
      // done_reason is 'stop', but a turn that requested tools is a tool_use stop
      { type: 'message_stop', stopReason: 'tool_use' },
    ]);
  });

  it('maps thinking fields to thinking_delta', async () => {
    const { fetchFn } = mockFetch([
      ndjson([
        { model: 'llama3.2', message: { role: 'assistant', thinking: 'hmm', content: '' } },
        {
          model: 'llama3.2',
          message: { role: 'assistant', content: 'answer' },
          done: true,
          done_reason: 'stop',
        },
      ]),
    ]);
    const events = await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(events[1]).toEqual({ type: 'thinking_delta', text: 'hmm' });
  });

  it('surfaces error lines as ProviderError', async () => {
    const { fetchFn } = mockFetch([ndjson([{ error: 'model "nope" not found' }])]);
    const error = await firstValueFrom(model(fetchFn).stream([user('hi')])).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ProviderError);
    expect((error as ProviderError).provider).toBe('ollama');
  });
});

describe('ollama adapter — request shape', () => {
  it('targets /api/chat on the local daemon with no auth header', async () => {
    const { fetchFn, calls } = mockFetch([ndjson(textLines)]);
    await firstValueFrom(model(fetchFn).stream([user('hi')]).pipe(toArray()));
    expect(calls[0]?.url).toBe('http://localhost:11434/api/chat');
    expect(calls[0]?.init.headers).toEqual({ 'content-type': 'application/json' });
  });

  it('maps ChatOptions into the options object Ollama expects', async () => {
    const { fetchFn, calls } = mockFetch([ndjson(textLines)]);
    await firstValueFrom(
      model(fetchFn)
        .stream([user('hi')], { temperature: 0.1, maxTokens: 128, stopSequences: ['END'] })
        .pipe(toArray()),
    );
    expect(calls[0]?.bodyJson()).toMatchObject({
      model: 'llama3.2',
      stream: true,
      options: { temperature: 0.1, num_predict: 128, stop: ['END'] },
    });
  });
});
