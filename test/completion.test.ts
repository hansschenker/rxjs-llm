import { firstValueFrom, from } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { collectCompletion, foldEvents } from '../src/completion';
import type { StreamEvent } from '../src/types';

const textStream: StreamEvent[] = [
  { type: 'message_start', model: 'claude-fable-5' },
  { type: 'text_delta', text: 'Hello' },
  { type: 'text_delta', text: ', world' },
  { type: 'usage', input: 10, output: 4 },
  { type: 'message_stop', stopReason: 'end_turn' },
];

const toolStream: StreamEvent[] = [
  { type: 'message_start', model: 'claude-fable-5' },
  { type: 'thinking_delta', text: 'The user wants weather. ' },
  { type: 'thinking_delta', text: 'I should call the tool.' },
  { type: 'tool_call_delta', id: 'call_1', name: 'get_weather', argsDelta: '' },
  { type: 'tool_call_delta', id: 'call_1', argsDelta: '{"city":' },
  { type: 'tool_call_delta', id: 'call_1', argsDelta: '"Rapperswil"}' },
  { type: 'tool_call_delta', id: 'call_2', name: 'get_time', argsDelta: '{}' },
  { type: 'usage', input: 50, output: 30 },
  { type: 'message_stop', stopReason: 'tool_use' },
];

describe('foldEvents', () => {
  it('reduces text deltas to the final text', () => {
    const completion = foldEvents(textStream);
    expect(completion).toEqual({
      model: 'claude-fable-5',
      text: 'Hello, world',
      thinking: '',
      toolCalls: [],
      usage: { input: 10, output: 4 },
      stopReason: 'end_turn',
    });
  });

  it('assembles tool calls from fragments, preserving first-seen order', () => {
    const completion = foldEvents(toolStream);
    expect(completion.thinking).toBe('The user wants weather. I should call the tool.');
    expect(completion.toolCalls).toEqual([
      { id: 'call_1', name: 'get_weather', args: '{"city":"Rapperswil"}' },
      { id: 'call_2', name: 'get_time', args: '{}' },
    ]);
    expect(completion.stopReason).toBe('tool_use');
  });

  it('yields defaults for an empty stream', () => {
    const completion = foldEvents([]);
    expect(completion.stopReason).toBe('other');
    expect(completion.text).toBe('');
    expect(completion.toolCalls).toEqual([]);
  });
});

describe('collectCompletion', () => {
  it('emits exactly one completion when the source completes', async () => {
    const completion = await firstValueFrom(from(textStream).pipe(collectCompletion()));
    expect(completion.text).toBe('Hello, world');
  });

  it('keeps state independent across subscriptions to the same cold pipe', async () => {
    const piped = from(toolStream).pipe(collectCompletion());
    const first = await firstValueFrom(piped);
    const second = await firstValueFrom(piped);
    expect(second).toEqual(first);
    expect(second.toolCalls[0]?.args).toBe('{"city":"Rapperswil"}');
  });
});
