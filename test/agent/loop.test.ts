import { firstValueFrom, of } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import type { AgentEvent } from '../../src/agent/events';
import { runAgent } from '../../src/agent/loop';
import { tool } from '../../src/agent/tool';
import type { ChatMessage } from '../../src/types';
import { scriptedModel } from '../helpers/scripted-model';

const user = (content: string): ChatMessage => ({ role: 'user', content });

const weather = tool({
  name: 'get_weather',
  description: 'Weather for a city',
  input: z.object({ city: z.string() }),
  execute: ({ city }) => of(`rainy in ${city}, 14°C`),
});

describe('runAgent — the happy path (D6.1)', () => {
  it('answers directly when the model requests no tools', async () => {
    const scripted = scriptedModel([{ text: 'Just an answer.' }]);
    const { result$ } = runAgent(scripted.model, { messages: [user('hi')] });
    const outcome = await firstValueFrom(result$);

    expect(outcome.type).toBe('complete');
    if (outcome.type !== 'complete') throw new Error('unreachable');
    expect(outcome.text).toBe('Just an answer.');
    expect(outcome.iterations).toBe(1);
    expect(outcome.usage).toEqual({ input: 10, output: 5 });
    expect(scripted.requests).toHaveLength(1);
  });

  it('executes tool calls, appends results, recurses, and terminates — message growth pinned', async () => {
    const scripted = scriptedModel([
      { toolCalls: [{ id: 't1', name: 'get_weather', args: '{"city":"Rapperswil"}' }] },
      { text: 'Bring an umbrella.' },
    ]);
    const { result$ } = runAgent(scripted.model, {
      tools: [weather],
      messages: [user('Do I need an umbrella?')],
    });
    const outcome = await firstValueFrom(result$);

    expect(outcome.type).toBe('complete');
    if (outcome.type !== 'complete') throw new Error('unreachable');
    expect(outcome.text).toBe('Bring an umbrella.');
    expect(outcome.iterations).toBe(2);
    expect(outcome.usage).toEqual({ input: 20, output: 10 }); // summed across calls

    // the final transcript: user, assistant(tool call), tool result, assistant(answer)
    expect(outcome.messages.map((m) => m.role)).toEqual(['user', 'assistant', 'tool', 'assistant']);
    expect(outcome.messages[1]?.toolCalls).toEqual([
      { id: 't1', name: 'get_weather', args: '{"city":"Rapperswil"}' },
    ]);
    expect(outcome.messages[2]).toEqual({
      role: 'tool',
      content: 'rainy in Rapperswil, 14°C',
      toolCallId: 't1',
    });

    // the SECOND model call received the grown message list
    expect(scripted.requests[1]).toHaveLength(3);
    expect(scripted.requests[1]?.[2]?.content).toBe('rainy in Rapperswil, 14°C');
    // and tools were declared to the provider on every call
    expect(scripted.requestOptions[0]?.tools?.[0]?.name).toBe('get_weather');
  });

  it('a self-correction round-trip: invalid args come back as a tool result the model recovers from', async () => {
    const scripted = scriptedModel([
      { toolCalls: [{ id: 't1', name: 'get_weather', args: '{"city":42}' }] },
      { toolCalls: [{ id: 't2', name: 'get_weather', args: '{"city":"Bern"}' }] },
      { text: 'Sunny in Bern.' },
    ]);
    const { result$ } = runAgent(scripted.model, {
      tools: [weather],
      messages: [user('weather in Bern?')],
    });
    const outcome = await firstValueFrom(result$);

    expect(outcome.type).toBe('complete');
    expect(outcome.messages.filter((m) => m.role === 'tool')).toHaveLength(2);
    const firstToolResult = outcome.messages.find((m) => m.role === 'tool');
    expect(firstToolResult?.content).toMatch(/^Error: invalid arguments/);
    expect(outcome.iterations).toBe(3); // the model got to try again
  });

  it('progress$ interleaves model deltas and tool lifecycle, tagged by iteration, one terminal event', async () => {
    const scripted = scriptedModel([
      { toolCalls: [{ id: 't1', name: 'get_weather', args: '{"city":"Chur"}' }] },
      { text: 'Answer.' },
    ]);
    const { result$, progress$ } = runAgent(scripted.model, {
      tools: [weather],
      messages: [user('q')],
    });
    const events: AgentEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    await firstValueFrom(result$);
    await new Promise((r) => setTimeout(r, 0));

    const kinds = events.map((e) => e.type);
    expect(kinds.filter((k) => k === 'tool_start')).toHaveLength(1);
    expect(kinds.filter((k) => k === 'tool_result')).toHaveLength(1);
    expect(kinds.at(-1)).toBe('agent_complete');
    expect(kinds.filter((k) => k === 'agent_complete')).toHaveLength(1);

    const iterations = events
      .filter((e): e is Extract<AgentEvent, { type: 'model_event' }> => e.type === 'model_event')
      .map((e) => e.iteration);
    expect(new Set(iterations)).toEqual(new Set([1, 2])); // deltas tagged per model call

    const toolStart = events.find((e) => e.type === 'tool_start');
    expect(toolStart).toMatchObject({ iteration: 1, id: 't1', tool: 'get_weather' });
    // ordering: tool lifecycle sits between iteration 1's and iteration 2's deltas
    const lastIter1Delta = kinds.lastIndexOf('model_event');
    expect(events.findIndex((e) => e.type === 'tool_start')).toBeLessThan(lastIter1Delta);
  });

  it('the agent contract is the chain contract: lazy, latched, one execution (D6.4)', async () => {
    const scripted = scriptedModel([{ text: 'once' }]);
    const { result$ } = runAgent(scripted.model, { messages: [user('q')] });
    expect(scripted.requests).toHaveLength(0); // lazy — nothing before subscribe

    const first = await firstValueFrom(result$);
    const second = await firstValueFrom(result$); // latched — no re-execution
    expect(second).toBe(first);
    expect(scripted.requests).toHaveLength(1);
  });
});
