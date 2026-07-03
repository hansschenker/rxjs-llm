import { firstValueFrom, Observable, of, timer, map as rxMap } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import type { AgentEvent } from '../../src/agent/events';
import { runAgent } from '../../src/agent/loop';
import { tool, type ToolContext } from '../../src/agent/tool';
import type { ChatMessage } from '../../src/types';
import { scriptedModel, type ScriptedTurn } from '../helpers/scripted-model';

const user = (content: string): ChatMessage => ({ role: 'user', content });
const tick = (ms = 10) => new Promise((resolve) => setTimeout(resolve, ms));

const call = (id: string, name = 'noop', args = '{}') => ({ id, name, args });

const noop = tool({ name: 'noop', input: z.object({}), execute: () => of('ok') });

describe('safety rail: max iterations (D6.1)', () => {
  it('a tool-calling model that never stops hits the budget as an OUTCOME, not an error', async () => {
    const turns: ScriptedTurn[] = Array.from({ length: 10 }, (_, i) => ({
      toolCalls: [call(`t${i}`)],
    }));
    const scripted = scriptedModel(turns);
    const { result$ } = runAgent(scripted.model, {
      tools: [noop],
      messages: [user('loop forever')],
      maxIterations: 3,
    });
    const errors: unknown[] = [];
    const outcome = await firstValueFrom(result$).catch((e: unknown) => {
      errors.push(e);
      throw e;
    });

    expect(errors).toHaveLength(0);
    expect(outcome.type).toBe('max_iterations');
    if (outcome.type !== 'max_iterations') throw new Error('unreachable');
    expect(outcome.iterations).toBe(3); // exactly the budget
    expect(scripted.requests).toHaveLength(3); // and not one call more
    // the transcript is intact: 1 user + 3 × (assistant + tool result)
    expect(outcome.messages).toHaveLength(1 + 3 * 2);
    expect(outcome.usage).toEqual({ input: 30, output: 15 });
  });

  it('maxIterations: 0 answers immediately without ever calling the model', async () => {
    const scripted = scriptedModel([]);
    const { result$ } = runAgent(scripted.model, { messages: [user('q')], maxIterations: 0 });
    const outcome = await firstValueFrom(result$);
    expect(outcome.type).toBe('max_iterations');
    expect(scripted.requests).toHaveLength(0);
  });
});

describe('safety rail: per-tool timeout inside the loop (D6.3)', () => {
  it('a hung tool becomes a timeout notice and the loop continues to the next turn', async () => {
    const hang = tool({
      name: 'hang',
      input: z.object({}),
      execute: () => new Observable<never>(() => undefined),
      timeoutMs: 20,
    });
    const scripted = scriptedModel([
      { toolCalls: [call('t1', 'hang')] },
      { text: 'Recovered without the tool.' },
    ]);
    const { result$ } = runAgent(scripted.model, {
      tools: [hang],
      messages: [user('q')],
    });
    const outcome = await firstValueFrom(result$);

    expect(outcome.type).toBe('complete');
    const toolResult = outcome.messages.find((m) => m.role === 'tool');
    expect(toolResult?.content).toBe("Error: tool 'hang' timed out after 20ms");
    // the model saw the notice on its second call
    expect(scripted.requests[1]?.some((m) => m.content.includes('timed out'))).toBe(true);
  });
});

describe('safety rail: tool concurrency cap (D6.3)', () => {
  it('parallel tool calls run concurrently but never beyond the cap; results append in call order', async () => {
    let active = 0;
    let maxActive = 0;
    const slow = tool({
      name: 'slow',
      input: z.object({ n: z.number() }),
      execute: ({ n }) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        return timer(15).pipe(
          rxMap(() => {
            active -= 1;
            return `done ${n}`;
          }),
        );
      },
    });
    const scripted = scriptedModel([
      {
        toolCalls: [1, 2, 3, 4, 5].map((n) => ({
          id: `t${n}`,
          name: 'slow',
          args: `{"n":${n}}`,
        })),
      },
      { text: 'All done.' },
    ]);
    const { result$ } = runAgent(scripted.model, {
      tools: [slow],
      messages: [user('q')],
      toolConcurrency: 2,
    });
    const outcome = await firstValueFrom(result$);

    expect(maxActive).toBe(2); // genuinely parallel, capped at 2
    const toolMessages = outcome.messages.filter((m) => m.role === 'tool');
    expect(toolMessages.map((m) => m.toolCallId)).toEqual(['t1', 't2', 't3', 't4', 't5']);
    expect(toolMessages.map((m) => m.content)).toEqual([
      'done 1',
      'done 2',
      'done 3',
      'done 4',
      'done 5',
    ]);
  });
});

describe('the cancellation matrix (D6.3) — the crown jewel', () => {
  it('(a) unsubscribe during model streaming tears the model call down, both channels silent', async () => {
    const scripted = scriptedModel([{ hang: true }]);
    const { result$, progress$ } = runAgent(scripted.model, { messages: [user('q')] });
    const events: AgentEvent[] = [];
    let progressCompleted = false;
    let anyError = false;
    progress$.subscribe({
      next: (e) => events.push(e),
      complete: () => (progressCompleted = true),
      error: () => (anyError = true),
    });
    const subscription = result$.subscribe({ error: () => (anyError = true) });
    await vi.waitFor(() => expect(events.length).toBeGreaterThan(0)); // streaming

    subscription.unsubscribe();
    await tick();
    expect(scripted.tornDown[0]).toBe(true); // the model call was aborted
    expect(progressCompleted).toBe(true);
    expect(events.some((e) => e.type === 'agent_complete' || e.type === 'agent_failed')).toBe(
      false, // cancellation is silent — no terminal event
    );
    expect(anyError).toBe(false);
  });

  it('(b) unsubscribe during tool execution aborts EVERY in-flight tool — all AbortSignals fire', async () => {
    const signals: AbortSignal[] = [];
    const torndown: string[] = [];
    const hanging = tool({
      name: 'hanging',
      input: z.object({ id: z.string() }),
      execute: ({ id }, { signal }: ToolContext) => {
        signals.push(signal);
        return new Observable<never>(() => () => torndown.push(id));
      },
    });
    const scripted = scriptedModel([
      {
        toolCalls: [
          { id: 'a', name: 'hanging', args: '{"id":"a"}' },
          { id: 'b', name: 'hanging', args: '{"id":"b"}' },
        ],
      },
    ]);
    const { result$, progress$ } = runAgent(scripted.model, {
      tools: [hanging],
      messages: [user('q')],
    });
    const events: AgentEvent[] = [];
    let anyError = false;
    progress$.subscribe({ next: (e) => events.push(e), error: () => (anyError = true) });
    const subscription = result$.subscribe({ error: () => (anyError = true) });
    await vi.waitFor(() =>
      expect(events.filter((e) => e.type === 'tool_start')).toHaveLength(2),
    );

    subscription.unsubscribe();
    await tick();
    expect(torndown.sort()).toEqual(['a', 'b']); // both tool executions torn down
    expect(signals).toHaveLength(2);
    expect(signals.every((s) => s.aborted)).toBe(true); // multi-AbortSignal assertion
    expect(anyError).toBe(false);
  });

  it('(c) unsubscribe between iterations aborts the just-started next model call', async () => {
    const fast = tool({ name: 'fast', input: z.object({}), execute: () => of('done') });
    const scripted = scriptedModel([
      { toolCalls: [call('t1', 'fast')] },
      { hang: true }, // iteration 2's model call
    ]);
    const { result$, progress$ } = runAgent(scripted.model, {
      tools: [fast],
      messages: [user('q')],
    });
    const events: AgentEvent[] = [];
    let anyError = false;
    progress$.subscribe({ next: (e) => events.push(e), error: () => (anyError = true) });
    const subscription = result$.subscribe({ error: () => (anyError = true) });

    // wait for the boundary: tool_result observed → iteration 2 begins
    await vi.waitFor(() => expect(events.some((e) => e.type === 'tool_result')).toBe(true));
    await vi.waitFor(() => expect(scripted.requests).toHaveLength(2)); // call 2 issued
    subscription.unsubscribe();
    await tick();

    expect(scripted.tornDown[1]).toBe(true); // iteration 2's model call aborted
    expect(anyError).toBe(false);
    expect(events.some((e) => e.type === 'agent_complete' || e.type === 'agent_failed')).toBe(
      false,
    );
  });
});
