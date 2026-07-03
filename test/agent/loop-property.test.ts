import fc from 'fast-check';
import { firstValueFrom, of } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { runAgent } from '../../src/agent/loop';
import { tool } from '../../src/agent/tool';
import type { ChatMessage } from '../../src/types';
import { scriptedModel, type ScriptedTurn } from '../helpers/scripted-model';

const user = (content: string): ChatMessage => ({ role: 'user', content });

const echo = tool({
  name: 'echo',
  input: z.object({ value: z.string() }),
  execute: ({ value }) => of(`echo:${value}`),
});

/** Random scenario: N tool-calling turns (1–3 calls each), then a final answer. */
const scenarioArb = fc
  .array(fc.integer({ min: 1, max: 3 }), { minLength: 0, maxLength: 5 })
  .map((callCounts) => {
    let id = 0;
    const turns: ScriptedTurn[] = callCounts.map((count) => ({
      toolCalls: Array.from({ length: count }, () => {
        id += 1;
        return { id: `t${id}`, name: 'echo', args: `{"value":"v${id}"}` };
      }),
    }));
    turns.push({ text: 'final answer' });
    return { turns, callCounts };
  });

describe('loop state properties (D6.1)', () => {
  it('messages grow strictly per iteration; every tool_use id has exactly one tool_result', async () => {
    await fc.assert(
      fc.asyncProperty(scenarioArb, async ({ turns, callCounts }) => {
        const scripted = scriptedModel(turns);
        const { result$ } = runAgent(scripted.model, {
          tools: [echo],
          messages: [user('go')],
          maxIterations: 10,
        });
        const outcome = await firstValueFrom(result$);
        expect(outcome.type).toBe('complete');
        const transcript = outcome.messages;

        // exact growth: 1 user + per tool-turn (assistant + k results) + final assistant
        const expectedLength =
          1 + callCounts.reduce((sum, k) => sum + 1 + k, 0) + 1;
        expect(transcript).toHaveLength(expectedLength);
        expect(outcome.iterations).toBe(callCounts.length + 1);

        // strict growth per iteration, observed at the wire: each model call
        // received strictly more messages than the one before
        const requestSizes = scripted.requests.map((m) => m.length);
        for (let i = 1; i < requestSizes.length; i += 1) {
          expect(requestSizes[i]!).toBeGreaterThan(requestSizes[i - 1]!);
        }

        // 1:1 pairing — every tool_use id has exactly one matching tool_result
        const callIds = transcript
          .filter((m) => m.role === 'assistant')
          .flatMap((m) => m.toolCalls ?? [])
          .map((c) => c.id);
        const resultIds = transcript
          .filter((m) => m.role === 'tool')
          .map((m) => m.toolCallId);
        expect([...resultIds].sort()).toEqual([...callIds].sort());
        expect(new Set(resultIds).size).toBe(resultIds.length); // no duplicates

        // and every result follows its call: for each assistant tool-turn,
        // the NEXT messages are exactly its results, in call order
        for (let i = 0; i < transcript.length; i += 1) {
          const message = transcript[i]!;
          if (message.role !== 'assistant' || message.toolCalls === undefined) continue;
          const expectedIds = message.toolCalls.map((c) => c.id);
          const following = transcript
            .slice(i + 1, i + 1 + expectedIds.length)
            .map((m) => ({ role: m.role, id: m.toolCallId }));
          expect(following).toEqual(expectedIds.map((id) => ({ role: 'tool', id })));
        }
      }),
      { numRuns: 30 },
    );
  });
});
