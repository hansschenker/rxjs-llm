import fc from 'fast-check';
import { firstValueFrom } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { createMemory, type Turn } from '../../src/memory/core';
import { fullView, tokenBudgetView, turnTokens, windowView } from '../../src/memory/views';
import type { ChatMessage } from '../../src/types';
import type { Tokenizer } from '../../src/index/split';

const turn = (n: number): Turn => ({ user: `question ${n}`, assistant: `answer ${n}` });

describe('createMemory + fullView (D5.1)', () => {
  it('starts empty and reflects every recorded turn, in order', async () => {
    const memory = createMemory();
    expect(await firstValueFrom(memory.view())).toEqual([]);
    memory.record(turn(1));
    memory.record(turn(2));
    expect(await firstValueFrom(memory.view())).toEqual([
      { role: 'user', content: 'question 1' },
      { role: 'assistant', content: 'answer 1' },
      { role: 'user', content: 'question 2' },
      { role: 'assistant', content: 'answer 2' },
    ]);
  });

  it('is reactive: an EXISTING subscriber receives an updated view per record()', () => {
    const memory = createMemory();
    const emissions: ChatMessage[][] = [];
    memory.view().subscribe((messages) => emissions.push(messages));
    memory.record(turn(1));
    memory.record(turn(2));
    expect(emissions.map((m) => m.length)).toEqual([0, 2, 4]);
  });

  it('view() is shared: late subscribers get the current projection, not a replay of history', async () => {
    const memory = createMemory();
    memory.record(turn(1));
    const late: ChatMessage[][] = [];
    memory.view().subscribe((m) => late.push(m));
    expect(late).toHaveLength(1); // just the latest
    expect(late[0]).toHaveLength(2);
  });

  it('dispose() completes the view and stops accepting records', () => {
    const memory = createMemory();
    let completed = false;
    memory.view().subscribe({ complete: () => (completed = true) });
    memory.record(turn(1));
    memory.dispose();
    expect(completed).toBe(true);
    expect(() => memory.record(turn(2))).not.toThrow(); // ignored, not an error
  });
});

describe('windowView', () => {
  it('projects only the last n turns', async () => {
    const memory = createMemory({ view: windowView(2) });
    for (let i = 1; i <= 5; i += 1) memory.record(turn(i));
    const messages = await firstValueFrom(memory.view());
    expect(messages.map((m) => m.content)).toEqual([
      'question 4',
      'answer 4',
      'question 5',
      'answer 5',
    ]);
  });

  it('rejects a non-positive window', () => {
    expect(() => windowView(0)).toThrow(RangeError);
  });
});

describe('tokenBudgetView (D5.3)', () => {
  const wordTokenizer: Tokenizer = {
    count: (text) => text.split(/\s+/).filter((w) => w !== '').length,
  };

  it('keeps the newest turns that fit and evicts oldest-first', async () => {
    // each turn costs 4 word-tokens ("question N" + "answer N")
    const memory = createMemory({ view: tokenBudgetView(9, wordTokenizer) });
    for (let i = 1; i <= 5; i += 1) memory.record(turn(i));
    const messages = await firstValueFrom(memory.view());
    // budget 9 fits two whole turns (8), not three (12)
    expect(messages.map((m) => m.content)).toEqual([
      'question 4',
      'answer 4',
      'question 5',
      'answer 5',
    ]);
  });

  it('never splits a turn: one oversized turn yields an empty history', async () => {
    const memory = createMemory({ view: tokenBudgetView(3, wordTokenizer) });
    memory.record({ user: 'one two three', assistant: 'four five six' });
    expect(await firstValueFrom(memory.view())).toEqual([]);
  });

  it('property: kept turns fit the budget, are a suffix, and pair correctly', () => {
    const turnArb = fc.record({
      user: fc.string({ minLength: 1, maxLength: 60 }),
      assistant: fc.string({ minLength: 1, maxLength: 60 }),
    });
    fc.assert(
      fc.property(
        fc.array(turnArb, { minLength: 0, maxLength: 20 }),
        fc.integer({ min: 1, max: 80 }),
        (turns, budget) => {
          const memory = createMemory({ view: tokenBudgetView(budget) });
          for (const t of turns) memory.record(t);
          let messages: ChatMessage[] = [];
          memory.view().subscribe((m) => (messages = m));

          expect(messages.length % 2).toBe(0);
          const keptTurns: Turn[] = [];
          for (let i = 0; i < messages.length; i += 2) {
            expect(messages[i]?.role).toBe('user');
            expect(messages[i + 1]?.role).toBe('assistant');
            keptTurns.push({ user: messages[i]!.content, assistant: messages[i + 1]!.content });
          }
          // total cost within budget
          const cost = keptTurns.reduce(
            (sum, t) => sum + turnTokens(t, { count: (x) => Math.ceil(x.length / 4) }),
            0,
          );
          expect(cost).toBeLessThanOrEqual(budget);
          // kept turns are exactly a suffix of the input
          expect(keptTurns).toEqual(turns.slice(turns.length - keptTurns.length));
          memory.dispose();
        },
      ),
    );
  });
});

describe('views are swappable over the same reducer (D5.1)', () => {
  it('two memories restored from one snapshot project differently but from the same truth', async () => {
    const source = createMemory();
    for (let i = 1; i <= 4; i += 1) source.record(turn(i));
    const snapshot = source.snapshot();

    const full = createMemory({ view: fullView(), restore: snapshot });
    const windowed = createMemory({ view: windowView(1), restore: snapshot });
    expect(await firstValueFrom(full.view())).toHaveLength(8);
    expect(await firstValueFrom(windowed.view())).toHaveLength(2);
  });
});
