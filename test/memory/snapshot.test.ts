import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { createMemory, type MemoryView, type Turn } from '../../src/memory/core';
import { fullView, tokenBudgetView, windowView } from '../../src/memory/views';
import type { ChatMessage } from '../../src/types';

function currentView(memory: ReturnType<typeof createMemory>): ChatMessage[] {
  let messages: ChatMessage[] = [];
  memory.view().subscribe((m) => (messages = m));
  return messages;
}

describe('snapshot / restore (D5.4)', () => {
  it('snapshot is plain serializable data', () => {
    const memory = createMemory();
    memory.record({ user: 'q', assistant: 'a' });
    const snapshot = memory.snapshot();
    expect(JSON.parse(JSON.stringify(snapshot))).toEqual(snapshot);
    expect(snapshot).toEqual({ turns: [{ user: 'q', assistant: 'a' }] });
  });

  it('snapshot is a copy: later records do not leak into it', () => {
    const memory = createMemory();
    memory.record({ user: 'q1', assistant: 'a1' });
    const snapshot = memory.snapshot();
    memory.record({ user: 'q2', assistant: 'a2' });
    expect(snapshot.turns).toHaveLength(1);
  });

  it('property: for any turn sequence and any pure view, restore(snapshot(m)) yields an equivalent view', () => {
    const turnArb = fc.record({
      user: fc.string({ maxLength: 40 }),
      assistant: fc.string({ maxLength: 40 }),
    });
    const viewArb = fc.constantFrom<[string, () => MemoryView]>(
      ['full', () => fullView()],
      ['window(2)', () => windowView(2)],
      ['budget(25)', () => tokenBudgetView(25)],
    );
    fc.assert(
      fc.property(
        fc.array(turnArb, { maxLength: 15 }),
        viewArb,
        (turns: Turn[], [, makeView]) => {
          const original = createMemory({ view: makeView() });
          for (const turn of turns) original.record(turn);

          const restored = createMemory({ view: makeView(), restore: original.snapshot() });
          expect(currentView(restored)).toEqual(currentView(original));
          expect(restored.snapshot()).toEqual(original.snapshot());

          original.dispose();
          restored.dispose();
        },
      ),
    );
  });

  it('a restored memory keeps recording from where the snapshot left off', () => {
    const original = createMemory();
    original.record({ user: 'q1', assistant: 'a1' });

    const restored = createMemory({ restore: original.snapshot() });
    restored.record({ user: 'q2', assistant: 'a2' });
    expect(currentView(restored).map((m) => m.content)).toEqual(['q1', 'a1', 'q2', 'a2']);
    expect(currentView(original)).toHaveLength(2); // the original is untouched
  });
});
