import { defer, map, Observable, of, timer } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { createMemory, type Turn } from '../../src/memory/core';
import { summaryView } from '../../src/memory/summary';
import type { ChatCompletion, ChatMessage, ChatModel } from '../../src/types';

const turn = (n: number): Turn => ({ user: `question ${n}`, assistant: `answer ${n}` });

const completion = (text: string): ChatCompletion => ({
  model: 'mock',
  text,
  thinking: '',
  toolCalls: [],
  usage: { input: 0, output: 0 },
  stopReason: 'end_turn',
});

/** A ChatModel whose complete() is scripted per call; stream() is unused. */
function scriptedModel(behavior: (call: number, prompt: string) => Observable<string>) {
  let calls = 0;
  const prompts: string[] = [];
  const model: ChatModel = {
    stream: () => {
      throw new Error('summary folds use complete(), not stream()');
    },
    complete: (messages) =>
      defer(() => {
        calls += 1;
        const prompt = messages[0]?.content ?? '';
        prompts.push(prompt);
        return behavior(calls, prompt).pipe(map(completion));
      }),
  };
  return { model, calls: () => calls, prompts };
}

const tick = (ms = 10) => new Promise((resolve) => setTimeout(resolve, ms));

describe('summaryView (D5.2)', () => {
  it('does not fold below the threshold', async () => {
    const { model, calls } = scriptedModel(() => of('SUMMARY'));
    const memory = createMemory({ view: summaryView(model, undefined, { foldAfter: 4 }) });
    for (let i = 1; i <= 4; i += 1) memory.record(turn(i)); // 4 is not > 4
    await tick();
    expect(calls()).toBe(0);
    let messages: ChatMessage[] = [];
    memory.view().subscribe((m) => (messages = m));
    expect(messages).toHaveLength(8); // raw, no summary
    memory.dispose();
  });

  it('folds the tail once the threshold is crossed, keeping recent turns verbatim', async () => {
    const { model, calls, prompts } = scriptedModel(() => timer(5).pipe(map(() => 'THE SUMMARY')));
    const memory = createMemory({
      view: summaryView(model, undefined, { foldAfter: 3, keepRecent: 2 }),
    });
    for (let i = 1; i <= 4; i += 1) memory.record(turn(i)); // 4 > 3 → fold turns 1..2

    await vi.waitFor(() => expect(calls()).toBe(1));
    await tick(15);
    let messages: ChatMessage[] = [];
    memory.view().subscribe((m) => (messages = m));

    expect(messages[0]).toEqual({
      role: 'system',
      content: 'Summary of the earlier conversation:\nTHE SUMMARY',
    });
    expect(messages.slice(1).map((m) => m.content)).toEqual([
      'question 3',
      'answer 3',
      'question 4',
      'answer 4',
    ]);
    expect(prompts[0]).toContain('(none yet)'); // first fold has no prior summary
    expect(prompts[0]).toContain('question 1');
    expect(prompts[0]).toContain('answer 2');
    expect(prompts[0]).not.toContain('question 3'); // kept-recent turns are not folded
    memory.dispose();
  });

  it('is eventually consistent: the raw tail serves while the fold is in flight, pending$ exposes the window', async () => {
    const { model } = scriptedModel(() => timer(30).pipe(map(() => 'LATE SUMMARY')));
    const view = summaryView(model, undefined, { foldAfter: 2, keepRecent: 0 });
    const memory = createMemory({ view });
    const pendingStates: boolean[] = [];
    view.pending$.subscribe((p) => pendingStates.push(p));

    for (let i = 1; i <= 3; i += 1) memory.record(turn(i));
    await tick(5); // fold in flight

    let during: ChatMessage[] = [];
    memory.view().subscribe((m) => (during = m));
    expect(during).toHaveLength(6); // raw turns — the conversation never blocked
    expect(during[0]?.role).toBe('user');
    expect(pendingStates.at(-1)).toBe(true);

    await vi.waitFor(() => expect(pendingStates.at(-1)).toBe(false));
    let after: ChatMessage[] = [];
    memory.view().subscribe((m) => (after = m));
    expect(after[0]?.role).toBe('system'); // summary landed
    expect(after[0]?.content).toContain('LATE SUMMARY');
    memory.dispose();
  });

  it('never overlaps folds, and a backlog folds again right after (exhaustMap + state re-eval)', async () => {
    const { model, calls } = scriptedModel((call) => timer(20).pipe(map(() => `S${call}`)));
    const memory = createMemory({
      view: summaryView(model, undefined, { foldAfter: 2, keepRecent: 0 }),
    });

    for (let i = 1; i <= 3; i += 1) memory.record(turn(i)); // triggers fold 1
    await tick(5);
    expect(calls()).toBe(1);
    for (let i = 4; i <= 7; i += 1) memory.record(turn(i)); // arrives mid-fold
    await tick(5);
    expect(calls()).toBe(1); // exhaustMap: no overlap

    // fold 1 completes (folded=3), backlog of 4 un-summarized > 2 → fold 2 fires
    await vi.waitFor(() => expect(calls()).toBe(2));
    memory.dispose();
  });

  it('a fold failure degrades to the raw view and retries on the next record — never amnesia', async () => {
    const { model, calls } = scriptedModel((call) =>
      call === 1
        ? timer(5).pipe(
            map((): string => {
              throw new Error('summarizer down');
            }),
          )
        : timer(5).pipe(map(() => 'RECOVERED')),
    );
    const memory = createMemory({
      view: summaryView(model, undefined, { foldAfter: 2, keepRecent: 0 }),
    });
    for (let i = 1; i <= 3; i += 1) memory.record(turn(i));
    await vi.waitFor(() => expect(calls()).toBe(1));
    await tick(15);

    let messages: ChatMessage[] = [];
    memory.view().subscribe((m) => (messages = m));
    expect(messages).toHaveLength(6); // all raw turns intact after the failure
    expect(messages.every((m) => m.role !== 'system')).toBe(true);

    memory.record(turn(4)); // retry trigger
    await vi.waitFor(() => expect(calls()).toBe(2));
    await tick(15);
    memory.view().subscribe((m) => (messages = m));
    expect(messages[0]?.content).toContain('RECOVERED');
    memory.dispose();
  });

  it('dispose() mid-fold aborts the model call and completes pending$', async () => {
    let torndown = false;
    const model: ChatModel = {
      stream: () => {
        throw new Error('unused');
      },
      complete: () =>
        new Observable<ChatCompletion>(() => {
          return () => (torndown = true);
        }),
    };
    const view = summaryView(model, undefined, { foldAfter: 1, keepRecent: 0 });
    const memory = createMemory({ view });
    let pendingCompleted = false;
    const pendingStates: boolean[] = [];
    view.pending$.subscribe({
      next: (p) => pendingStates.push(p),
      complete: () => (pendingCompleted = true),
    });

    memory.record(turn(1));
    memory.record(turn(2)); // 2 > 1 → fold starts, hangs
    await tick(5);
    expect(pendingStates.at(-1)).toBe(true);

    memory.dispose();
    expect(torndown).toBe(true); // in-flight fold aborted
    expect(pendingStates.at(-1)).toBe(false); // finalize ran
    expect(pendingCompleted).toBe(true);
  });

  it('binds to exactly one memory', () => {
    const { model } = scriptedModel(() => of('S'));
    const view = summaryView(model);
    createMemory({ view });
    expect(() => createMemory({ view })).toThrow(/one memory/);
  });

  it('a custom prompt template receives the running summary and rendered turns', async () => {
    const { model, calls, prompts } = scriptedModel((call) => of(`S${call}`));
    const memory = createMemory({
      view: summaryView(
        model,
        ({ summary, turns }) => `PREV<${summary}> NEW<${turns}>`,
        { foldAfter: 1, keepRecent: 0 },
      ),
    });
    memory.record(turn(1));
    memory.record(turn(2));
    await vi.waitFor(() => expect(calls()).toBeGreaterThanOrEqual(1));
    expect(prompts[0]).toBe(
      'PREV<(none yet)> NEW<User: question 1\nAssistant: answer 1\n\nUser: question 2\nAssistant: answer 2>',
    );
    memory.dispose();
  });
});
