import { describe, expect, expectTypeOf, it } from 'vitest';
import {
  assistant,
  fewShot,
  messagePrompt,
  system,
  user,
} from '../../src/prompt/messages';
import type { ChatMessage } from '../../src/types';

describe('builders', () => {
  it('construct single turns', () => {
    expect(system('be terse')).toEqual({ role: 'system', content: 'be terse' });
    expect(user('hi')).toEqual({ role: 'user', content: 'hi' });
    expect(assistant('hello')).toEqual({ role: 'assistant', content: 'hello' });
  });

  it('fewShot flattens pairs in order', () => {
    expect(fewShot([{ user: 'q1', assistant: 'a1' }, { user: 'q2', assistant: 'a2' }])).toEqual([
      { role: 'user', content: 'q1' },
      { role: 'assistant', content: 'a1' },
      { role: 'user', content: 'q2' },
      { role: 'assistant', content: 'a2' },
    ]);
  });
});

describe('messagePrompt (D2.1)', () => {
  const qa = messagePrompt({
    system: 'Answer only from the provided context about {topic}. Say "unknown" otherwise.',
    fewShot: [{ user: 'Q: capital of France?', assistant: 'Paris' }],
    user: 'Q: {question}\nContext: {context}',
  });

  it('assembles system + few-shot + user with placeholders interpolated in system AND user', () => {
    const messages = qa({ topic: 'geography', question: 'capital of Peru?', context: 'Lima is the capital.' });
    expect([...messages]).toEqual([
      { role: 'system', content: 'Answer only from the provided context about geography. Say "unknown" otherwise.' },
      { role: 'user', content: 'Q: capital of France?' },
      { role: 'assistant', content: 'Paris' },
      { role: 'user', content: 'Q: capital of Peru?\nContext: Lima is the capital.' },
    ]);
  });

  it('is a real ChatMessage[] — array behavior intact, withHistory invisible', () => {
    const messages = qa({ topic: 't', question: 'q', context: 'c' });
    expect(Array.isArray(messages)).toBe(true);
    expect(messages).toHaveLength(4);
    expect(Object.keys(messages)).toEqual(['0', '1', '2', '3']);
    expect(JSON.parse(JSON.stringify(messages))).toHaveLength(4);
  });

  it('withHistory splices between the few-shot block and the final user turn', () => {
    const history: ChatMessage[] = [
      { role: 'user', content: 'earlier question' },
      { role: 'assistant', content: 'earlier answer' },
    ];
    const messages = qa({ topic: 't', question: 'now', context: 'ctx' });
    const withHistory = messages.withHistory(history);
    expect(withHistory.map((m) => m.role)).toEqual([
      'system',
      'user', // few-shot q
      'assistant', // few-shot a
      'user', // history
      'assistant', // history
      'user', // the actual question — always last
    ]);
    expect(withHistory.at(-1)?.content).toBe('Q: now\nContext: ctx');
  });

  it('withHistory is pure — the applied messages are untouched, calls are independent', () => {
    const messages = qa({ topic: 't', question: 'q', context: 'c' });
    const a = messages.withHistory([user('h1')]);
    const b = messages.withHistory([user('h2')]);
    expect(messages).toHaveLength(4);
    expect(a).toHaveLength(5);
    expect(b).toHaveLength(5);
    expect(a[3]?.content).toBe('h1');
    expect(b[3]?.content).toBe('h2');
  });

  it('system and fewShot are optional; the slot then sits before the only user turn', () => {
    const bare = messagePrompt({ user: 'Just {x}' });
    const messages = bare({ x: 'this' });
    expect([...messages]).toEqual([{ role: 'user', content: 'Just this' }]);
    expect(bare({ x: 'this' }).withHistory([assistant('prior')])).toEqual([
      { role: 'assistant', content: 'prior' },
      { role: 'user', content: 'Just this' },
    ]);
  });

  it('type level: vars are the union of system and user placeholders, exact', () => {
    expectTypeOf(qa)
      .parameter(0)
      .toEqualTypeOf<{
        topic: string | number;
        question: string | number;
        context: string | number;
      }>();
    // @ts-expect-error — 'context' is missing
    qa({ topic: 't', question: 'q' });

    const fixed = messagePrompt({ user: 'no placeholders' });
    // @ts-expect-error — takes no argument
    fixed({ anything: 1 });
  });
});
