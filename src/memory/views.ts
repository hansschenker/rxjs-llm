import { map } from 'rxjs';
import { charEstimator, type Tokenizer } from '../index/split.js';
import type { ChatMessage } from '../types.js';
import type { MemoryView, Turn } from './core.js';

export function turnsToMessages(turns: readonly Turn[]): ChatMessage[] {
  return turns.flatMap((turn): ChatMessage[] => [
    { role: 'user', content: turn.user },
    { role: 'assistant', content: turn.assistant },
  ]);
}

/** The whole history, verbatim. */
export function fullView(): MemoryView {
  return map(turnsToMessages);
}

/** The last `n` turns (2n messages), verbatim. */
export function windowView(n: number): MemoryView {
  if (!Number.isInteger(n) || n < 1) throw new RangeError(`window must be ≥ 1, got ${n}`);
  return map((turns) => turnsToMessages(turns.slice(-n)));
}

export function turnTokens(turn: Turn, tokenizer: Tokenizer): number {
  return tokenizer.count(turn.user) + tokenizer.count(turn.assistant);
}

/**
 * Token budget is a view concern (decision D5.3, ADR-0021): walk newest-
 * first, keep whole turns while they fit, stop at the first that does not —
 * so eviction is oldest-first and turn-atomic, and the kept turns are
 * always a suffix of history. A single turn larger than the budget yields
 * an empty history rather than a split turn.
 */
export function tokenBudgetView(budget: number, tokenizer: Tokenizer = charEstimator): MemoryView {
  if (budget < 1) throw new RangeError(`budget must be ≥ 1, got ${budget}`);
  return map((turns) => {
    const kept: Turn[] = [];
    let used = 0;
    for (let i = turns.length - 1; i >= 0; i -= 1) {
      const cost = turnTokens(turns[i]!, tokenizer);
      if (used + cost > budget) break;
      kept.unshift(turns[i]!);
      used += cost;
    }
    return turnsToMessages(kept);
  });
}
