import type { ChatMessage } from '../types';
import { renderTemplate, type ExtractVars, type PromptVars } from './template';

/** Single-turn builders — trivial, but they read well in chains and tests. */
export const system = (content: string): ChatMessage => ({ role: 'system', content });
export const user = (content: string): ChatMessage => ({ role: 'user', content });
export const assistant = (content: string): ChatMessage => ({ role: 'assistant', content });

export interface FewShotPair {
  user: string;
  assistant: string;
}

/** Few-shot pairs flattened to alternating user/assistant messages. */
export function fewShot(pairs: readonly FewShotPair[]): ChatMessage[] {
  return pairs.flatMap((pair) => [user(pair.user), assistant(pair.assistant)]);
}

/**
 * The applied message prompt: a real ChatMessage[] (usable directly by
 * ChatModel.stream) carrying one extra non-enumerable method. The history
 * slot sits between the few-shot block and the final user turn — the shape
 * Module 3's examples assume and Module 5's memory consumes (ADR-0008).
 */
export interface AppliedMessages extends Array<ChatMessage> {
  /** Pure: returns a NEW plain array with `history` spliced into the slot. */
  withHistory(history: readonly ChatMessage[]): ChatMessage[];
}

export type MessagePromptFn<V extends string> = [V] extends [never]
  ? () => AppliedMessages
  : (vars: PromptVars<V>) => AppliedMessages;

export interface MessagePromptSpec<S extends string, U extends string> {
  /** Optional system turn; placeholders allowed. */
  system?: S;
  /** Literal examples — deliberately NOT templated (examples are static). */
  fewShot?: readonly FewShotPair[];
  /** The final user turn; placeholders allowed. */
  user: U;
}

/**
 * Message-prompt form (decision D2.1): a full ChatMessage[] with system,
 * few-shot pairs, a history slot, and the user turn. Placeholder keys are
 * the union of the system and user templates', checked at compile time
 * exactly like promptTemplate.
 */
export function messagePrompt<U extends string, S extends string = ''>(
  spec: MessagePromptSpec<S, U>,
): MessagePromptFn<ExtractVars<S> | ExtractVars<U>> {
  const fn = (vars?: Record<string, string | number>) => {
    const v = vars ?? {};
    const messages: ChatMessage[] = [];
    if (spec.system !== undefined) messages.push(system(renderTemplate(spec.system, v)));
    messages.push(...fewShot(spec.fewShot ?? []));
    const historySlot = messages.length;
    messages.push(user(renderTemplate(spec.user, v)));
    return makeApplied(messages, historySlot);
  };
  return fn as MessagePromptFn<ExtractVars<S> | ExtractVars<U>>;
}

function makeApplied(messages: ChatMessage[], slot: number): AppliedMessages {
  const applied = messages.slice() as AppliedMessages;
  Object.defineProperty(applied, 'withHistory', {
    value: (history: readonly ChatMessage[]): ChatMessage[] => [
      ...messages.slice(0, slot),
      ...history,
      ...messages.slice(slot),
    ],
    enumerable: false, // stays invisible to iteration, spread, and JSON
  });
  return applied;
}
