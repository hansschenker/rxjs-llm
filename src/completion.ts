import { map, toArray, type OperatorFunction } from 'rxjs';
import type { ChatCompletion, StreamEvent, ToolCall } from './types';

/**
 * Pure fold from an event sequence to a completion. Exported so the reduction
 * is testable without any Observable machinery.
 */
export function foldEvents(events: readonly StreamEvent[]): ChatCompletion {
  let model = '';
  let text = '';
  let thinking = '';
  let usage = { input: 0, output: 0 };
  let stopReason: ChatCompletion['stopReason'] = 'other';
  const toolOrder: string[] = [];
  const tools = new Map<string, { name: string; args: string }>();

  for (const event of events) {
    switch (event.type) {
      case 'message_start':
        model = event.model;
        break;
      case 'text_delta':
        text += event.text;
        break;
      case 'thinking_delta':
        thinking += event.text;
        break;
      case 'tool_call_delta': {
        let entry = tools.get(event.id);
        if (!entry) {
          entry = { name: '', args: '' };
          tools.set(event.id, entry);
          toolOrder.push(event.id);
        }
        if (event.name !== undefined) entry.name = event.name;
        entry.args += event.argsDelta;
        break;
      }
      case 'usage':
        usage = { input: event.input, output: event.output };
        break;
      case 'message_stop':
        stopReason = event.stopReason;
        break;
    }
  }

  const toolCalls: ToolCall[] = toolOrder.map((id) => {
    const entry = tools.get(id)!;
    return { id, name: entry.name, args: entry.args };
  });

  return { model, text, thinking, toolCalls, usage, stopReason };
}

/**
 * `complete()` = `stream()` + this operator. Buffering the full event list is
 * deliberate: a chat stream is thousands of small objects at most, and the
 * pure fold keeps every subscription's state trivially independent.
 */
export function collectCompletion(): OperatorFunction<StreamEvent, ChatCompletion> {
  return (source) => source.pipe(toArray(), map(foldEvents));
}
