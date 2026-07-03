import { defer, from, Observable, type Subscriber } from 'rxjs';
import { collectCompletion } from '../../src/completion';
import type { ChatMessage, ChatModel, ChatOptions, StreamEvent } from '../../src/types';

/** One scripted model turn: tool calls, final text, or a hang (for teardown tests). */
export interface ScriptedTurn {
  text?: string;
  toolCalls?: { id: string; name: string; args: string }[];
  /** Emit message_start then stay open until torn down. */
  hang?: boolean;
}

export interface ScriptedModel {
  model: ChatModel;
  /** Messages passed to each stream() call, in order. */
  requests: ChatMessage[][];
  /** ChatOptions passed to each call. */
  requestOptions: (ChatOptions | undefined)[];
  /** Teardown flags, one per issued call (true once torn down). */
  tornDown: boolean[];
}

/**
 * A ChatModel that plays scripted turns as normalized StreamEvents — the
 * unit-level counterpart of the mock server's scenario DSL. Turn N answers
 * the Nth stream() call; running past the script throws (a test bug).
 */
export function scriptedModel(turns: ScriptedTurn[]): ScriptedModel {
  let call = 0;
  const requests: ChatMessage[][] = [];
  const requestOptions: (ChatOptions | undefined)[] = [];
  const tornDown: boolean[] = [];

  const stream = (messages: ChatMessage[], options?: ChatOptions): Observable<StreamEvent> =>
    defer(() => {
      const index = call;
      call += 1;
      const turn = turns[index];
      if (turn === undefined) {
        throw new Error(`scripted model exhausted: call ${index + 1} of ${turns.length}`);
      }
      requests.push(messages.map((m) => ({ ...m })));
      requestOptions.push(options);
      tornDown.push(false);

      if (turn.hang) {
        return new Observable<StreamEvent>((subscriber: Subscriber<StreamEvent>) => {
          subscriber.next({ type: 'message_start', model: 'scripted' });
          return () => {
            tornDown[index] = true;
          };
        });
      }
      return from(turnToEvents(turn));
    });

  return {
    model: {
      stream,
      complete: (messages, options) => stream(messages, options).pipe(collectCompletion()),
    },
    requests,
    requestOptions,
    tornDown,
  };
}

function turnToEvents(turn: ScriptedTurn): StreamEvent[] {
  const events: StreamEvent[] = [{ type: 'message_start', model: 'scripted' }];
  if (turn.text !== undefined && turn.text !== '') {
    const half = Math.ceil(turn.text.length / 2);
    events.push({ type: 'text_delta', text: turn.text.slice(0, half) });
    if (half < turn.text.length) {
      events.push({ type: 'text_delta', text: turn.text.slice(half) });
    }
  }
  for (const toolCall of turn.toolCalls ?? []) {
    events.push({ type: 'tool_call_delta', id: toolCall.id, name: toolCall.name, argsDelta: '' });
    const half = Math.ceil(toolCall.args.length / 2);
    events.push({ type: 'tool_call_delta', id: toolCall.id, argsDelta: toolCall.args.slice(0, half) });
    if (half < toolCall.args.length) {
      events.push({
        type: 'tool_call_delta',
        id: toolCall.id,
        argsDelta: toolCall.args.slice(half),
      });
    }
  }
  events.push({ type: 'usage', input: 10, output: 5 });
  events.push({
    type: 'message_stop',
    stopReason: (turn.toolCalls?.length ?? 0) > 0 ? 'tool_use' : 'end_turn',
  });
  return events;
}
