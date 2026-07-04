import {
  concatMap,
  EMPTY,
  expand,
  from,
  last,
  map,
  mergeMap,
  of,
  tap,
  toArray,
  type Observable,
} from 'rxjs';
import { dualChannel, type DualChannel } from '../chain/dual-channel.js';
import { collectCompletion } from '../completion.js';
import type {
  ChatCompletion,
  ChatMessage,
  ChatModel,
  ChatOptions,
  ToolCall,
} from '../types.js';
import type { AgentEvent } from './events.js';
import { executeToolCall, toolRegistry, toToolDefinition, type Tool } from './tool.js';

/**
 * The loop's answer, as a RESULT VARIANT (decision D6.1, ADR-0025):
 * exceeding the iteration budget is an answer, not a failure — the
 * transcript is intact, the caller decides what it means. Errors on
 * result$ are reserved for real failures (transport, provider).
 */
export type AgentOutcome =
  | {
      type: 'complete';
      text: string;
      completion: ChatCompletion;
      messages: ChatMessage[];
      iterations: number;
      usage: { input: number; output: number };
    }
  | {
      type: 'max_iterations';
      messages: ChatMessage[];
      iterations: number;
      usage: { input: number; output: number };
    };

export interface AgentOptions {
  tools?: readonly Tool[];
  messages: readonly ChatMessage[];
  /** Maximum model calls. Default 8. */
  maxIterations?: number;
  /** Parallel tool executions per iteration. Default 4. */
  toolConcurrency?: number;
  /** Per-call chat options (system, temperature, …); tools are managed here. */
  chatOptions?: Omit<ChatOptions, 'tools'>;
}

interface LoopState {
  messages: ChatMessage[];
  iteration: number;
  usage: { input: number; output: number };
  outcome?: AgentOutcome;
}

/**
 * The tool-call loop as an `expand()` recursion (decision D6.1, ADR-0025).
 * Each expansion: stream one model turn (deltas tapped to progress$, then
 * reduced — semantically Module 1's complete()), and either terminate with
 * the final text or execute the requested tools (mergeMap under the
 * concurrency cap — parallel tool calls are real) and recurse with the
 * results appended. Tool results append in CALL ORDER regardless of
 * completion order, so transcripts are deterministic. No ReAct scaffolding
 * anywhere: the provider reasons natively over tool_use events.
 *
 * `{ result$, progress$ }` is the chain contract verbatim (D6.4,
 * ADR-0026): one runAgent() call = one execution, outcome latched,
 * cancellation aborts the in-flight model call and every in-flight tool.
 */
export function runAgent(
  model: ChatModel,
  options: AgentOptions,
): DualChannel<AgentOutcome, AgentEvent> {
  const registry = toolRegistry(options.tools ?? []);
  const toolDefinitions = [...registry.values()].map(toToolDefinition);
  const maxIterations = options.maxIterations ?? 8;
  const toolConcurrency = options.toolConcurrency ?? 4;

  return dualChannel<AgentOutcome, AgentEvent>({
    terminal: {
      complete: () => ({ type: 'agent_complete' }),
      error: (error: unknown) => ({
        type: 'agent_failed',
        message: error instanceof Error ? error.message : String(error),
      }),
    },
    work: (emit) => {
      const step = (state: LoopState): Observable<LoopState> => {
        if (state.outcome !== undefined) return EMPTY; // terminal — stop expanding
        if (state.iteration >= maxIterations) {
          return of({
            ...state,
            outcome: {
              type: 'max_iterations' as const,
              messages: state.messages,
              iterations: state.iteration,
              usage: state.usage,
            },
          });
        }
        const iteration = state.iteration + 1;
        const callOptions: ChatOptions = { ...options.chatOptions };
        if (toolDefinitions.length > 0) callOptions.tools = toolDefinitions;

        return model.stream(state.messages, callOptions).pipe(
          tap((event) => emit({ type: 'model_event', iteration, event })),
          collectCompletion(),
          concatMap((completion) => {
            const usage = {
              input: state.usage.input + completion.usage.input,
              output: state.usage.output + completion.usage.output,
            };
            const assistant: ChatMessage = {
              role: 'assistant',
              content: completion.text,
              ...(completion.toolCalls.length > 0 && { toolCalls: completion.toolCalls }),
            };
            const messages = [...state.messages, assistant];

            if (completion.toolCalls.length === 0) {
              return of({
                messages,
                iteration,
                usage,
                outcome: {
                  type: 'complete' as const,
                  text: completion.text,
                  completion,
                  messages,
                  iterations: iteration,
                  usage,
                },
              });
            }
            return runTools(completion.toolCalls, iteration).pipe(
              map((toolMessages) => ({
                messages: [...messages, ...toolMessages],
                iteration,
                usage,
              })),
            );
          }),
        );
      };

      const runTools = (calls: ToolCall[], iteration: number): Observable<ChatMessage[]> =>
        from(calls.map((call, index) => ({ call, index }))).pipe(
          mergeMap(({ call, index }) => {
            emit({ type: 'tool_start', iteration, id: call.id, tool: call.name, args: call.args });
            return executeToolCall(registry, call).pipe(
              map(({ content, isError }) => {
                emit({
                  type: 'tool_result',
                  iteration,
                  id: call.id,
                  tool: call.name,
                  content,
                  isError,
                });
                const message: ChatMessage = { role: 'tool', content, toolCallId: call.id };
                return { index, message };
              }),
            );
          }, toolConcurrency),
          toArray(),
          map((indexed) =>
            indexed.sort((a, b) => a.index - b.index).map((entry) => entry.message),
          ),
        );

      const initial: LoopState = {
        messages: [...options.messages],
        iteration: 0,
        usage: { input: 0, output: 0 },
      };
      return of(initial).pipe(
        expand(step, 1),
        last(),
        map((state) => state.outcome!), // the last expanded state always carries the outcome
      );
    },
  });
}
