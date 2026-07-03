import { defer, Observable, type OperatorFunction } from 'rxjs';
import { collectCompletion } from '../completion';
import { ParseError, ProviderError } from '../errors';
import { fetchStream } from '../transport/fetch-stream';
import { parseNdjson } from '../transport/ndjson';
import type {
  ChatMessage,
  ChatModel,
  ChatOptions,
  StopReason,
  StreamEvent,
} from '../types';

export interface OllamaConfig {
  model: string;
  /** Defaults to the local daemon. No API key — Ollama is unauthenticated. */
  baseUrl?: string;
  fetchFn?: typeof fetch;
}

const PROVIDER = 'ollama';

/**
 * Ollama /api/chat adapter. The wire format is NDJSON, not SSE — this adapter
 * is why the transport layer has two framing strategies (see parseNdjson).
 */
export function ollama(config: OllamaConfig): ChatModel {
  const baseUrl = config.baseUrl ?? 'http://localhost:11434';

  const stream = (messages: ChatMessage[], options: ChatOptions = {}): Observable<StreamEvent> =>
    defer(() => {
      const init: Parameters<typeof fetchStream>[1] = {
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(buildRequestBody(config, messages, options)),
        provider: PROVIDER,
      };
      if (config.fetchFn) init.fetchFn = config.fetchFn;
      return fetchStream(`${baseUrl}/api/chat`, init).pipe(parseNdjson(), mapOllamaEvents());
    });

  return {
    stream,
    complete: (messages, options) => stream(messages, options).pipe(collectCompletion()),
  };
}

function buildRequestBody(
  config: OllamaConfig,
  messages: ChatMessage[],
  options: ChatOptions,
): Record<string, unknown> {
  const mapped = messages.map(toOllamaMessage);
  if (options.system !== undefined) {
    mapped.unshift({ role: 'system', content: options.system });
  }
  const body: Record<string, unknown> = {
    model: options.model ?? config.model,
    messages: mapped,
    stream: true,
  };
  const runtimeOptions: Record<string, unknown> = {};
  if (options.temperature !== undefined) runtimeOptions['temperature'] = options.temperature;
  if (options.maxTokens !== undefined) runtimeOptions['num_predict'] = options.maxTokens;
  if (options.stopSequences !== undefined) runtimeOptions['stop'] = options.stopSequences;
  if (Object.keys(runtimeOptions).length > 0) body['options'] = runtimeOptions;
  if (options.tools !== undefined) {
    body['tools'] = options.tools.map((tool) => ({
      type: 'function',
      function: {
        name: tool.name,
        ...(tool.description !== undefined && { description: tool.description }),
        parameters: tool.inputSchema,
      },
    }));
  }
  return body;
}

function toOllamaMessage(message: ChatMessage): Record<string, unknown> {
  if (message.role === 'tool') {
    return { role: 'tool', content: message.content };
  }
  if (message.role === 'assistant' && message.toolCalls !== undefined) {
    return {
      role: 'assistant',
      content: message.content,
      tool_calls: message.toolCalls.map((call) => ({
        function: { name: call.name, arguments: parseArgsLeniently(call.args) },
      })),
    };
  }
  return { role: message.role, content: message.content };
}

/** Ollama wants arguments as an object; args from other providers are JSON strings. */
function parseArgsLeniently(args: string): unknown {
  try {
    return JSON.parse(args);
  } catch {
    return {};
  }
}

const STOP_REASONS: Record<string, StopReason> = {
  stop: 'end_turn',
  length: 'max_tokens',
};

// The slice of Ollama's NDJSON format we consume.
interface OllamaLine {
  model?: string;
  message?: {
    content?: string;
    thinking?: string;
    tool_calls?: { function?: { name?: string; arguments?: unknown } }[];
  };
  done?: boolean;
  done_reason?: string;
  prompt_eval_count?: number;
  eval_count?: number;
  error?: string;
}

/**
 * Ollama NDJSON → StreamEvent. Tool calls arrive whole (arguments as an
 * object, no id), so each becomes a single tool_call_delta with a synthetic
 * id and the full JSON as its one fragment.
 */
function mapOllamaEvents(): OperatorFunction<unknown, StreamEvent> {
  return (source) =>
    new Observable<StreamEvent>((subscriber) => {
      let started = false;
      let toolCallCount = 0;
      let sawToolCalls = false;

      const subscription = source.subscribe({
        next: (value) => {
          if (value === null || typeof value !== 'object') {
            subscriber.error(
              new ParseError(`expected an NDJSON object, got ${typeof value}`, {
                provider: PROVIDER,
              }),
            );
            return;
          }
          const line = value as OllamaLine;

          if (line.error !== undefined) {
            subscriber.error(new ProviderError(line.error, { provider: PROVIDER }));
            return;
          }

          if (!started) {
            started = true;
            subscriber.next({ type: 'message_start', model: line.model ?? '' });
          }

          const message = line.message;
          if (message?.thinking !== undefined && message.thinking !== '') {
            subscriber.next({ type: 'thinking_delta', text: message.thinking });
          }
          if (message?.content !== undefined && message.content !== '') {
            subscriber.next({ type: 'text_delta', text: message.content });
          }
          for (const call of message?.tool_calls ?? []) {
            sawToolCalls = true;
            toolCallCount += 1;
            subscriber.next({
              type: 'tool_call_delta',
              id: `ollama_call_${toolCallCount}`,
              name: call.function?.name ?? '',
              argsDelta: JSON.stringify(call.function?.arguments ?? {}),
            });
          }

          if (line.done === true) {
            subscriber.next({
              type: 'usage',
              input: line.prompt_eval_count ?? 0,
              output: line.eval_count ?? 0,
            });
            const stopReason = sawToolCalls
              ? 'tool_use'
              : (STOP_REASONS[line.done_reason ?? ''] ?? 'other');
            subscriber.next({ type: 'message_stop', stopReason });
          }
        },
        error: (error: unknown) => subscriber.error(error),
        complete: () => subscriber.complete(),
      });

      return () => subscription.unsubscribe();
    });
}
