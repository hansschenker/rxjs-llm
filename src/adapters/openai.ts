import { defer, Observable, type OperatorFunction } from 'rxjs';
import { collectCompletion } from '../completion';
import { ParseError, ProviderError } from '../errors';
import { fetchStream } from '../transport/fetch-stream';
import { parseSse, type SseEvent } from '../transport/sse';
import type {
  ChatMessage,
  ChatModel,
  ChatOptions,
  StopReason,
  StreamEvent,
} from '../types';

export interface OpenAiConfig {
  apiKey: string;
  model: string;
  baseUrl?: string;
  fetchFn?: typeof fetch;
}

const PROVIDER = 'openai';

/** OpenAI Chat Completions adapter. Emits the normalized StreamEvent union (ADR-0002). */
export function openai(config: OpenAiConfig): ChatModel {
  const baseUrl = config.baseUrl ?? 'https://api.openai.com';

  const stream = (messages: ChatMessage[], options: ChatOptions = {}): Observable<StreamEvent> =>
    defer(() => {
      const init: Parameters<typeof fetchStream>[1] = {
        headers: {
          authorization: `Bearer ${config.apiKey}`,
          'content-type': 'application/json',
          accept: 'text/event-stream',
        },
        body: JSON.stringify(buildRequestBody(config, messages, options)),
        provider: PROVIDER,
      };
      if (config.fetchFn) init.fetchFn = config.fetchFn;
      return fetchStream(`${baseUrl}/v1/chat/completions`, init).pipe(
        parseSse(),
        mapOpenAiEvents(),
      );
    });

  return {
    stream,
    complete: (messages, options) => stream(messages, options).pipe(collectCompletion()),
  };
}

function buildRequestBody(
  config: OpenAiConfig,
  messages: ChatMessage[],
  options: ChatOptions,
): Record<string, unknown> {
  const mapped = messages.map(toOpenAiMessage);
  if (options.system !== undefined) {
    mapped.unshift({ role: 'system', content: options.system });
  }
  const body: Record<string, unknown> = {
    model: options.model ?? config.model,
    messages: mapped,
    stream: true,
    stream_options: { include_usage: true },
  };
  if (options.maxTokens !== undefined) body['max_tokens'] = options.maxTokens;
  if (options.temperature !== undefined) body['temperature'] = options.temperature;
  if (options.stopSequences !== undefined) body['stop'] = options.stopSequences;
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

function toOpenAiMessage(message: ChatMessage): Record<string, unknown> {
  if (message.role === 'tool') {
    return {
      role: 'tool',
      tool_call_id: message.toolCallId ?? '',
      content: message.content,
    };
  }
  if (message.role === 'assistant' && message.toolCalls !== undefined) {
    return {
      role: 'assistant',
      content: message.content === '' ? null : message.content,
      tool_calls: message.toolCalls.map((call) => ({
        id: call.id,
        type: 'function',
        function: { name: call.name, arguments: call.args },
      })),
    };
  }
  return { role: message.role, content: message.content };
}

const STOP_REASONS: Record<string, StopReason> = {
  stop: 'end_turn',
  length: 'max_tokens',
  tool_calls: 'tool_use',
};

// The slice of OpenAI's chunk format we consume.
interface OpenAiChunk {
  model?: string;
  choices?: {
    delta?: {
      content?: string | null;
      tool_calls?: {
        index: number;
        id?: string;
        function?: { name?: string; arguments?: string };
      }[];
    };
    finish_reason?: string | null;
  }[];
  usage?: { prompt_tokens?: number; completion_tokens?: number } | null;
  error?: { message?: string; type?: string; code?: string | null };
}

const RETRYABLE_PROVIDER_CODES = new Set(['server_error']);

/**
 * OpenAI SSE → StreamEvent. Stateful per subscription: tool calls arrive as
 * delta fragments addressed by array index (the id appears only on the first
 * fragment), and the stream ends with a `[DONE]` sentinel rather than a
 * structured stop event — usage and message_stop are emitted when it arrives.
 */
function mapOpenAiEvents(): OperatorFunction<SseEvent, StreamEvent> {
  return (source) =>
    new Observable<StreamEvent>((subscriber) => {
      const toolCallIdsByIndex = new Map<number, string>();
      let started = false;
      let usage = { input: 0, output: 0 };
      let stopReason: StopReason = 'other';

      const subscription = source.subscribe({
        next: (sse) => {
          if (sse.data === '[DONE]') {
            subscriber.next({ type: 'usage', ...usage });
            subscriber.next({ type: 'message_stop', stopReason });
            return;
          }

          let chunk: OpenAiChunk;
          try {
            chunk = JSON.parse(sse.data) as OpenAiChunk;
          } catch (cause) {
            subscriber.error(
              new ParseError('malformed JSON in stream chunk', { provider: PROVIDER, cause }),
            );
            return;
          }

          if (chunk.error !== undefined) {
            const code = chunk.error.code ?? chunk.error.type;
            subscriber.error(
              new ProviderError(chunk.error.message ?? 'provider error', {
                provider: PROVIDER,
                ...(code != null && { code }),
                retryable: code != null && RETRYABLE_PROVIDER_CODES.has(code),
              }),
            );
            return;
          }

          if (!started) {
            started = true;
            subscriber.next({ type: 'message_start', model: chunk.model ?? '' });
          }

          if (chunk.usage != null) {
            usage = {
              input: chunk.usage.prompt_tokens ?? 0,
              output: chunk.usage.completion_tokens ?? 0,
            };
          }

          const choice = chunk.choices?.[0];
          if (choice === undefined) return;

          if (choice.finish_reason != null) {
            stopReason = STOP_REASONS[choice.finish_reason] ?? 'other';
          }

          const delta = choice.delta;
          if (delta?.content != null && delta.content !== '') {
            subscriber.next({ type: 'text_delta', text: delta.content });
          }
          for (const fragment of delta?.tool_calls ?? []) {
            if (fragment.id !== undefined) toolCallIdsByIndex.set(fragment.index, fragment.id);
            const id = toolCallIdsByIndex.get(fragment.index);
            if (id === undefined) continue; // fragment for a call we never saw announced
            const name = fragment.function?.name;
            subscriber.next({
              type: 'tool_call_delta',
              id,
              ...(name !== undefined && { name }),
              argsDelta: fragment.function?.arguments ?? '',
            });
          }
        },
        error: (error: unknown) => subscriber.error(error),
        complete: () => subscriber.complete(),
      });

      return () => subscription.unsubscribe();
    });
}
