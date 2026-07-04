import { defer, Observable, type OperatorFunction } from 'rxjs';
import { collectCompletion } from '../completion.js';
import { ParseError, ProviderError } from '../errors.js';
import { fetchStream } from '../transport/fetch-stream.js';
import { parseSse, type SseEvent } from '../transport/sse.js';
import type {
  ChatMessage,
  ChatModel,
  ChatOptions,
  StopReason,
  StreamEvent,
} from '../types.js';

export interface AnthropicConfig {
  apiKey: string;
  /** Required — a reference implementation should not ship a default that rots. */
  model: string;
  baseUrl?: string;
  /** `anthropic-version` header. */
  version?: string;
  /** The Messages API requires max_tokens; used when a call doesn't set it. */
  maxTokens?: number;
  fetchFn?: typeof fetch;
}

const PROVIDER = 'anthropic';

/** Anthropic Messages API adapter. Emits the normalized StreamEvent union (ADR-0002). */
export function anthropic(config: AnthropicConfig): ChatModel {
  const baseUrl = config.baseUrl ?? 'https://api.anthropic.com';

  const stream = (messages: ChatMessage[], options: ChatOptions = {}): Observable<StreamEvent> =>
    defer(() => {
      const init: Parameters<typeof fetchStream>[1] = {
        headers: {
          'x-api-key': config.apiKey,
          'anthropic-version': config.version ?? '2023-06-01',
          'content-type': 'application/json',
          accept: 'text/event-stream',
        },
        body: JSON.stringify(buildRequestBody(config, messages, options)),
        provider: PROVIDER,
      };
      if (config.fetchFn) init.fetchFn = config.fetchFn;
      return fetchStream(`${baseUrl}/v1/messages`, init).pipe(parseSse(), mapAnthropicEvents());
    });

  return {
    stream,
    complete: (messages, options) => stream(messages, options).pipe(collectCompletion()),
  };
}

function buildRequestBody(
  config: AnthropicConfig,
  messages: ChatMessage[],
  options: ChatOptions,
): Record<string, unknown> {
  const systemParts = messages
    .filter((m) => m.role === 'system')
    .map((m) => m.content);
  if (options.system !== undefined) systemParts.unshift(options.system);

  const body: Record<string, unknown> = {
    model: options.model ?? config.model,
    max_tokens: options.maxTokens ?? config.maxTokens ?? 4096,
    messages: messages.filter((m) => m.role !== 'system').map(toAnthropicMessage),
    stream: true,
  };
  if (systemParts.length > 0) body['system'] = systemParts.join('\n\n');
  if (options.temperature !== undefined) body['temperature'] = options.temperature;
  if (options.stopSequences !== undefined) body['stop_sequences'] = options.stopSequences;
  if (options.tools !== undefined) {
    body['tools'] = options.tools.map((tool) => ({
      name: tool.name,
      ...(tool.description !== undefined && { description: tool.description }),
      input_schema: tool.inputSchema,
    }));
  }
  return body;
}

function toAnthropicMessage(message: ChatMessage): Record<string, unknown> {
  if (message.role === 'tool') {
    return {
      role: 'user',
      content: [
        {
          type: 'tool_result',
          tool_use_id: message.toolCallId ?? '',
          content: message.content,
        },
      ],
    };
  }
  if (message.role === 'assistant' && message.toolCalls !== undefined) {
    const blocks: Record<string, unknown>[] = [];
    if (message.content !== '') blocks.push({ type: 'text', text: message.content });
    for (const call of message.toolCalls) {
      blocks.push({
        type: 'tool_use',
        id: call.id,
        name: call.name,
        input: parseToolArgs(call.args, call.id),
      });
    }
    return { role: 'assistant', content: blocks };
  }
  return { role: message.role, content: message.content };
}

function parseToolArgs(args: string, id: string): unknown {
  if (args.trim() === '') return {};
  try {
    return JSON.parse(args);
  } catch (cause) {
    throw new ParseError(`tool call ${id} carries unparseable args: ${args.slice(0, 200)}`, {
      provider: PROVIDER,
      cause,
    });
  }
}

const STOP_REASONS: Record<string, StopReason> = {
  end_turn: 'end_turn',
  max_tokens: 'max_tokens',
  stop_sequence: 'stop_sequence',
  tool_use: 'tool_use',
};

// The slice of Anthropic's wire format we consume.
interface AnthropicPayload {
  type: string;
  message?: { model?: string; usage?: { input_tokens?: number } };
  index?: number;
  content_block?: { type: string; id?: string; name?: string };
  delta?: {
    type?: string;
    text?: string;
    thinking?: string;
    partial_json?: string;
    stop_reason?: string;
  };
  usage?: { output_tokens?: number };
  error?: { type?: string; message?: string };
}

const RETRYABLE_PROVIDER_CODES = new Set(['overloaded_error', 'api_error']);

/**
 * Anthropic SSE → StreamEvent. Stateful per subscription: tool_use blocks are
 * announced once with id+name (content_block_start), then their argument JSON
 * arrives as input_json_delta fragments addressed by block index — this map
 * re-attaches each fragment to its call id.
 */
function mapAnthropicEvents(): OperatorFunction<SseEvent, StreamEvent> {
  return (source) =>
    new Observable<StreamEvent>((subscriber) => {
      const toolBlocksByIndex = new Map<number, { id: string }>();
      let inputTokens = 0;
      let outputTokens = 0;
      let stopReason: StopReason = 'other';

      const subscription = source.subscribe({
        next: (sse) => {
          if (sse.event === 'ping') return;
          let payload: AnthropicPayload;
          try {
            payload = JSON.parse(sse.data) as AnthropicPayload;
          } catch (cause) {
            subscriber.error(
              new ParseError(`malformed JSON in "${sse.event}" event`, {
                provider: PROVIDER,
                cause,
              }),
            );
            return;
          }

          switch (payload.type) {
            case 'message_start':
              inputTokens = payload.message?.usage?.input_tokens ?? 0;
              subscriber.next({ type: 'message_start', model: payload.message?.model ?? '' });
              break;
            case 'content_block_start': {
              const block = payload.content_block;
              if (block?.type === 'tool_use' && payload.index !== undefined) {
                const id = block.id ?? '';
                toolBlocksByIndex.set(payload.index, { id });
                subscriber.next({
                  type: 'tool_call_delta',
                  id,
                  ...(block.name !== undefined && { name: block.name }),
                  argsDelta: '',
                });
              }
              break;
            }
            case 'content_block_delta': {
              const delta = payload.delta;
              if (delta?.type === 'text_delta' && delta.text !== undefined) {
                subscriber.next({ type: 'text_delta', text: delta.text });
              } else if (delta?.type === 'thinking_delta' && delta.thinking !== undefined) {
                subscriber.next({ type: 'thinking_delta', text: delta.thinking });
              } else if (delta?.type === 'input_json_delta' && payload.index !== undefined) {
                const block = toolBlocksByIndex.get(payload.index);
                if (block) {
                  subscriber.next({
                    type: 'tool_call_delta',
                    id: block.id,
                    argsDelta: delta.partial_json ?? '',
                  });
                }
              }
              break;
            }
            case 'message_delta':
              if (payload.delta?.stop_reason !== undefined) {
                stopReason = STOP_REASONS[payload.delta.stop_reason] ?? 'other';
              }
              outputTokens = payload.usage?.output_tokens ?? outputTokens;
              break;
            case 'message_stop':
              subscriber.next({ type: 'usage', input: inputTokens, output: outputTokens });
              subscriber.next({ type: 'message_stop', stopReason });
              break;
            case 'error': {
              const code = payload.error?.type;
              subscriber.error(
                new ProviderError(payload.error?.message ?? 'provider error', {
                  provider: PROVIDER,
                  ...(code !== undefined && { code }),
                  retryable: code !== undefined && RETRYABLE_PROVIDER_CODES.has(code),
                }),
              );
              break;
            }
            default:
              break; // content_block_stop and unknown events carry nothing we need
          }
        },
        error: (error: unknown) => subscriber.error(error),
        complete: () => subscriber.complete(),
      });

      return () => subscription.unsubscribe();
    });
}
