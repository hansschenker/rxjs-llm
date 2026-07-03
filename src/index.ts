export type {
  ChatCompletion,
  ChatMessage,
  ChatModel,
  ChatOptions,
  StopReason,
  StreamEvent,
  ToolCall,
  ToolDefinition,
} from './types';

export {
  HttpError,
  isRetryable,
  LlmError,
  ParseError,
  ProviderError,
  RateLimitError,
  TimeoutError,
  TransportError,
  type LlmErrorOptions,
} from './errors';

export { collectCompletion, foldEvents } from './completion';

export {
  fetchStream,
  parseRetryAfter,
  type FetchStreamInit,
} from './transport/fetch-stream';
export { parseSse, type SseEvent } from './transport/sse';
export { parseNdjson } from './transport/ndjson';

export { anthropic, type AnthropicConfig } from './adapters/anthropic';
export { openai, type OpenAiConfig } from './adapters/openai';
export { ollama, type OllamaConfig } from './adapters/ollama';

export { retryWithBackoff, type RetryBackoffOptions } from './operators/retry-backoff';
export { streamTimeout, type StreamTimeoutOptions } from './operators/stream-timeout';
export { rateLimit, type RateLimitOptions } from './operators/rate-limit';
