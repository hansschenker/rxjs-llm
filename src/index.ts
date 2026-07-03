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

export {
  prompt,
  promptTemplate,
  renderTemplate,
  templateVars,
  type ExtractVars,
  type PromptFn,
  type PromptVars,
} from './prompt/template';

export {
  assistant,
  fewShot,
  messagePrompt,
  system,
  user,
  type AppliedMessages,
  type FewShotPair,
  type MessagePromptFn,
  type MessagePromptSpec,
} from './prompt/messages';

export {
  asBullets,
  asJson,
  noJargon,
  type FormatInstruction,
  type JsonFormat,
} from './prompt/format';

export {
  chain,
  type ChainBuilder,
  type ChainRun,
  type RunnableChain,
} from './chain/chain';
export { stage, type EmitFn } from './chain/stage';
export { collectText } from './chain/collect-text';
export type { ChainEvent } from './chain/events';
