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
