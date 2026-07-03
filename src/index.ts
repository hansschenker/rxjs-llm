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
export { fetchJson } from './transport/fetch-json';

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

export type { Chunk, Doc, TextChunk } from './index/types';
export {
  charEstimator,
  splitDocs,
  splitText,
  type SplitOptions,
  type Tokenizer,
} from './index/split';

export type {
  MetadataFilter,
  QueryMatch,
  VectorEntry,
  VectorStore,
} from './index/store/types';
export { memoryStore } from './index/store/memory';

export type { Embedder } from './index/embed/types';
export { openaiEmbedder, type OpenAiEmbedderConfig } from './index/embed/openai';
export { ollamaEmbedder, type OllamaEmbedderConfig } from './index/embed/ollama';
export {
  embedBatched,
  type EmbedBatchedOptions,
  type EmbeddedChunk,
} from './index/embed/batch';

export { textFileLoader, type TextFileLoaderOptions } from './index/loaders/text-file';
export { extractText, webLoader, type WebLoaderOptions } from './index/loaders/web';
export { jsonLoader, type JsonLoaderOptions } from './index/loaders/json';

export {
  retrieveContext,
  type RetrievedContext,
  type RetrieveOptions,
} from './index/retrieve';
export { ingest, toVectorEntry, upsertInto, type IngestOptions } from './index/ingest';

export {
  chain,
  type ChainBuilder,
  type ChainOptions,
  type ChainRun,
  type RunnableChain,
} from './chain/chain';
export {
  stage,
  stageOf,
  stages,
  type EmitFn,
  type MergedPatch,
  type ParallelBranches,
  type StageErrorPolicy,
  type StageFn,
  type StageOptions,
} from './chain/stage';
export {
  collectorSink,
  consoleSink,
  traced,
  type CollectorSink,
  type TraceContext,
  type TraceEvent,
  type TraceSink,
} from './chain/trace';
export { collectText } from './chain/collect-text';
export type { ChainEvent } from './chain/events';
