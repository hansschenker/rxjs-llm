export type {
  ChatCompletion,
  ChatMessage,
  ChatModel,
  ChatOptions,
  StopReason,
  StreamEvent,
  ToolCall,
  ToolDefinition,
} from './types.js';

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
} from './errors.js';

export { collectCompletion, foldEvents } from './completion.js';

export {
  fetchStream,
  parseRetryAfter,
  type FetchStreamInit,
} from './transport/fetch-stream.js';
export { parseSse, type SseEvent } from './transport/sse.js';
export { parseNdjson } from './transport/ndjson.js';
export { fetchJson } from './transport/fetch-json.js';

export { anthropic, type AnthropicConfig } from './adapters/anthropic.js';
export { openai, type OpenAiConfig } from './adapters/openai.js';
export { ollama, type OllamaConfig } from './adapters/ollama.js';

export { retryWithBackoff, type RetryBackoffOptions } from './operators/retry-backoff.js';
export { streamTimeout, type StreamTimeoutOptions } from './operators/stream-timeout.js';
export { rateLimit, type RateLimitOptions } from './operators/rate-limit.js';

export {
  prompt,
  promptTemplate,
  renderTemplate,
  templateVars,
  type ExtractVars,
  type PromptFn,
  type PromptVars,
} from './prompt/template.js';

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
} from './prompt/messages.js';

export {
  asBullets,
  asJson,
  noJargon,
  type FormatInstruction,
  type JsonFormat,
} from './prompt/format.js';

export type { Chunk, Doc, TextChunk } from './index/types.js';
export {
  charEstimator,
  splitDocs,
  splitText,
  type SplitOptions,
  type Tokenizer,
} from './index/split.js';

export type {
  MetadataFilter,
  QueryMatch,
  VectorEntry,
  VectorStore,
} from './index/store/types.js';
export { memoryStore } from './index/store/memory.js';

export type { Embedder } from './index/embed/types.js';
export { openaiEmbedder, type OpenAiEmbedderConfig } from './index/embed/openai.js';
export { ollamaEmbedder, type OllamaEmbedderConfig } from './index/embed/ollama.js';
export {
  embedBatched,
  type EmbedBatchedOptions,
  type EmbeddedChunk,
} from './index/embed/batch.js';

export { textFileLoader, type TextFileLoaderOptions } from './index/loaders/text-file.js';
export { extractText, webLoader, type WebLoaderOptions } from './index/loaders/web.js';
export { jsonLoader, type JsonLoaderOptions } from './index/loaders/json.js';

export {
  retrieveContext,
  type RetrievedContext,
  type RetrieveOptions,
} from './index/retrieve.js';
export { ingest, toVectorEntry, upsertInto, type IngestOptions } from './index/ingest.js';

export {
  createMemory,
  type Memory,
  type MemoryOptions,
  type MemorySnapshot,
  type MemoryView,
  type Turn,
} from './memory/core.js';
export {
  fullView,
  tokenBudgetView,
  turnsToMessages,
  turnTokens,
  windowView,
} from './memory/views.js';
export {
  summaryView,
  type SummaryPrompt,
  type SummaryViewOptions,
} from './memory/summary.js';

export {
  executeToolCall,
  tool,
  toolRegistry,
  toToolDefinition,
  type Tool,
  type ToolContext,
  type ToolExecutionResult,
  type ToolSpec,
} from './agent/tool.js';
export { runAgent, type AgentOptions, type AgentOutcome } from './agent/loop.js';
export type { AgentEvent } from './agent/events.js';

export {
  chain,
  type ChainBuilder,
  type ChainOptions,
  type ChainRun,
  type RunnableChain,
} from './chain/chain.js';
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
} from './chain/stage.js';
export {
  collectorSink,
  consoleSink,
  traced,
  type CollectorSink,
  type TraceContext,
  type TraceEvent,
  type TraceSink,
} from './chain/trace.js';
export { collectText } from './chain/collect-text.js';
export type { ChainEvent } from './chain/events.js';
