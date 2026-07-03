import type { Observable } from 'rxjs';

/** A single conversational turn. `tool` messages carry results back to the model. */
export interface ChatMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string;
  /** Present on `tool` messages: links the result to the call that requested it. */
  toolCallId?: string;
  /** Present on `assistant` messages that requested tool invocations. */
  toolCalls?: ToolCall[];
}

/** A fully-assembled tool call: `args` is the raw JSON string as sent by the provider. */
export interface ToolCall {
  id: string;
  name: string;
  args: string;
}

/** Provider-facing tool declaration. `inputSchema` is JSON Schema. */
export interface ToolDefinition {
  name: string;
  description?: string;
  inputSchema: Record<string, unknown>;
}

export type StopReason =
  | 'end_turn'
  | 'max_tokens'
  | 'stop_sequence'
  | 'tool_use'
  | 'other';

/**
 * The normalized event taxonomy every adapter emits (decision D2).
 * Modules 3–6 consume only this union — never provider wire formats.
 */
export type StreamEvent =
  | { type: 'message_start'; model: string }
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_delta'; id: string; name?: string; argsDelta: string }
  | { type: 'usage'; input: number; output: number }
  | { type: 'message_stop'; stopReason: StopReason };

export interface ChatOptions {
  /** Overrides the adapter's default model for this call. */
  model?: string;
  maxTokens?: number;
  temperature?: number;
  /** System prompt. Adapters place it wherever their wire format wants it. */
  system?: string;
  stopSequences?: string[];
  tools?: ToolDefinition[];
}

/** The reduced form of a stream: what `complete()` resolves to. */
export interface ChatCompletion {
  model: string;
  text: string;
  thinking: string;
  toolCalls: ToolCall[];
  usage: { input: number; output: number };
  stopReason: StopReason;
}

/**
 * The uniform model interface (Module 1's reason to exist).
 *
 * Contract — enforced by law tests, not convention:
 * - cold: each subscribe issues exactly one HTTP request
 * - lazy: no fetch before subscribe, nothing emits in the subscribe call frame
 * - unicast: subscribers never share a request
 * - teardown-complete: unsubscribe aborts the underlying fetch
 * - cancellation is silent teardown; it never surfaces on the error channel
 */
export interface ChatModel {
  stream(messages: ChatMessage[], options?: ChatOptions): Observable<StreamEvent>;
  complete(messages: ChatMessage[], options?: ChatOptions): Observable<ChatCompletion>;
}
