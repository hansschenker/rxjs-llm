import {
  catchError,
  defaultIfEmpty,
  defer,
  filter,
  from,
  last,
  map,
  Observable,
  of,
  type ObservableInput,
} from 'rxjs';
import { z } from 'zod';
import { TimeoutError } from '../errors.js';
import { retryWithBackoff } from '../operators/retry-backoff.js';
import { streamTimeout } from '../operators/stream-timeout.js';
import type { ToolCall, ToolDefinition } from '../types.js';

export interface ToolContext {
  /** Fires when the tool execution is cancelled — pass it to fetch etc. */
  signal: AbortSignal;
}

/**
 * The registry-facing tool shape. `execute` takes `unknown` here; the
 * `tool()` factory below is the typed bridge — the loop only ever calls
 * execute with schema-validated data, which is what makes the cast in the
 * factory sound.
 */
export interface Tool {
  name: string;
  description?: string;
  inputSchema: z.ZodType;
  execute: (input: unknown, context: ToolContext) => ObservableInput<unknown>;
  /** Bounds the WHOLE execution (every emission gap included). */
  timeoutMs?: number;
  /** Retry attempts on execution failure (timeouts included). Default 0. */
  retries?: number;
}

export interface ToolSpec<Schema extends z.ZodType> {
  name: string;
  description?: string;
  input: Schema;
  execute: (input: z.infer<Schema>, context: ToolContext) => ObservableInput<unknown>;
  timeoutMs?: number;
  retries?: number;
}

/**
 * Zod-defined tools (decision D6.2, ADR-0024): the schema is the single
 * source of truth — it validates the model's arguments at runtime AND
 * derives the provider-facing JSON Schema (zod v4's native z.toJSONSchema;
 * no zod-to-json-schema dependency).
 */
export function tool<Schema extends z.ZodType>(spec: ToolSpec<Schema>): Tool {
  return {
    name: spec.name,
    ...(spec.description !== undefined && { description: spec.description }),
    inputSchema: spec.input,
    execute: spec.execute as Tool['execute'],
    ...(spec.timeoutMs !== undefined && { timeoutMs: spec.timeoutMs }),
    ...(spec.retries !== undefined && { retries: spec.retries }),
  };
}

/** Duplicate names are a configuration bug, not a runtime condition. */
export function toolRegistry(tools: readonly Tool[]): ReadonlyMap<string, Tool> {
  const registry = new Map<string, Tool>();
  for (const t of tools) {
    if (registry.has(t.name)) throw new RangeError(`duplicate tool name: ${t.name}`);
    registry.set(t.name, t);
  }
  return registry;
}

/** Provider-facing declaration; `$schema` is noise to an LLM API. */
export function toToolDefinition(t: Tool): ToolDefinition {
  const { $schema: _ignored, ...schema } = z.toJSONSchema(t.inputSchema) as Record<
    string,
    unknown
  >;
  return {
    name: t.name,
    ...(t.description !== undefined && { description: t.description }),
    inputSchema: schema,
  };
}

export interface ToolExecutionResult {
  content: string;
  isError: boolean;
}

/**
 * Run one tool call to a guaranteed result (decision D6.3, ADR-0027):
 * success, validation error, execution error, or timeout notice — this
 * Observable NEVER errors, so the loop never stalls waiting for a missing
 * tool_result. Failures are messages RETURNED TO THE MODEL (prefixed
 * `Error:`) so it can self-correct — the key robustness trick (D6.2).
 */
export function executeToolCall(
  registry: ReadonlyMap<string, Tool>,
  call: ToolCall,
): Observable<ToolExecutionResult> {
  return defer(() => {
    const target = registry.get(call.name);
    if (target === undefined) {
      return of(errorResult(
        `unknown tool '${call.name}'. Available tools: ${[...registry.keys()].join(', ')}`,
      ));
    }

    let raw: unknown;
    try {
      raw = call.args.trim() === '' ? {} : JSON.parse(call.args);
    } catch (cause) {
      return of(errorResult(
        `arguments for '${call.name}' are not valid JSON (${cause instanceof Error ? cause.message : String(cause)})`,
      ));
    }

    const parsed = target.inputSchema.safeParse(raw);
    if (!parsed.success) {
      return of(errorResult(`invalid arguments for '${call.name}': ${formatIssues(parsed.error)}`));
    }

    let execution = abortableExecute(target, parsed.data);
    if (target.timeoutMs !== undefined) {
      execution = execution.pipe(
        streamTimeout({
          firstByteMs: target.timeoutMs,
          idleMs: target.timeoutMs,
          provider: `tool:${target.name}`,
        }),
      );
    }
    let result = execution.pipe(
      defaultIfEmpty(undefined as unknown),
      last(),
      map((value): ToolExecutionResult => ({ content: serializeResult(value), isError: false })),
    );
    if (target.retries !== undefined && target.retries > 0) {
      result = result.pipe(
        retryWithBackoff({ maxRetries: target.retries, baseMs: 200, shouldRetry: () => true }),
      );
    }
    return result.pipe(
      catchError((error: unknown) =>
        of(
          error instanceof TimeoutError
            ? errorResult(`tool '${call.name}' timed out after ${target.timeoutMs}ms`)
            : errorResult(
                `tool '${call.name}' failed: ${error instanceof Error ? error.message : String(error)}`,
              ),
        ),
      ),
    );
  });
}

const ABORT_SWALLOWED: unique symbol = Symbol('rxjs-llm.agent.abort-swallowed');

/**
 * Wrap one execution with an AbortController wired to teardown. The
 * promise branch matters: a promise that rejects AFTER our own abort
 * would hit rxjs's closed-subscriber path and become an uncaught
 * exception — post-abort rejections are swallowed instead. Each
 * subscription gets a fresh controller, so retries re-execute cleanly.
 */
function abortableExecute(target: Tool, input: unknown): Observable<unknown> {
  return new Observable<unknown>((subscriber) => {
    const controller = new AbortController();
    let produced: ObservableInput<unknown>;
    try {
      produced = target.execute(input, { signal: controller.signal });
    } catch (error) {
      subscriber.error(error);
      return undefined;
    }
    const source =
      produced instanceof Promise
        ? from(
            produced.catch((error: unknown) => {
              if (controller.signal.aborted) return ABORT_SWALLOWED;
              throw error;
            }),
          )
        : from(produced);
    const subscription = source
      .pipe(filter((value) => value !== ABORT_SWALLOWED))
      .subscribe(subscriber);
    return () => {
      controller.abort();
      subscription.unsubscribe();
    };
  });
}

function errorResult(message: string): ToolExecutionResult {
  return { content: `Error: ${message}`, isError: true };
}

function formatIssues(error: z.ZodError): string {
  return error.issues
    .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
    .join('; ');
}

function serializeResult(value: unknown): string {
  if (typeof value === 'string') return value;
  if (value === undefined) return '';
  return JSON.stringify(value) ?? '';
}
