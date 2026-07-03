import { firstValueFrom, Observable, of, timer, map as rxMap } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import {
  executeToolCall,
  tool,
  toolRegistry,
  toToolDefinition,
  type ToolContext,
} from '../../src/agent/tool';
import type { ToolCall } from '../../src/types';

const call = (name: string, args: string, id = 'call_1'): ToolCall => ({ id, name, args });

const weather = tool({
  name: 'get_weather',
  description: 'Current weather for a city',
  input: z.object({ city: z.string(), units: z.enum(['c', 'f']).optional() }),
  execute: ({ city }) => of({ city, forecast: 'rainy', temp: 14 }),
});

describe('tool() + toToolDefinition (D6.2)', () => {
  it('derives the provider JSON Schema from the zod schema — no $schema noise', () => {
    const definition = toToolDefinition(weather);
    expect(definition.name).toBe('get_weather');
    expect(definition.description).toBe('Current weather for a city');
    expect(definition.inputSchema['$schema']).toBeUndefined();
    expect(definition.inputSchema).toMatchObject({
      type: 'object',
      required: ['city'],
      properties: {
        city: { type: 'string' },
        units: { enum: ['c', 'f'] },
      },
    });
  });

  it('the registry rejects duplicate names loudly', () => {
    expect(() => toolRegistry([weather, weather])).toThrow(/duplicate tool name/);
  });
});

describe('executeToolCall — validation round-trip (D6.2)', () => {
  const registry = toolRegistry([weather]);

  it('valid args execute and serialize the result as JSON', async () => {
    const result = await firstValueFrom(
      executeToolCall(registry, call('get_weather', '{"city":"Rapperswil"}')),
    );
    expect(result.isError).toBe(false);
    expect(JSON.parse(result.content)).toEqual({ city: 'Rapperswil', forecast: 'rainy', temp: 14 });
  });

  it('string results pass through unserialized; empty completions become empty strings', async () => {
    const registry2 = toolRegistry([
      tool({ name: 'echo', input: z.object({}), execute: () => of('plain text') }),
      tool({ name: 'silent', input: z.object({}), execute: () => of() }),
    ]);
    expect((await firstValueFrom(executeToolCall(registry2, call('echo', '{}')))).content).toBe(
      'plain text',
    );
    expect((await firstValueFrom(executeToolCall(registry2, call('silent', '{}')))).content).toBe(
      '',
    );
  });

  it('invalid arguments produce a self-correction message, not a thrown error', async () => {
    const result = await firstValueFrom(
      executeToolCall(registry, call('get_weather', '{"city":42,"units":"kelvin"}')),
    );
    expect(result.isError).toBe(true);
    expect(result.content).toMatch(/^Error: invalid arguments for 'get_weather'/);
    expect(result.content).toContain('city:');
    expect(result.content).toContain('units:');
  });

  it('malformed JSON args produce a self-correction message', async () => {
    const result = await firstValueFrom(executeToolCall(registry, call('get_weather', '{city:')));
    expect(result.isError).toBe(true);
    expect(result.content).toMatch(/^Error: arguments for 'get_weather' are not valid JSON/);
  });

  it('empty args parse as an empty object (providers send "" for no-arg tools)', async () => {
    const registry2 = toolRegistry([
      tool({ name: 'ping', input: z.object({}), execute: () => of('pong') }),
    ]);
    const result = await firstValueFrom(executeToolCall(registry2, call('ping', '')));
    expect(result).toEqual({ content: 'pong', isError: false });
  });

  it('an unknown tool names the available ones', async () => {
    const result = await firstValueFrom(executeToolCall(registry, call('get_wether', '{}')));
    expect(result.isError).toBe(true);
    expect(result.content).toContain("unknown tool 'get_wether'");
    expect(result.content).toContain('get_weather');
  });
});

describe('executeToolCall — execution safety (D6.3)', () => {
  it('execution errors become tool results, never stream errors', async () => {
    const registry = toolRegistry([
      tool({
        name: 'boom',
        input: z.object({}),
        execute: () => {
          throw new Error('kaput');
        },
      }),
    ]);
    const result = await firstValueFrom(executeToolCall(registry, call('boom', '{}')));
    expect(result).toEqual({ content: "Error: tool 'boom' failed: kaput", isError: true });
  });

  it('rejected promises become tool results too', async () => {
    const registry = toolRegistry([
      tool({
        name: 'rejects',
        input: z.object({}),
        execute: () => Promise.reject(new Error('no network')),
      }),
    ]);
    const result = await firstValueFrom(executeToolCall(registry, call('rejects', '{}')));
    expect(result.content).toBe("Error: tool 'rejects' failed: no network");
  });

  it('a hung tool times out into a notice — the loop can never stall', async () => {
    const registry = toolRegistry([
      tool({
        name: 'hang',
        input: z.object({}),
        execute: () => new Observable<never>(() => undefined),
        timeoutMs: 20,
      }),
    ]);
    const result = await firstValueFrom(executeToolCall(registry, call('hang', '{}')));
    expect(result).toEqual({
      content: "Error: tool 'hang' timed out after 20ms",
      isError: true,
    });
  });

  it('retries re-execute with a fresh AbortController and can succeed', async () => {
    let attempts = 0;
    const signals: AbortSignal[] = [];
    const registry = toolRegistry([
      tool({
        name: 'flaky',
        input: z.object({}),
        execute: (_input, context: ToolContext) => {
          attempts += 1;
          signals.push(context.signal);
          return attempts < 3
            ? timer(5).pipe(
                rxMap((): string => {
                  throw new Error('flaky');
                }),
              )
            : of('finally');
        },
        retries: 3,
      }),
    ]);
    const result = await firstValueFrom(executeToolCall(registry, call('flaky', '{}')));
    expect(result).toEqual({ content: 'finally', isError: false });
    expect(attempts).toBe(3);
    expect(new Set(signals).size).toBe(3); // one controller per attempt
  });

  it('cancellation aborts the signal, and a post-abort rejection never escapes', async () => {
    let capturedSignal: AbortSignal | undefined;
    const registry = toolRegistry([
      tool({
        name: 'slow_fetch',
        input: z.object({}),
        execute: (_input, { signal }) => {
          capturedSignal = signal;
          return new Promise((_resolve, reject) => {
            signal.addEventListener('abort', () =>
              reject(new DOMException('The operation was aborted.', 'AbortError')),
            );
          });
        },
      }),
    ]);
    const errors: unknown[] = [];
    const subscription = executeToolCall(registry, call('slow_fetch', '{}')).subscribe({
      error: (e) => errors.push(e),
    });
    await new Promise((r) => setTimeout(r, 5));
    subscription.unsubscribe();

    expect(capturedSignal?.aborted).toBe(true);
    await new Promise((r) => setTimeout(r, 10)); // room for a stray unhandled rejection
    expect(errors).toHaveLength(0); // silent teardown — and no uncaught throw
  });

  it('typed execute: the schema type flows into the handler', async () => {
    const typed = tool({
      name: 'add',
      input: z.object({ a: z.number(), b: z.number() }),
      // a and b are number here — a type error otherwise
      execute: ({ a, b }) => of(a + b),
    });
    const result = await firstValueFrom(
      executeToolCall(toolRegistry([typed]), call('add', '{"a":2,"b":3}')),
    );
    expect(result.content).toBe('5');
  });
});
