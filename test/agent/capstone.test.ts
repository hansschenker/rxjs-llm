import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { defer, finalize, firstValueFrom, last, map, of } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { anthropic } from '../../src/adapters/anthropic';
import { runAgent } from '../../src/agent/loop';
import { tool } from '../../src/agent/tool';
import { chain } from '../../src/chain/chain';
import type { ChainEvent } from '../../src/chain/events';
import { stage } from '../../src/chain/stage';
import { collectorSink } from '../../src/chain/trace';
import { ingest } from '../../src/index/ingest';
import { textFileLoader } from '../../src/index/loaders/text-file';
import { retrieveContext } from '../../src/index/retrieve';
import { memoryStore } from '../../src/index/store/memory';
import type { Embedder } from '../../src/index/embed/types';
import { createMemory } from '../../src/memory/core';
import { windowView } from '../../src/memory/views';
import { promptTemplate } from '../../src/prompt/template';
import { user } from '../../src/prompt/messages';
import { startMockServer, type MockServer } from '../helpers/mock-server';

// THE CAPSTONE (Module 6, Phase 4) — the repo's definition of done:
// retrieve (Module 4) → agent with tools (Module 6) → memory record
// (Module 5), composed as a chain (Module 3) with a Module 2 prompt,
// against the mock provider server through the real Module 1 adapter.
// Six modules, one pipeline, real HTTP, no API keys.

const corpusDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'fixtures', 'corpus');

function bagOfWords(): Embedder {
  const embedOne = (text: string): Float32Array => {
    const vector = new Float32Array(128);
    for (const word of text.toLowerCase().split(/[^a-zä-ü0-9]+/)) {
      if (word.length < 3) continue;
      let hash = 5381;
      for (let i = 0; i < word.length; i += 1) hash = (hash * 33 + word.charCodeAt(i)) >>> 0;
      const bucket = hash % 128;
      vector[bucket] = (vector[bucket] ?? 0) + 1;
    }
    return vector;
  };
  return { embed: (texts) => of(texts.map(embedOne)) };
}

let server: MockServer;

beforeAll(async () => {
  server = await startMockServer();
});

afterAll(async () => {
  await server.close();
});

const FINAL_ANSWER =
  'Espresso is brewed at roughly nine bars of pressure — see espresso.md.';

describe('the capstone: all six modules, one pipeline', () => {
  it('retrieve → agent (with a retrieval tool) → memory, as a chain over real HTTP', async () => {
    // ---- Module 4: ingest the corpus, build the retrieval plumbing
    const store = memoryStore();
    const embedder = bagOfWords();
    await firstValueFrom(
      ingest(textFileLoader(corpusDir), {
        split: { maxTokens: 60, overlap: 8 },
        embedder,
        store,
      }).pipe(last()),
    );

    // ---- Module 5: conversation memory
    const memory = createMemory({ view: windowView(10) });

    // ---- Module 1: the real adapter, wired to the mock server
    const model = anthropic({
      apiKey: 'test',
      model: 'mock-model',
      baseUrl: `${server.url}/anthropic`,
    });

    // ---- Module 6: a tool whose body IS Module 4's retriever
    const searchDocs = tool({
      name: 'search_docs',
      description: 'Search the ingested knowledge base',
      input: z.object({ query: z.string() }),
      execute: ({ query }) =>
        of(query).pipe(
          retrieveContext(store, embedder, 3),
          map((retrieved) => retrieved.context),
        ),
      timeoutMs: 5_000,
    });

    // ---- the scripted conversation: the model asks for the tool, then answers
    server.script([
      { toolCalls: [{ id: 'call_1', name: 'search_docs', args: { query: 'espresso pressure' } }] },
      { text: FINAL_ANSWER },
    ]);

    // ---- Module 2: the question prompt
    const ask = promptTemplate('Answer using the knowledge base: {question}');

    // ---- Module 3: the chain — an agent is just a stage (D6.4)
    const sink = collectorSink();
    const question = 'How much pressure does espresso brewing use?';
    const pipeline = chain<{ question: string }>({ trace: sink }).pipe(
      stage('agent', (ctx, emit) =>
        defer(() => {
          const agent = runAgent(model, {
            tools: [searchDocs],
            messages: [user(ask({ question: ctx.question }))],
            maxIterations: 5,
            toolConcurrency: 2,
          });
          // the agent's model deltas ARE stage events — no adapter needed
          const forward = agent.progress$.subscribe((event) => {
            if (event.type === 'model_event') emit(event.event);
          });
          return agent.result$.pipe(
            finalize(() => forward.unsubscribe()),
            map((outcome) => ({
              answer: outcome.type === 'complete' ? outcome.text : '(iteration budget exceeded)',
              iterations: outcome.iterations,
            })),
          );
        }),
      ),
      stage('remember', (ctx) => {
        memory.record({ user: ctx.question, assistant: ctx.answer });
        return of({ remembered: true });
      }),
    );

    const { result$, progress$ } = pipeline.run({ question });
    const chainEvents: ChainEvent[] = [];
    progress$.subscribe((e) => chainEvents.push(e));
    const final = await firstValueFrom(result$);

    // ---- the answer came through the whole stack
    expect(final.answer).toBe(FINAL_ANSWER);
    expect(final.iterations).toBe(2);
    expect(final.remembered).toBe(true);

    // ---- exactly two model calls crossed the wire
    const modelRequests = server.requests.filter((u) => u === '/anthropic/v1/messages');
    expect(modelRequests).toHaveLength(2);

    // ---- the second request carried the tool result — and that result is
    //      Module 4's retrieval: the espresso chunk, source-attributed
    const secondBody = server.bodies.at(-1) as {
      messages: { role: string; content: unknown }[];
    };
    const toolResultBlock = secondBody.messages
      .flatMap((m) => (Array.isArray(m.content) ? m.content : []))
      .find((block: { type?: string }) => block.type === 'tool_result') as
      | { tool_use_id: string; content: string }
      | undefined;
    expect(toolResultBlock?.tool_use_id).toBe('call_1');
    expect(toolResultBlock?.content).toContain('[source: espresso.md]');
    expect(toolResultBlock?.content).toContain('nine bars');

    // ---- Module 5 recorded the exchange
    let history: unknown[] = [];
    memory.view().subscribe((m) => (history = m));
    expect(history).toEqual([
      { role: 'user', content: question },
      { role: 'assistant', content: FINAL_ANSWER },
    ]);

    // ---- the chain's progress carried the agent's deltas (D6.4 composition)
    const streamed = chainEvents.filter(
      (e): e is Extract<ChainEvent, { type: 'stage_event' }> => e.type === 'stage_event',
    );
    expect(streamed.filter((e) => e.event.type === 'message_start')).toHaveLength(2);
    const text = streamed
      .filter((e) => e.event.type === 'text_delta')
      .map((e) => (e.event as { text: string }).text)
      .join('');
    expect(text).toBe(FINAL_ANSWER);
    expect(chainEvents.at(-1)).toEqual({ type: 'run_complete' });

    // ---- and the trace saw both stages, in order, under one correlation id
    expect(sink.events.map((e) => `${e.stage}:${e.type}`)).toEqual([
      'agent:stage_start',
      'agent:stage_complete',
      'remember:stage_start',
      'remember:stage_complete',
    ]);
    expect(new Set(sink.events.map((e) => e.runId)).size).toBe(1);

    memory.dispose();
  });
});
