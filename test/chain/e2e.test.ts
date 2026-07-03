import { firstValueFrom, map, of } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { anthropic } from '../../src/adapters/anthropic';
import { chain } from '../../src/chain/chain';
import type { ChainEvent } from '../../src/chain/events';
import { collectText } from '../../src/chain/collect-text';
import { stage, stages } from '../../src/chain/stage';
import { collectorSink } from '../../src/chain/trace';
import { retryWithBackoff } from '../../src/operators/retry-backoff';
import { promptTemplate } from '../../src/prompt/template';
import { user } from '../../src/prompt/messages';
import { startMockServer, type MockServer } from '../helpers/mock-server';

// Module 3's end-to-end proof: prompts (Module 2) feeding a real adapter
// (Module 1) inside a chain using every Phase — sequential stages, parallel
// fan-out, a conditional, retry composition, tracing, and the dual channels
// — over real HTTP against the mock server. No API keys.

let server: MockServer;

beforeAll(async () => {
  server = await startMockServer();
});

afterAll(async () => {
  await server.close();
});

const PAGES: Record<string, string> = {
  'https://rxjs.dev': 'Reactive Extensions Library for JavaScript.',
};

const summarize = promptTemplate('Summarize: {page}');
const qa = promptTemplate('Q: {question} | Context: {context}');

describe('Module 3 end-to-end', () => {
  it('runs the full pipeline: fetch → parallel(summary, keywords) → when(clarify) → answer', async () => {
    const sink = collectorSink();
    const model = anthropic({
      apiKey: 'test',
      model: 'mock-model',
      baseUrl: `${server.url}/anthropic`,
    });

    const pipeline = chain<{ url: string; question: string }>({ trace: sink }).pipe(
      stage('fetch', (ctx) => of({ page: PAGES[ctx.url] ?? '' })),
      stages.parallel({
        summary: (ctx, emit) =>
          model
            .stream([user(summarize({ page: ctx.page }))])
            .pipe(collectText(emit), map((summary) => ({ summary }))),
        keywords: (ctx, emit) =>
          model
            .stream([user(`Keywords for: ${ctx.page}`)])
            .pipe(collectText(emit), map((keywords) => ({ keywords }))),
      }),
      stage.when(
        'clarify',
        (ctx) => ctx.question.endsWith('?'),
        (ctx) => of({ clarified: ctx.question.replace('?', '').trim() }),
      ),
      stage('answer', (ctx, emit) =>
        model
          .stream([user(qa({ question: ctx.question, context: ctx.summary }))])
          .pipe(
            retryWithBackoff({ maxRetries: 2, baseMs: 1 }),
            collectText(emit),
            map((answer) => ({ answer })),
          ),
      ),
    );

    const { result$, progress$ } = pipeline.run({
      url: 'https://rxjs.dev',
      question: 'What is RxJS?',
    });
    const events: ChainEvent[] = [];
    progress$.subscribe((e) => events.push(e));
    const final = await firstValueFrom(result$);

    // The context accumulated every stage's contribution
    expect(final.page).toBe('Reactive Extensions Library for JavaScript.');
    expect(final.summary).toBe('echo: Summarize: Reactive Extensions Library for JavaScript.');
    expect(final.keywords).toBe('echo: Keywords for: Reactive Extensions Library for JavaScript.');
    expect(final.clarified).toBe('What is RxJS');
    expect(final.answer).toBe(`echo: Q: What is RxJS? | Context: ${final.summary}`);

    // Three model calls crossed the wire — no more, no fewer
    expect(server.requests.filter((u) => u === '/anthropic/v1/messages')).toHaveLength(3);

    // progress$: model events tagged by stage; exactly one terminal event
    const taggedStages = new Set(
      events
        .filter((e): e is Extract<ChainEvent, { type: 'stage_event' }> => e.type === 'stage_event')
        .map((e) => e.stage),
    );
    expect(taggedStages).toEqual(new Set(['summary', 'keywords', 'answer']));
    expect(events.filter((e) => e.type === 'run_complete')).toHaveLength(1);
    expect(events.at(-1)).toEqual({ type: 'run_complete' });

    // trace: one runId, all stages present, fetch strictly first,
    // parallel branches both start before either completes
    expect(new Set(sink.events.map((e) => e.runId)).size).toBe(1);
    const order = sink.events.map((e) => `${e.stage}:${e.type}`);
    expect(order[0]).toBe('fetch:stage_start');
    expect(order[1]).toBe('fetch:stage_complete');
    const idx = (entry: string) => order.indexOf(entry);
    expect(idx('summary:stage_start')).toBeLessThan(idx('summary:stage_complete'));
    expect(idx('keywords:stage_start')).toBeLessThan(idx('keywords:stage_complete'));
    expect(
      Math.max(idx('summary:stage_start'), idx('keywords:stage_start')),
    ).toBeLessThan(Math.min(idx('summary:stage_complete'), idx('keywords:stage_complete')));
    expect(idx('answer:stage_start')).toBeGreaterThan(idx('summary:stage_complete'));
    expect(order.at(-1)).toBe('answer:stage_complete');
    expect(order).toContain('clarify:stage_start'); // predicate was true
  });
});
