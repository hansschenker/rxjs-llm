import { firstValueFrom, Observable, of, toArray } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { describe, expect, it, vi } from 'vitest';
import { HttpError, ParseError } from '../../src/errors';
import { embedBatched, type EmbeddedChunk } from '../../src/index/embed/batch';
import { ollamaEmbedder } from '../../src/index/embed/ollama';
import { openaiEmbedder } from '../../src/index/embed/openai';
import type { Embedder } from '../../src/index/embed/types';
import type { Chunk } from '../../src/index/types';
import { mockFetch } from '../helpers/mock-fetch';

const chunk = (id: string, text: string): Chunk => ({
  id,
  docId: 'doc',
  index: 0,
  text,
  start: 0,
  end: text.length,
  metadata: {},
});

function openaiResponse(count: number, dims = 3): string {
  return JSON.stringify({
    data: Array.from({ length: count }, (_, index) => ({
      index,
      embedding: Array.from({ length: dims }, (_, d) => index + d / 10),
    })),
    usage: { prompt_tokens: 5, total_tokens: 5 },
  });
}

describe('openaiEmbedder', () => {
  it('embeds texts index-aligned and typed as Float32Array', async () => {
    const { fetchFn, calls } = mockFetch([openaiResponse(2)]);
    const embedder = openaiEmbedder({ apiKey: 'sk', model: 'text-embedding-3-small', fetchFn });
    const vectors = await firstValueFrom(embedder.embed(['a', 'b']));

    expect(vectors).toHaveLength(2);
    expect(vectors[0]).toBeInstanceOf(Float32Array);
    expect(Array.from(vectors[0]!)).toEqual(Array.from(Float32Array.from([0, 0.1, 0.2])));
    expect(Array.from(vectors[1]!)).toEqual(Array.from(Float32Array.from([1, 1.1, 1.2])));
    expect(calls[0]?.url).toBe('https://api.openai.com/v1/embeddings');
    expect(calls[0]?.bodyJson()).toEqual({ model: 'text-embedding-3-small', input: ['a', 'b'] });
    expect(calls[0]?.init.headers).toMatchObject({ authorization: 'Bearer sk' });
  });

  it('is lazy and cold: one request per subscribe, none before', async () => {
    const { fetchFn, calls } = mockFetch([openaiResponse(1)]);
    const embed$ = openaiEmbedder({ apiKey: 'k', model: 'm', fetchFn }).embed(['x']);
    expect(calls).toHaveLength(0);
    await firstValueFrom(embed$);
    await firstValueFrom(embed$);
    expect(calls).toHaveLength(2);
  });

  it('unsubscribe aborts the request silently', async () => {
    const { fetchFn, calls } = mockFetch([openaiResponse(1)], { hang: true });
    const errors: unknown[] = [];
    const subscription = openaiEmbedder({ apiKey: 'k', model: 'm', fetchFn })
      .embed(['x'])
      .subscribe({ error: (e) => errors.push(e) });
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    subscription.unsubscribe();
    expect(calls[0]?.init.signal?.aborted).toBe(true);
    await new Promise((r) => setTimeout(r, 0));
    expect(errors).toHaveLength(0);
  });

  it('maps HTTP errors through the Module 1 taxonomy', async () => {
    const { fetchFn } = mockFetch([], { status: 500, errorBody: 'oops' });
    const error = await firstValueFrom(
      openaiEmbedder({ apiKey: 'k', model: 'm', fetchFn }).embed(['x']),
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(HttpError);
    expect((error as HttpError).provider).toBe('openai');
  });

  it('rejects a count mismatch as ParseError', async () => {
    const { fetchFn } = mockFetch([openaiResponse(1)]);
    const error = await firstValueFrom(
      openaiEmbedder({ apiKey: 'k', model: 'm', fetchFn }).embed(['a', 'b']),
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ParseError);
  });
});

describe('ollamaEmbedder', () => {
  it('embeds via /api/embed with no auth', async () => {
    const { fetchFn, calls } = mockFetch([JSON.stringify({ embeddings: [[1, 0], [0, 1]] })]);
    const embedder = ollamaEmbedder({ model: 'nomic-embed-text', fetchFn });
    const vectors = await firstValueFrom(embedder.embed(['a', 'b']));
    expect(Array.from(vectors[0]!)).toEqual([1, 0]);
    expect(calls[0]?.url).toBe('http://localhost:11434/api/embed');
    expect(calls[0]?.init.headers).toEqual({ 'content-type': 'application/json' });
  });
});

describe('embedBatched', () => {
  function recordingEmbedder(): { embedder: Embedder; batches: string[][] } {
    const batches: string[][] = [];
    return {
      batches,
      embedder: {
        embed: (texts) => {
          batches.push([...texts]);
          return of(texts.map((_, i) => Float32Array.from([batches.length, i])));
        },
      },
    };
  }

  it('forms batches of batchSize and zips vectors back onto chunks in order', async () => {
    const { embedder, batches } = recordingEmbedder();
    const chunks = ['a', 'b', 'c', 'd', 'e'].map((t) => chunk(`c-${t}`, t));
    const embedded = await firstValueFrom(
      of(...chunks).pipe(embedBatched(embedder, { batchSize: 2 }), toArray()),
    );

    expect(batches).toEqual([['a', 'b'], ['c', 'd'], ['e']]);
    expect(embedded.map((c: EmbeddedChunk) => c.id)).toEqual(['c-a', 'c-b', 'c-c', 'c-d', 'c-e']);
    expect(Array.from(embedded[3]!.vector)).toEqual([2, 1]); // batch 2, position 1
  });

  it('errors loudly when the embedder returns the wrong count', async () => {
    const embedder: Embedder = { embed: () => of([Float32Array.from([1])]) };
    const error = await firstValueFrom(
      of(chunk('a', 'a'), chunk('b', 'b')).pipe(embedBatched(embedder, { batchSize: 2 })),
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RangeError);
  });

  it('marble: the rate limiter spaces embedding requests at the sustained rate', () => {
    const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));
    scheduler.run(({ cold, expectObservable }) => {
      const embedder: Embedder = {
        embed: (texts) => cold('(v|)', { v: texts.map(() => Float32Array.from([1])) }),
      };
      const source = cold('(abcd|)', {
        a: chunk('a', 'a'),
        b: chunk('b', 'b'),
        c: chunk('c', 'c'),
        d: chunk('d', 'd'),
      });
      // batches [a,b] and [c,d]; 1 request per 10ms, burst 1:
      // batch 1 embeds at 0 (a,b emitted together), batch 2 waits for a token → 10ms
      expectObservable(
        source.pipe(
          embedBatched(embedder, {
            batchSize: 2,
            requestsPerInterval: 1,
            intervalMs: 10,
            scheduler,
          }),
          // ids only, for readable marbles
          // eslint-disable-next-line
        ),
      ).toBe('(ab)------(cd|)', {
        a: expect.objectContaining({ id: 'a' }),
        b: expect.objectContaining({ id: 'b' }),
        c: expect.objectContaining({ id: 'c' }),
        d: expect.objectContaining({ id: 'd' }),
      });
    });
  });

  it('unsubscribing mid-batch leaves no dangling embedder request', async () => {
    let aborted = false;
    const hanging: Embedder = {
      embed: () =>
        new Observable<Float32Array[]>(() => () => {
          aborted = true;
        }),
    };
    const subscription = of(chunk('a', 'a'))
      .pipe(embedBatched(hanging, { batchSize: 1 }))
      .subscribe();
    await new Promise((r) => setTimeout(r, 5));
    subscription.unsubscribe();
    expect(aborted).toBe(true);
  });
});
