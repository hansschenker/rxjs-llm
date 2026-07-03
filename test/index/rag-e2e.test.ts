import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { defer, firstValueFrom, last, Observable, of, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { ingest, upsertInto } from '../../src/index/ingest';
import { textFileLoader } from '../../src/index/loaders/text-file';
import { retrieveContext } from '../../src/index/retrieve';
import { memoryStore } from '../../src/index/store/memory';
import type { QueryMatch } from '../../src/index/store/types';
import type { Embedder } from '../../src/index/embed/types';

const corpusDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'fixtures', 'corpus');

/**
 * Deterministic bag-of-words embedder: hash words into a 128-dim vector.
 * No network, no keys — and cosine similarity genuinely reflects shared
 * vocabulary, so relevance assertions are real, not mocked.
 */
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

describe('RAG end-to-end (Module 4 capstone)', () => {
  it('ingests the fixture corpus and retrieves the known-relevant chunk', async () => {
    const store = memoryStore();
    const embedder = bagOfWords();

    const total = await firstValueFrom(
      ingest(textFileLoader(corpusDir), {
        split: { maxTokens: 60, overlap: 8 },
        embedder,
        store,
        batch: { batchSize: 8 },
      }).pipe(last()),
    );
    expect(total).toBeGreaterThanOrEqual(3); // at least one chunk per document

    const retrieved = await firstValueFrom(
      of('how much water pressure does espresso brewing use?').pipe(
        retrieveContext(store, embedder, 3),
      ),
    );

    // the top match comes from the espresso document, and the context
    // carries the source attribution the formatter promises
    expect(retrieved.matches[0]?.metadata['docId']).toBe('espresso.md');
    expect(retrieved.context).toContain('[source: espresso.md]');
    expect(retrieved.context).toContain('pressure');

    // and a different question retrieves a different document
    const rxjs = await firstValueFrom(
      of('how do observables and operators compose reactive streams?').pipe(
        retrieveContext(store, embedder, 3),
      ),
    );
    expect(rxjs.matches[0]?.metadata['docId']).toBe('rxjs.md');
  });

  it('a token budget trims to whole blocks but never below the top match', async () => {
    const store = memoryStore();
    const embedder = bagOfWords();
    await firstValueFrom(
      ingest(textFileLoader(corpusDir), {
        split: { maxTokens: 60 },
        embedder,
        store,
      }).pipe(last()),
    );

    const generous = await firstValueFrom(
      of('espresso pressure').pipe(retrieveContext(store, embedder, 5)),
    );
    const tight = await firstValueFrom(
      of('espresso pressure').pipe(retrieveContext(store, embedder, 5, { tokenBudget: 30 })),
    );
    expect(generous.matches.length).toBeGreaterThan(tight.matches.length);
    expect(tight.matches).toHaveLength(1); // degraded to best chunk, not to nothing
    expect(tight.matches[0]?.id).toBe(generous.matches[0]?.id);
  });

  it('the rerank hook reorders before formatting', async () => {
    const store = memoryStore();
    const embedder = bagOfWords();
    await firstValueFrom(
      ingest(textFileLoader(corpusDir), { split: { maxTokens: 60 }, embedder, store }).pipe(
        last(),
      ),
    );
    const reversed = await firstValueFrom(
      of('espresso pressure').pipe(
        retrieveContext(store, embedder, 3, {
          rerank: (_query, matches: QueryMatch[]) => of([...matches].reverse()),
        }),
      ),
    );
    const normal = await firstValueFrom(
      of('espresso pressure').pipe(retrieveContext(store, embedder, 3)),
    );
    expect(reversed.matches.map((m) => m.id)).toEqual(normal.matches.map((m) => m.id).reverse());
  });

  it('cancelling mid-ingest aborts the in-flight embedding and stops upserts', async () => {
    const store = memoryStore();
    let inFlight = 0;
    let torndown = 0;
    const slowEmbedder: Embedder = {
      embed: (texts) =>
        defer(() => {
          inFlight += 1;
          return new Observable<Float32Array[]>((subscriber) => {
            const timer = setTimeout(() => {
              subscriber.next(texts.map(() => Float32Array.from([1, 0])));
              subscriber.complete();
            }, 50);
            return () => {
              torndown += 1;
              clearTimeout(timer);
            };
          });
        }),
    };

    const subscription = ingest(textFileLoader(corpusDir), {
      split: { maxTokens: 40 },
      embedder: slowEmbedder,
      store,
      batch: { batchSize: 4 },
    }).subscribe();

    await new Promise((r) => setTimeout(r, 20)); // first batch is in flight
    expect(inFlight).toBeGreaterThan(0);
    subscription.unsubscribe();
    expect(torndown).toBe(inFlight); // no dangling embedding request

    await new Promise((r) => setTimeout(r, 80));
    const matches = await firstValueFrom(store.query(Float32Array.from([1, 0]), 10));
    expect(matches).toHaveLength(0); // nothing was upserted after cancellation
  });

  it('upsertInto emits per-batch counts for progress reporting', async () => {
    const store = memoryStore();
    const chunks = ['a', 'b', 'c', 'd', 'e'].map((t, i) => ({
      id: `c${i}`,
      docId: 'doc',
      index: i,
      text: t,
      start: 0,
      end: 1,
      metadata: {},
      vector: Float32Array.from([1, i]),
    }));
    const counts = await firstValueFrom(
      of(...chunks).pipe(upsertInto(store, { batchSize: 2 }), toArray()),
    );
    expect(counts).toEqual([2, 2, 1]);
    const stored = await firstValueFrom(store.query(Float32Array.from([1, 0]), 10));
    expect(stored).toHaveLength(5);
    expect(stored[0]?.metadata).toMatchObject({ docId: 'doc' }); // provenance folded in
  });
});
