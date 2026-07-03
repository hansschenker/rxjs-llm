import fc from 'fast-check';
import { firstValueFrom } from 'rxjs';
import { describe, expect, it } from 'vitest';
import type { VectorEntry, VectorStore } from '../../src/index/store/types';

const vec = (...values: number[]) => Float32Array.from(values);

const entry = (
  id: string,
  vector: Float32Array,
  metadata: Record<string, unknown> = {},
): VectorEntry => ({
  id,
  vector,
  text: `text of ${id}`,
  metadata,
});

export interface StoreHarness {
  store: VectorStore;
  cleanup?: () => Promise<void>;
}

/** Dimensions are a factory input: pgvector columns are fixed-dimension,
 * so the suite tells each store what it is about to insert. */
export type StoreFactory = (dimensions: number) => Promise<StoreHarness>;

/**
 * The store law tests (decision D4.4): one suite, every implementation.
 * A store that passes this file is interchangeable with any other.
 */
export function storeContractTests(name: string, factory: StoreFactory): void {
  const withStore = async (
    dimensions: number,
    run: (store: VectorStore) => Promise<void>,
  ): Promise<void> => {
    const harness = await factory(dimensions);
    try {
      await run(harness.store);
    } finally {
      await harness.cleanup?.();
    }
  };

  describe(`VectorStore contract: ${name}`, () => {
    it('querying with a stored vector returns that entry first, score ≈ 1', () =>
      withStore(3, async (store) => {
        await firstValueFrom(
          store.upsert([
            entry('a', vec(1, 0, 0)),
            entry('b', vec(0, 1, 0)),
            entry('c', vec(0.6, 0.8, 0)),
          ]),
        );
        const matches = await firstValueFrom(store.query(vec(0, 1, 0), 3));
        expect(matches[0]?.id).toBe('b');
        expect(matches[0]?.score).toBeCloseTo(1, 5);
      }));

    it('ranks by cosine similarity against hand-computed fixtures', () =>
      withStore(2, async (store) => {
        await firstValueFrom(
          store.upsert([
            entry('east', vec(1, 0)),
            entry('north', vec(0, 1)),
            entry('northeast', vec(Math.SQRT1_2, Math.SQRT1_2)),
          ]),
        );
        const matches = await firstValueFrom(store.query(vec(1, 0), 3));
        expect(matches.map((m) => m.id)).toEqual(['east', 'northeast', 'north']);
        expect(matches[0]?.score).toBeCloseTo(1, 5);
        expect(matches[1]?.score).toBeCloseTo(Math.SQRT1_2, 5);
        expect(matches[2]?.score).toBeCloseTo(0, 5);
      }));

    it('k caps the result count; a k beyond the store size returns everything', () =>
      withStore(2, async (store) => {
        await firstValueFrom(store.upsert([entry('a', vec(1, 0)), entry('b', vec(0, 1))]));
        expect(await firstValueFrom(store.query(vec(1, 0), 1))).toHaveLength(1);
        expect(await firstValueFrom(store.query(vec(1, 0), 99))).toHaveLength(2);
      }));

    it('upsert replaces on id collision instead of duplicating', () =>
      withStore(2, async (store) => {
        await firstValueFrom(store.upsert([entry('a', vec(1, 0))]));
        await firstValueFrom(store.upsert([{ ...entry('a', vec(0, 1)), text: 'replaced' }]));
        const matches = await firstValueFrom(store.query(vec(0, 1), 10));
        expect(matches).toHaveLength(1);
        expect(matches[0]?.text).toBe('replaced');
        expect(matches[0]?.score).toBeCloseTo(1, 5);
      }));

    it('delete removes entries and reports the count', () =>
      withStore(2, async (store) => {
        await firstValueFrom(store.upsert([entry('a', vec(1, 0)), entry('b', vec(0, 1))]));
        expect(await firstValueFrom(store.delete(['a', 'missing']))).toBe(1);
        const matches = await firstValueFrom(store.query(vec(1, 0), 10));
        expect(matches.map((m) => m.id)).toEqual(['b']);
      }));

    it('a metadata filter restricts results before k applies', () =>
      withStore(2, async (store) => {
        await firstValueFrom(
          store.upsert([
            entry('a', vec(1, 0), { lang: 'en' }),
            entry('b', vec(0.99, 0.14), { lang: 'de' }),
            entry('c', vec(0.9, 0.44), { lang: 'en' }),
          ]),
        );
        const matches = await firstValueFrom(
          store.query(vec(1, 0), 2, (metadata) => metadata['lang'] === 'en'),
        );
        expect(matches.map((m) => m.id)).toEqual(['a', 'c']);
      }));

    it('metadata and text round-trip through the store', () =>
      withStore(2, async (store) => {
        const metadata = { source: 'doc.md', page: 3, tags: ['a', 'b'] };
        await firstValueFrom(store.upsert([{ ...entry('a', vec(1, 0)), metadata }]));
        const matches = await firstValueFrom(store.query(vec(1, 0), 1));
        expect(matches[0]?.metadata).toEqual(metadata);
        expect(matches[0]?.text).toBe('text of a');
      }));

    it('operations are lazy and cold: nothing happens before subscribe, resubscribe re-runs', () =>
      withStore(2, async (store) => {
        const upsert$ = store.upsert([entry('a', vec(1, 0))]);
        const before = await firstValueFrom(store.query(vec(1, 0), 10));
        expect(before).toHaveLength(0); // building the upsert Observable did nothing
        await firstValueFrom(upsert$);
        const after = await firstValueFrom(store.query(vec(1, 0), 10));
        expect(after).toHaveLength(1);
      }));

    it('property: querying with any stored vector returns its own id first', () =>
      withStore(4, async (store) => {
        const vectorArb = fc
          .array(fc.double({ min: -1, max: 1, noNaN: true }), { minLength: 4, maxLength: 4 })
          .filter((values) => Math.hypot(...values) > 0.1);
        const vectorsArb = fc.array(vectorArb, { minLength: 2, maxLength: 6 }).filter((vectors) => {
          // pairwise distinct directions, so "itself first" has a unique answer
          const unit = (v: number[]) => {
            const n = Math.hypot(...v);
            return v.map((x) => x / n);
          };
          const units = vectors.map(unit);
          for (let i = 0; i < units.length; i++) {
            for (let j = i + 1; j < units.length; j++) {
              const dot = units[i]!.reduce((s, x, d) => s + x * units[j]![d]!, 0);
              if (dot > 0.999) return false;
            }
          }
          return true;
        });

        await fc.assert(
          fc.asyncProperty(vectorsArb, async (vectors) => {
            const ids = vectors.map((_, i) => `v${i}`);
            try {
              await firstValueFrom(
                store.upsert(vectors.map((v, i) => entry(ids[i]!, Float32Array.from(v)))),
              );
              for (let i = 0; i < vectors.length; i++) {
                const matches = await firstValueFrom(
                  store.query(Float32Array.from(vectors[i]!), 1),
                );
                expect(matches[0]?.id).toBe(ids[i]);
              }
            } finally {
              await firstValueFrom(store.delete(ids)); // one store, clean slate per run
            }
          }),
          { numRuns: 15 },
        );
      }));
  });
}
