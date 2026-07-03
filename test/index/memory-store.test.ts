import { firstValueFrom } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { memoryStore } from '../../src/index/store/memory';
import { storeContractTests } from './store-contract';

// the memory store is dimension-agnostic; the factory input is for pgvector
storeContractTests('memory', () => Promise.resolve({ store: memoryStore() }));

describe('memoryStore specifics', () => {
  it('rejects dimension mismatches loudly', async () => {
    const store = memoryStore();
    await firstValueFrom(store.upsert([{ id: 'a', vector: Float32Array.from([1, 0]), text: '', metadata: {} }]));
    const error = await firstValueFrom(store.query(Float32Array.from([1, 0, 0]), 1)).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(RangeError);
  });

  it('a zero vector scores 0 against everything rather than NaN', async () => {
    const store = memoryStore();
    await firstValueFrom(store.upsert([{ id: 'a', vector: Float32Array.from([1, 0]), text: '', metadata: {} }]));
    const matches = await firstValueFrom(store.query(Float32Array.from([0, 0]), 1));
    expect(matches[0]?.score).toBe(0);
  });
});
