import { defer, of } from 'rxjs';
import type { MetadataFilter, QueryMatch, VectorEntry, VectorStore } from './types.js';

/**
 * Brute-force cosine over Float32Arrays (decision D4.4). Fine to roughly
 * 50k vectors — a full scan of 50k × 1536 dims is single-digit
 * milliseconds. Beyond that you want a real index, which is a NON_GOAL.
 */
export function memoryStore(): VectorStore {
  const entries = new Map<string, { entry: VectorEntry; norm: number }>();

  return {
    upsert: (batch) =>
      defer(() => {
        for (const entry of batch) {
          entries.set(entry.id, { entry, norm: l2norm(entry.vector) });
        }
        return of(batch.length);
      }),

    query: (vector, k, filter?: MetadataFilter) =>
      defer(() => {
        const queryNorm = l2norm(vector);
        const matches: QueryMatch[] = [];
        for (const { entry, norm } of entries.values()) {
          if (entry.vector.length !== vector.length) {
            throw new RangeError(
              `dimension mismatch: query has ${vector.length}, entry ${entry.id} has ${entry.vector.length}`,
            );
          }
          if (filter !== undefined && !filter(entry.metadata, entry.id)) continue;
          matches.push({ ...entry, score: cosine(vector, entry.vector, queryNorm, norm) });
        }
        matches.sort((a, b) => b.score - a.score);
        return of(matches.slice(0, k));
      }),

    delete: (ids) =>
      defer(() => {
        let removed = 0;
        for (const id of ids) {
          if (entries.delete(id)) removed += 1;
        }
        return of(removed);
      }),
  };
}

function l2norm(vector: Float32Array): number {
  let sum = 0;
  for (let i = 0; i < vector.length; i += 1) sum += vector[i]! * vector[i]!;
  return Math.sqrt(sum);
}

function cosine(a: Float32Array, b: Float32Array, normA: number, normB: number): number {
  if (normA === 0 || normB === 0) return 0;
  let dot = 0;
  for (let i = 0; i < a.length; i += 1) dot += a[i]! * b[i]!;
  return dot / (normA * normB);
}
