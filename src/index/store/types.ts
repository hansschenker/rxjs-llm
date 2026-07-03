import type { Observable } from 'rxjs';

/** One stored vector with its chunk text and metadata. */
export interface VectorEntry {
  id: string;
  vector: Float32Array;
  text: string;
  metadata: Record<string, unknown>;
}

export interface QueryMatch extends VectorEntry {
  /** Cosine similarity in [-1, 1]; higher is closer. */
  score: number;
}

export type MetadataFilter = (metadata: Record<string, unknown>, id: string) => boolean;

/**
 * A small interface, not a database abstraction (decision D4.4, ADR-0015).
 * Everything is Observable-returning and obeys the Module 1 laws: cold,
 * lazy, one operation per subscribe. `upsert` replaces on id collision.
 */
export interface VectorStore {
  upsert(entries: readonly VectorEntry[]): Observable<number>;
  query(vector: Float32Array, k: number, filter?: MetadataFilter): Observable<QueryMatch[]>;
  delete(ids: readonly string[]): Observable<number>;
}
