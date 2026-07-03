import type { Observable } from 'rxjs';

/**
 * Embeddings mirror the ChatModel pattern (decision D4.3, ADR-0016): one
 * small interface, provider adapters behind it, the same Observable laws —
 * cold, lazy, unicast, one HTTP request per subscribe, teardown aborts.
 */
export interface Embedder {
  /** One request; the result array is index-aligned with the input. */
  embed(texts: readonly string[]): Observable<Float32Array[]>;
}
