import {
  bufferCount,
  concatMap,
  map,
  scan,
  type Observable,
  type OperatorFunction,
} from 'rxjs';
import type { EmbeddedChunk } from './embed/batch';
import { embedBatched, type EmbedBatchedOptions } from './embed/batch';
import type { Embedder } from './embed/types';
import { splitDocs, type SplitOptions } from './split';
import type { Doc } from './types';
import type { VectorEntry, VectorStore } from './store/types';

/** Fold chunk provenance into the entry's metadata for later citations. */
export function toVectorEntry(chunk: EmbeddedChunk): VectorEntry {
  return {
    id: chunk.id,
    vector: chunk.vector,
    text: chunk.text,
    metadata: { ...chunk.metadata, docId: chunk.docId, start: chunk.start, end: chunk.end },
  };
}

/**
 * Buffered upsert (decision D4.5's plumbing): batches embedded chunks into
 * the store and emits each batch's size, so `scan` gives running progress.
 */
export function upsertInto(
  store: VectorStore,
  options: { batchSize?: number } = {},
): OperatorFunction<EmbeddedChunk, number> {
  return (source) =>
    source.pipe(
      bufferCount(options.batchSize ?? 64),
      concatMap((chunks) => store.upsert(chunks.map(toVectorEntry))),
    );
}

export interface IngestOptions {
  split: SplitOptions;
  embedder: Embedder;
  store: VectorStore;
  batch?: EmbedBatchedOptions & { upsertBatchSize?: number };
}

/**
 * The whole pipeline as one pipe: load → split → embed → upsert. Emits the
 * CUMULATIVE upserted count after each store batch and completes when the
 * source does. Unsubscribing anywhere mid-flight aborts the in-flight
 * embedding request and stops the loader — every piece is teardown-complete,
 * so the pipeline is too.
 */
export function ingest(docs: Observable<Doc>, options: IngestOptions): Observable<number> {
  return docs.pipe(
    splitDocs(options.split),
    embedBatched(options.embedder, options.batch ?? {}),
    upsertInto(options.store, { batchSize: options.batch?.upsertBatchSize ?? 64 }),
    scan((total, batchCount) => total + batchCount, 0),
    map((total) => total),
  );
}
