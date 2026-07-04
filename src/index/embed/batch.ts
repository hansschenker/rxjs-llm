import {
  bufferCount,
  concatMap,
  map,
  mergeMap,
  pipe,
  type OperatorFunction,
  type SchedulerLike,
} from 'rxjs';
import { rateLimit } from '../../operators/rate-limit.js';
import type { Chunk } from '../types.js';
import type { Embedder } from './types.js';

export interface EmbedBatchedOptions {
  /** Texts per embedding request. Default 64. */
  batchSize?: number;
  /** Optional request throttle — Module 1's token bucket, applied per BATCH. */
  requestsPerInterval?: number;
  intervalMs?: number;
  /** Injectable for marble tests. */
  scheduler?: SchedulerLike;
}

export type EmbeddedChunk = Chunk & { vector: Float32Array };

/**
 * The batching operator (decision D4.3) — deliberately nothing but Module 1
 * operators composing: bufferCount forms batches, rateLimit (token bucket)
 * spaces the requests, concatMap keeps one request in flight and preserves
 * order, and each batch's vectors are zipped back onto their chunks.
 * Unsubscribing mid-flight aborts the embedder's HTTP request — the
 * Embedder contract, not this operator, provides that.
 */
export function embedBatched(
  embedder: Embedder,
  options: EmbedBatchedOptions = {},
): OperatorFunction<Chunk, EmbeddedChunk> {
  const batchSize = options.batchSize ?? 64;
  const throttle =
    options.requestsPerInterval !== undefined && options.intervalMs !== undefined
      ? rateLimit<Chunk[]>({
          tokensPerInterval: options.requestsPerInterval,
          intervalMs: options.intervalMs,
          ...(options.scheduler !== undefined && { scheduler: options.scheduler }),
        })
      : (source: import('rxjs').Observable<Chunk[]>) => source;

  return pipe(
    bufferCount<Chunk>(batchSize),
    throttle,
    concatMap((batch) =>
      embedder.embed(batch.map((chunk) => chunk.text)).pipe(
        map((vectors) => {
          if (vectors.length !== batch.length) {
            throw new RangeError(
              `embedder returned ${vectors.length} vectors for a batch of ${batch.length}`,
            );
          }
          return batch.map((chunk, i) => ({ ...chunk, vector: vectors[i]! }));
        }),
      ),
    ),
    mergeMap((embedded) => embedded),
  );
}
