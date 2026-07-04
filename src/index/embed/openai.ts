import { defer, map } from 'rxjs';
import { ParseError } from '../../errors.js';
import { fetchJson } from '../../transport/fetch-json.js';
import type { Embedder } from './types.js';

export interface OpenAiEmbedderConfig {
  apiKey: string;
  /** e.g. 'text-embedding-3-small' */
  model: string;
  baseUrl?: string;
  /** Ask the API for truncated dimensions (embedding-3 models support it). */
  dimensions?: number;
  fetchFn?: typeof fetch;
}

const PROVIDER = 'openai';

interface EmbeddingsResponse {
  data?: { index?: number; embedding?: number[] }[];
}

/** OpenAI /v1/embeddings adapter — the reference Embedder shape. */
export function openaiEmbedder(config: OpenAiEmbedderConfig): Embedder {
  const baseUrl = config.baseUrl ?? 'https://api.openai.com';

  return {
    embed: (texts) =>
      defer(() => {
        const init: Parameters<typeof fetchJson>[1] = {
          headers: {
            authorization: `Bearer ${config.apiKey}`,
            'content-type': 'application/json',
          },
          body: JSON.stringify({
            model: config.model,
            input: texts,
            ...(config.dimensions !== undefined && { dimensions: config.dimensions }),
          }),
          provider: PROVIDER,
        };
        if (config.fetchFn) init.fetchFn = config.fetchFn;
        return fetchJson(`${baseUrl}/v1/embeddings`, init);
      }).pipe(
        map((response) => {
          const data = (response as EmbeddingsResponse).data;
          if (!Array.isArray(data) || data.length !== texts.length) {
            throw new ParseError(
              `expected ${texts.length} embeddings, got ${Array.isArray(data) ? data.length : 'none'}`,
              { provider: PROVIDER },
            );
          }
          // the API documents index-alignment but we sort defensively
          const vectors = new Array<Float32Array>(data.length);
          for (let i = 0; i < data.length; i += 1) {
            const item = data[i]!;
            vectors[item.index ?? i] = Float32Array.from(item.embedding ?? []);
          }
          return vectors;
        }),
      ),
  };
}
