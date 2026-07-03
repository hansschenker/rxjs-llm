import { defer, map } from 'rxjs';
import { ParseError } from '../../errors';
import { fetchJson } from '../../transport/fetch-json';
import type { Embedder } from './types';

export interface OllamaEmbedderConfig {
  /** e.g. 'nomic-embed-text' */
  model: string;
  baseUrl?: string;
  fetchFn?: typeof fetch;
}

const PROVIDER = 'ollama';

/** Ollama /api/embed adapter — local and keyless, like the chat adapter. */
export function ollamaEmbedder(config: OllamaEmbedderConfig): Embedder {
  const baseUrl = config.baseUrl ?? 'http://localhost:11434';

  return {
    embed: (texts) =>
      defer(() => {
        const init: Parameters<typeof fetchJson>[1] = {
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ model: config.model, input: texts }),
          provider: PROVIDER,
        };
        if (config.fetchFn) init.fetchFn = config.fetchFn;
        return fetchJson(`${baseUrl}/api/embed`, init);
      }).pipe(
        map((response) => {
          const embeddings = (response as { embeddings?: number[][] }).embeddings;
          if (!Array.isArray(embeddings) || embeddings.length !== texts.length) {
            throw new ParseError(
              `expected ${texts.length} embeddings, got ${Array.isArray(embeddings) ? embeddings.length : 'none'}`,
              { provider: PROVIDER },
            );
          }
          return embeddings.map((values) => Float32Array.from(values));
        }),
      ),
  };
}
