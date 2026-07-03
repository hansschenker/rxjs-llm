import { concatMap, from, map, of, type ObservableInput, type OperatorFunction } from 'rxjs';
import { charEstimator, type Tokenizer } from './split';
import type { Embedder } from './embed/types';
import type { MetadataFilter, QueryMatch, VectorStore } from './store/types';

export interface RetrieveOptions {
  /** Trim the formatted context to this many tokens (whole blocks only). */
  tokenBudget?: number;
  tokenizer?: Tokenizer;
  /** Restrict candidates before k applies (passed to the store). */
  filter?: MetadataFilter;
  /** Optional rerank hook: reorder/trim the store's top-k before formatting. */
  rerank?: (query: string, matches: QueryMatch[]) => ObservableInput<QueryMatch[]>;
}

export interface RetrievedContext {
  query: string;
  /** The matches that made it into the formatted context, in order. */
  matches: QueryMatch[];
  /** `[source: …]` blocks joined by blank lines, sized to the budget. */
  context: string;
}

/**
 * The retriever is one operator (decision D4.5, ADR-0019): query string in,
 * formatted context out — embed, top-k, optional rerank, format under a
 * token budget. Drops into a chain stage unchanged:
 *
 *   stage('retrieve', ctx => of(ctx.question).pipe(retrieveContext(store, embedder, 6)))
 */
export function retrieveContext(
  store: VectorStore,
  embedder: Embedder,
  k: number,
  options: RetrieveOptions = {},
): OperatorFunction<string, RetrievedContext> {
  const tokenizer = options.tokenizer ?? charEstimator;
  return concatMap((query: string) =>
    embedder.embed([query]).pipe(
      concatMap((vectors) => store.query(vectors[0]!, k, options.filter)),
      concatMap((matches) =>
        options.rerank === undefined ? of(matches) : from(options.rerank(query, matches)),
      ),
      map((matches) => format(query, matches, options.tokenBudget, tokenizer)),
    ),
  );
}

function format(
  query: string,
  matches: QueryMatch[],
  budget: number | undefined,
  tokenizer: Tokenizer,
): RetrievedContext {
  const included: QueryMatch[] = [];
  const blocks: string[] = [];
  let used = 0;
  for (const match of matches) {
    const source =
      typeof match.metadata['source'] === 'string' ? match.metadata['source'] : match.id;
    const block = `[source: ${source}]\n${match.text}`;
    const cost = tokenizer.count(block);
    // whole blocks only; the top match is always included so a tight
    // budget degrades to "best chunk", never to an empty context
    if (budget !== undefined && included.length > 0 && used + cost > budget) break;
    included.push(match);
    blocks.push(block);
    used += cost;
  }
  return { query, matches: included, context: blocks.join('\n\n') };
}
