import { concatMap, from, map, reduce, type Observable } from 'rxjs';
import { fetchStream } from '../../transport/fetch-stream';
import type { Doc } from '../types';

export interface WebLoaderOptions {
  fetchFn?: typeof fetch;
}

/**
 * Fetch one or more URLs and extract readable text (decision D4.1).
 * Sequential (concatMap) — polite to single hosts — and incremental: each
 * page emits as soon as it is extracted. Unsubscribing aborts the
 * in-flight request via fetchStream's teardown.
 *
 * Extraction is deliberately crude: strip script/style/head, drop tags,
 * decode common entities, collapse whitespace. Full readability/boilerplate
 * removal is a NON_GOAL (that is rxjs-rag territory).
 */
export function webLoader(urls: string | readonly string[], options: WebLoaderOptions = {}): Observable<Doc> {
  const list = typeof urls === 'string' ? [urls] : [...urls];

  return from(list).pipe(
    concatMap((url) => {
      const init: Parameters<typeof fetchStream>[1] = { method: 'GET' };
      if (options.fetchFn) init.fetchFn = options.fetchFn;
      return fetchStream(url, init).pipe(
        reduce((html, bytes) => html + decoder.decode(bytes, { stream: true }), ''),
        map((html): Doc => {
          const title = /<title[^>]*>([^<]*)<\/title>/i.exec(html)?.[1]?.trim() ?? '';
          return {
            id: url,
            text: extractText(html),
            metadata: { source: url, ...(title !== '' && { title }) },
          };
        }),
      );
    }),
  );
}

const decoder = new TextDecoder('utf-8');

const ENTITIES: Record<string, string> = {
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&quot;': '"',
  '&#39;': "'",
  '&apos;': "'",
  '&nbsp;': ' ',
};

export function extractText(html: string): string {
  return html
    .replace(/<(script|style|head|noscript|template)\b[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<(p|div|br|li|h[1-6]|tr|section|article)\b[^>]*>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&[a-z]+;|&#\d+;/gi, (entity) => ENTITIES[entity.toLowerCase()] ?? ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .trim();
}
