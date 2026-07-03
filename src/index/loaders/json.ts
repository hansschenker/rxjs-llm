import { defer, from, map, type Observable } from 'rxjs';
import type { Doc } from '../types';

export interface JsonLoaderOptions {
  /** Which field holds the text, or a function deriving it. */
  text: string | ((record: Record<string, unknown>) => string);
  /** Which field holds the id, or a function. Default: the array index. */
  id?: string | ((record: Record<string, unknown>, index: number) => string);
}

/**
 * One Doc per record of a JSON array (decision D4.1). Accepts a parsed
 * value or a JSON string; the remaining fields of each record become the
 * Doc's metadata. Pure apart from lazy parsing — runs anywhere.
 */
export function jsonLoader(input: unknown, options: JsonLoaderOptions): Observable<Doc> {
  return defer(() => {
    const value: unknown = typeof input === 'string' ? JSON.parse(input) : input;
    if (!Array.isArray(value)) {
      throw new TypeError('jsonLoader expects a JSON array of records');
    }
    return from(value as unknown[]).pipe(
      map((raw, index): Doc => {
        if (raw === null || typeof raw !== 'object') {
          throw new TypeError(`record ${index} is not an object`);
        }
        const record = raw as Record<string, unknown>;
        const text =
          typeof options.text === 'function'
            ? options.text(record)
            : String(record[options.text] ?? '');
        const id =
          options.id === undefined
            ? String(index)
            : typeof options.id === 'function'
              ? options.id(record, index)
              : String(record[options.id] ?? index);
        const metadata: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(record)) {
          if (key !== options.text && key !== options.id) metadata[key] = val;
        }
        return { id, text, metadata };
      }),
    );
  });
}
