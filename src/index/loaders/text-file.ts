import { Observable } from 'rxjs';
import type { Doc } from '../types.js';

export interface TextFileLoaderOptions {
  /** Keep files whose RELATIVE path matches. RegExp or predicate.
   * Default: common text extensions (md, txt, ts, js, json, html, css). */
  filter?: RegExp | ((relativePath: string) => boolean);
}

const DEFAULT_FILTER = /\.(md|markdown|txt|ts|js|tsx|jsx|json|html|css|py|rs|go|java)$/i;

/**
 * Recursive directory loader (decision D4.1): one Doc per file, emitted
 * INCREMENTALLY as each file is read — a directory of a thousand files
 * starts flowing after the first one. Unsubscribing stops the walk at the
 * next file boundary. `node:fs` arrives via dynamic import so the rest of
 * the package stays web-standard (ADR-0017); this loader is Node/Bun-only.
 */
export function textFileLoader(root: string, options: TextFileLoaderOptions = {}): Observable<Doc> {
  const filter = options.filter ?? DEFAULT_FILTER;
  const keep = typeof filter === 'function' ? filter : (path: string) => filter.test(path);

  return new Observable<Doc>((subscriber) => {
    let cancelled = false;

    void (async () => {
      const fs = await import('node:fs/promises');
      const pathModule = await import('node:path');

      const walk = async (dir: string): Promise<void> => {
        if (cancelled) return;
        const entries = await fs.readdir(dir, { withFileTypes: true });
        entries.sort((a, b) => a.name.localeCompare(b.name)); // deterministic order
        for (const entry of entries) {
          if (cancelled) return;
          const absolute = pathModule.join(dir, entry.name);
          if (entry.isDirectory()) {
            await walk(absolute);
          } else if (entry.isFile()) {
            const relative = pathModule.relative(root, absolute).replaceAll('\\', '/');
            if (!keep(relative)) continue;
            const text = await fs.readFile(absolute, 'utf8');
            if (cancelled) return;
            subscriber.next({
              id: relative,
              text,
              metadata: { source: relative, absolutePath: absolute },
            });
          }
        }
      };

      await walk(root);
      if (!cancelled) subscriber.complete();
    })().catch((error: unknown) => {
      if (!cancelled) subscriber.error(error);
    });

    return () => {
      cancelled = true;
    };
  });
}
