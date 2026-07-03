import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { firstValueFrom, take, toArray } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { jsonLoader } from '../../src/index/loaders/json';
import { extractText, webLoader } from '../../src/index/loaders/web';
import { textFileLoader } from '../../src/index/loaders/text-file';
import { mockFetch } from '../helpers/mock-fetch';

const corpusDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'fixtures', 'corpus');

describe('textFileLoader', () => {
  it('walks the directory recursively and emits one Doc per matching file', async () => {
    const docs = await firstValueFrom(textFileLoader(corpusDir).pipe(toArray()));
    expect(docs.map((d) => d.id).sort()).toEqual(['alps.md', 'espresso.md', 'rxjs.md']);
    const rxjs = docs.find((d) => d.id === 'rxjs.md')!;
    expect(rxjs.text).toContain('reactive programming');
    expect(rxjs.metadata['source']).toBe('rxjs.md');
  });

  it('applies a RegExp filter on the relative path', async () => {
    const docs = await firstValueFrom(
      textFileLoader(corpusDir, { filter: /^rxjs/ }).pipe(toArray()),
    );
    expect(docs.map((d) => d.id)).toEqual(['rxjs.md']);
  });

  it('is lazy: building the observable touches no files', async () => {
    const loader$ = textFileLoader(join(corpusDir, 'does-not-exist'));
    await new Promise((r) => setTimeout(r, 10)); // nothing throws while unsubscribed
    const error = await firstValueFrom(loader$).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(Error); // only on subscribe does the walk start (and fail)
  });

  it('cancellation stops the walk mid-directory', async () => {
    const first = await firstValueFrom(textFileLoader(corpusDir).pipe(take(1), toArray()));
    expect(first).toHaveLength(1); // take(1) unsubscribed; no error, no stray emissions
  });
});

describe('webLoader', () => {
  const html = `<!doctype html><html><head><title>Test Page</title>
    <style>body { color: red }</style><script>alert('nope')</script></head>
    <body><h1>Heading</h1><p>First &amp; second paragraph.</p>
    <div>Third block with a <a href="#">link</a>.</div></body></html>`;

  it('fetches, strips markup, decodes entities, and keeps the title as metadata', async () => {
    const { fetchFn, calls } = mockFetch([html]);
    const docs = await firstValueFrom(
      webLoader('https://example.test/page', { fetchFn }).pipe(toArray()),
    );
    expect(docs).toHaveLength(1);
    const doc = docs[0]!;
    expect(doc.id).toBe('https://example.test/page');
    expect(doc.metadata).toEqual({ source: 'https://example.test/page', title: 'Test Page' });
    expect(doc.text).toContain('Heading');
    expect(doc.text).toContain('First & second paragraph.');
    expect(doc.text).toContain('Third block with a link');
    expect(doc.text).not.toContain('alert');
    expect(doc.text).not.toContain('color: red');
    expect(calls[0]?.init.method).toBe('GET');
  });

  it('loads multiple urls sequentially, one Doc each', async () => {
    const { fetchFn, calls } = mockFetch(['<title>A</title>one']);
    const docs = await firstValueFrom(
      webLoader(['https://a.test', 'https://b.test'], { fetchFn }).pipe(toArray()),
    );
    expect(docs.map((d) => d.id)).toEqual(['https://a.test', 'https://b.test']);
    expect(calls.map((c) => c.url)).toEqual(['https://a.test', 'https://b.test']);
  });

  it('unsubscribing aborts the in-flight request', async () => {
    const { fetchFn, calls } = mockFetch(['<p>partial'], { hang: true });
    const subscription = webLoader('https://slow.test', { fetchFn }).subscribe();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    subscription.unsubscribe();
    expect(calls[0]?.init.signal?.aborted).toBe(true);
  });
});

describe('extractText', () => {
  it('collapses whitespace and inserts breaks at block boundaries', () => {
    expect(extractText('<p>a</p><p>b</p>')).toBe('a\nb');
    expect(extractText('lots   of\t\tspace')).toBe('lots of space');
  });
});

describe('jsonLoader', () => {
  const records = [
    { slug: 'first', body: 'text one', lang: 'en' },
    { slug: 'second', body: 'text two', lang: 'de' },
  ];

  it('emits one Doc per record with field-based text/id and the rest as metadata', async () => {
    const docs = await firstValueFrom(
      jsonLoader(records, { text: 'body', id: 'slug' }).pipe(toArray()),
    );
    expect(docs).toEqual([
      { id: 'first', text: 'text one', metadata: { lang: 'en' } },
      { id: 'second', text: 'text two', metadata: { lang: 'de' } },
    ]);
  });

  it('accepts a JSON string and function-based extractors', async () => {
    const docs = await firstValueFrom(
      jsonLoader(JSON.stringify(records), {
        text: (r) => `${String(r['slug'])}: ${String(r['body'])}`,
      }).pipe(toArray()),
    );
    expect(docs[0]?.id).toBe('0'); // index default
    expect(docs[0]?.text).toBe('first: text one');
  });

  it('rejects non-array input lazily, on subscribe', async () => {
    const loader$ = jsonLoader({ not: 'an array' }, { text: 'body' });
    const error = await firstValueFrom(loader$).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(TypeError);
  });
});
