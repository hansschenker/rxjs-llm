import fc from 'fast-check';
import { firstValueFrom, from, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { charEstimator, splitDocs, splitText, type Tokenizer } from '../../src/index/split';
import type { Doc } from '../../src/index/types';

/** Exact word counter — deliberately different from chars/4. */
const wordTokenizer: Tokenizer = {
  count: (text) => text.split(/\s+/).filter((w) => w !== '').length,
};

describe('splitText basics', () => {
  it('returns the whole text as one chunk when it fits', () => {
    const chunks = splitText('short text', { maxTokens: 100 });
    expect(chunks).toEqual([{ index: 0, text: 'short text', start: 0, end: 10 }]);
  });

  it('splits on paragraph boundaries first', () => {
    const source = 'First paragraph here.\n\nSecond paragraph here.';
    const chunks = splitText(source, { maxTokens: 8 }); // ~32 chars per chunk
    expect(chunks).toHaveLength(2);
    expect(chunks[0]?.text).toBe('First paragraph here.\n\n'); // separator stays with its paragraph
    expect(chunks[1]?.text).toBe('Second paragraph here.');
  });

  it('refines to sentence and word boundaries when paragraphs exceed the budget', () => {
    const source = 'One two three four five six seven eight nine ten.';
    const chunks = splitText(source, { maxTokens: 4, tokenizer: wordTokenizer });
    expect(chunks.length).toBeGreaterThan(1);
    for (const chunk of chunks) {
      expect(wordTokenizer.count(chunk.text)).toBeLessThanOrEqual(4);
    }
  });

  it('force-splits a single oversized word by code points', () => {
    const chunks = splitText('a'.repeat(100), { maxTokens: 5 }); // 20 chars per chunk
    expect(chunks.map((c) => c.text).join('')).toBe('a'.repeat(100));
    for (const chunk of chunks) {
      expect(charEstimator.count(chunk.text)).toBeLessThanOrEqual(5);
    }
  });

  it('empty input yields no chunks', () => {
    expect(splitText('', { maxTokens: 10 })).toEqual([]);
  });

  it('validates maxTokens and overlap', () => {
    expect(() => splitText('x', { maxTokens: 0 })).toThrow(RangeError);
    expect(() => splitText('x', { maxTokens: 10, overlap: 10 })).toThrow(RangeError);
    expect(() => splitText('x', { maxTokens: 10, overlap: -1 })).toThrow(RangeError);
  });

  it('overlap prepends the previous tail; owned spans stay disjoint', () => {
    const source = 'alpha bravo charlie delta echo foxtrot golf hotel india juliet';
    const chunks = splitText(source, { maxTokens: 4, overlap: 1, tokenizer: wordTokenizer });
    expect(chunks.length).toBeGreaterThan(1);
    for (let i = 1; i < chunks.length; i++) {
      const chunk = chunks[i]!;
      const owned = source.slice(chunk.start, chunk.end);
      expect(chunk.text.endsWith(owned)).toBe(true);
      const prefix = chunk.text.slice(0, chunk.text.length - owned.length);
      expect(prefix.length).toBeGreaterThan(0); // context was carried
      expect(source.slice(0, chunk.start).endsWith(prefix)).toBe(true);
      expect(wordTokenizer.count(chunk.text)).toBeLessThanOrEqual(4);
    }
  });
});

describe('splitText properties (the silent-bug hunting ground)', () => {
  const textArb = fc
    .array(
      fc.oneof(
        fc.string({ minLength: 1, maxLength: 30 }),
        fc.constantFrom('👋🌍', '日本語テキスト', 'ä ö ü', '\n\n', '\n', '. ', ' ', 'word'),
      ),
      { minLength: 1, maxLength: 30 },
    )
    .map((parts) => parts.join(''));

  const optionsArb = fc.record({
    maxTokens: fc.integer({ min: 2, max: 40 }),
    overlap: fc.integer({ min: 0, max: 1 }),
  });

  it('owned spans partition the source exactly (lossless)', () => {
    fc.assert(
      fc.property(textArb, optionsArb, (source, options) => {
        const chunks = splitText(source, options);
        const rebuilt = chunks.map((c) => source.slice(c.start, c.end)).join('');
        expect(rebuilt).toBe(source);
        // spans are contiguous and ordered
        let at = 0;
        for (const chunk of chunks) {
          expect(chunk.start).toBe(at);
          expect(chunk.end).toBeGreaterThan(chunk.start);
          at = chunk.end;
        }
        if (source.length > 0) expect(at).toBe(source.length);
      }),
    );
  });

  it('every chunk (overlap included) respects the token budget', () => {
    fc.assert(
      fc.property(textArb, optionsArb, (source, options) => {
        for (const chunk of splitText(source, options)) {
          expect(charEstimator.count(chunk.text)).toBeLessThanOrEqual(options.maxTokens);
        }
      }),
    );
  });

  it('never splits inside a surrogate pair', () => {
    const emojiHeavy = fc
      .array(fc.constantFrom('👋', '🌍', '🚀', '💡', 'x', ' '), { minLength: 5, maxLength: 200 })
      .map((parts) => parts.join(''));
    fc.assert(
      fc.property(emojiHeavy, fc.integer({ min: 1, max: 6 }), (source, maxTokens) => {
        for (const chunk of splitText(source, { maxTokens })) {
          for (const boundary of [chunk.start, chunk.end]) {
            if (boundary > 0 && boundary < source.length) {
              const before = source.charCodeAt(boundary - 1);
              const after = source.charCodeAt(boundary);
              const splitsPair =
                before >= 0xd800 && before <= 0xdbff && after >= 0xdc00 && after <= 0xdfff;
              expect(splitsPair).toBe(false);
            }
          }
          // the chunk text itself round-trips through UTF-8 unchanged
          expect(new TextDecoder().decode(new TextEncoder().encode(chunk.text))).toBe(chunk.text);
        }
      }),
    );
  });
});

describe('splitDocs operator', () => {
  it('attributes chunks to their document with derived ids and inherited metadata', async () => {
    const docs: Doc[] = [
      { id: 'doc-a', text: 'First paragraph.\n\nSecond paragraph.', metadata: { source: 'a.md' } },
      { id: 'doc-b', text: 'tiny', metadata: { source: 'b.md' } },
    ];
    const chunks = await firstValueFrom(
      from(docs).pipe(splitDocs({ maxTokens: 5 }), toArray()),
    );
    expect(chunks.map((c) => c.id)).toEqual(['doc-a#0', 'doc-a#1', 'doc-b#0']);
    expect(chunks[0]?.docId).toBe('doc-a');
    expect(chunks[0]?.metadata).toEqual({ source: 'a.md' });
    expect(chunks[2]?.text).toBe('tiny');
  });
});
