import { firstValueFrom, from, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { ParseError } from '../../src/errors';
import { parseNdjson } from '../../src/transport/ndjson';

const enc = new TextEncoder();

function parseChunks(chunks: string[]): Promise<unknown[]> {
  return firstValueFrom(from(chunks.map((c) => enc.encode(c))).pipe(parseNdjson(), toArray()));
}

describe('parseNdjson', () => {
  it('emits one parsed value per line', async () => {
    expect(await parseChunks(['{"a":1}\n{"b":2}\n'])).toEqual([{ a: 1 }, { b: 2 }]);
  });

  it('skips blank lines and parses a trailing line without a newline', async () => {
    expect(await parseChunks(['{"a":1}\n\n\n{"b":2}'])).toEqual([{ a: 1 }, { b: 2 }]);
  });

  it('errors with ParseError on a malformed line', async () => {
    const error = await parseChunks(['{"a":1}\nnot json\n']).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ParseError);
  });

  it('survives every two-chunk split, including mid-codepoint', async () => {
    const fixture = '{"text":"héllo 👋"}\n{"done":true,"reason":"stop"}\n';
    const bytes = enc.encode(fixture);
    const reference = await firstValueFrom(from([bytes]).pipe(parseNdjson(), toArray()));
    expect(reference).toHaveLength(2);
    for (let i = 1; i < bytes.length; i++) {
      const events = await firstValueFrom(
        from([bytes.slice(0, i), bytes.slice(i)]).pipe(parseNdjson(), toArray()),
      );
      expect(events, `split at byte ${i}`).toEqual(reference);
    }
  });
});
