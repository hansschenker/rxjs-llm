import { firstValueFrom, from, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { parseSse, type SseEvent } from '../../src/transport/sse';

const enc = new TextEncoder();

function parseChunks(chunks: (string | Uint8Array)[]): Promise<SseEvent[]> {
  const bytes = chunks.map((c) => (typeof c === 'string' ? enc.encode(c) : c));
  return firstValueFrom(from(bytes).pipe(parseSse(), toArray()));
}

describe('parseSse basics', () => {
  it('parses a minimal event with default type', async () => {
    expect(await parseChunks(['data: hello\n\n'])).toEqual([{ event: 'message', data: 'hello' }]);
  });

  it('honors the event field and resets it after dispatch', async () => {
    const events = await parseChunks(['event: start\ndata: a\n\ndata: b\n\n']);
    expect(events).toEqual([
      { event: 'start', data: 'a' },
      { event: 'message', data: 'b' },
    ]);
  });

  it('joins multiple data lines with newline', async () => {
    expect(await parseChunks(['data: line1\ndata: line2\n\n'])).toEqual([
      { event: 'message', data: 'line1\nline2' },
    ]);
  });

  it('ignores comment lines and unknown fields', async () => {
    expect(await parseChunks([': keep-alive\nweird: field\ndata: x\n\n'])).toEqual([
      { event: 'message', data: 'x' },
    ]);
  });

  it('strips exactly one leading space from values', async () => {
    expect(await parseChunks(['data:  two spaces\n\n'])).toEqual([
      { event: 'message', data: ' two spaces' },
    ]);
    expect(await parseChunks(['data:nospace\n\n'])).toEqual([
      { event: 'message', data: 'nospace' },
    ]);
  });

  it('keeps colons inside values intact', async () => {
    expect(await parseChunks(['data: {"a":"b:c"}\n\n'])).toEqual([
      { event: 'message', data: '{"a":"b:c"}' },
    ]);
  });

  it('a bare "data" line dispatches an empty string', async () => {
    expect(await parseChunks(['data\n\n'])).toEqual([{ event: 'message', data: '' }]);
  });

  it('does not dispatch an event whose data buffer is empty', async () => {
    expect(await parseChunks(['event: ping\n\n'])).toEqual([]);
  });

  it('captures id and retry; last event id persists to later events', async () => {
    const events = await parseChunks(['id: 42\nretry: 1500\ndata: a\n\ndata: b\n\n']);
    expect(events).toEqual([
      { event: 'message', data: 'a', id: '42', retry: 1500 },
      { event: 'message', data: 'b', id: '42' },
    ]);
  });

  it('discards an incomplete event at end of stream (spec)', async () => {
    expect(await parseChunks(['data: complete\n\ndata: dangling\n'])).toEqual([
      { event: 'message', data: 'complete' },
    ]);
  });
});

describe('parseSse line endings', () => {
  const expected = [
    { event: 'message', data: 'a\nb' },
    { event: 'message', data: 'c' },
  ];

  it.each([
    ['LF', 'data: a\ndata: b\n\ndata: c\n\n'],
    ['CRLF', 'data: a\r\ndata: b\r\n\r\ndata: c\r\n\r\n'],
    ['CR', 'data: a\rdata: b\r\rdata: c\r\r'],
  ])('%s produces identical events', async (_name, fixture) => {
    expect(await parseChunks([fixture])).toEqual(expected);
  });
});

describe('parseSse adversarial chunk splits', () => {
  // Emoji forces multi-byte UTF-8; CRLF forces the straddled-CR path;
  // comments, ids and multi-line data exercise every field-parsing branch.
  const fixture =
    'event: message_start\r\ndata: {"model":"m"}\r\n\r\n' +
    ': keep-alive\n' +
    'data: hello 👋 wörld\ndata: line2\n\n' +
    'id: 42\nretry: 1500\ndata: done\r\n\r\n';
  const bytes = enc.encode(fixture);

  it('every possible two-chunk split parses identically to the whole', async () => {
    const reference = await parseChunks([bytes]);
    expect(reference).toHaveLength(3);
    for (let i = 1; i < bytes.length; i++) {
      const events = await parseChunks([bytes.slice(0, i), bytes.slice(i)]);
      expect(events, `split at byte ${i}`).toEqual(reference);
    }
  });

  it('one byte per chunk parses identically to the whole', async () => {
    const reference = await parseChunks([bytes]);
    const shredded = Array.from(bytes, (b) => new Uint8Array([b]));
    expect(await parseChunks(shredded)).toEqual(reference);
  });

  it('is cold: re-subscribing the same pipe yields fresh, equal results', async () => {
    const piped = from([bytes]).pipe(parseSse(), toArray());
    expect(await firstValueFrom(piped)).toEqual(await firstValueFrom(piped));
  });
});
