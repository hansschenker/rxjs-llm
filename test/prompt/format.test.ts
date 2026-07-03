import { describe, expect, it } from 'vitest';
import { asBullets, asJson, noJargon } from '../../src/prompt/format';
import { promptTemplate } from '../../src/prompt/template';

describe('format helpers (D2.3 — pure string transformers)', () => {
  it('asBullets appends the bullet instruction', () => {
    expect(asBullets(3)('Summarize the doc.')).toBe(
      'Summarize the doc.\n\nRespond with exactly 3 bullet points, one line each.',
    );
  });

  it('noJargon appends the plain-language instruction', () => {
    expect(noJargon()('Explain quantum tunnelling.')).toContain('avoid jargon');
  });

  it('asJson embeds the schema and carries it for later parsing', () => {
    const schema = {
      type: 'object',
      properties: { city: { type: 'string' } },
      required: ['city'],
    };
    const format = asJson(schema);
    const rendered = format('Extract the city.');
    expect(rendered).toContain('ONLY valid JSON');
    expect(rendered).toContain('"required": [\n    "city"\n  ]');
    expect(format.schema).toBe(schema); // same reference — parse against it later
  });

  it('composes by nesting, outermost applied last', () => {
    const text = noJargon()(asBullets(2)('Explain RxJS.'));
    const bulletsAt = text.indexOf('bullet points');
    const jargonAt = text.indexOf('avoid jargon');
    expect(bulletsAt).toBeGreaterThan(-1);
    expect(jargonAt).toBeGreaterThan(bulletsAt);
  });

  it('composes with templates: format the rendered string', () => {
    const summarize = promptTemplate('Summarize {doc}.');
    const text = asBullets(4)(summarize({ doc: 'the paper' }));
    expect(text).toBe('Summarize the paper.\n\nRespond with exactly 4 bullet points, one line each.');
  });
});
