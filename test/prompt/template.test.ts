import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  prompt,
  promptTemplate,
  renderTemplate,
  templateVars,
} from '../../src/prompt/template';

describe('promptTemplate runtime', () => {
  it('interpolates values, coercing numbers', () => {
    const summarize = promptTemplate('Summarize {doc} in {n} bullets.');
    expect(summarize({ doc: 'the text', n: 5 })).toBe('Summarize the text in 5 bullets.');
  });

  it('is pure: applications are independent', () => {
    const greet = promptTemplate('Hi {name}');
    expect(greet({ name: 'a' })).toBe('Hi a');
    expect(greet({ name: 'b' })).toBe('Hi b');
    expect(greet({ name: 'a' })).toBe('Hi a');
  });

  it('renders {{ and }} as literal braces', () => {
    const literal = promptTemplate('Return {{"k": {v}}} as JSON');
    expect(literal({ v: 1 })).toBe('Return {"k": 1} as JSON');
  });

  it('a fully escaped template takes no vars and keeps its braces', () => {
    const none = promptTemplate('all {{foo}} literal');
    expect(none()).toBe('all {foo} literal');
  });

  it('handles adjacent placeholders', () => {
    const glue = promptTemplate('{a}{b}');
    expect(glue({ a: 'x', b: 'y' })).toBe('xy');
  });

  it('never re-scans values: braces in values pass through', () => {
    const one = promptTemplate('{a}');
    expect(one({ a: '{b}' })).toBe('{b}');
    expect(one({ a: '{{nested}}' })).toBe('{{nested}}');
  });

  it('treats an unclosed brace and a lone } as literals', () => {
    expect(renderTemplate('broken {oops', {})).toBe('broken {oops');
    expect(renderTemplate('a } b', {})).toBe('a } b');
  });

  it('handles unicode in names and values', () => {
    const jp = promptTemplate('こんにちは {名前}!');
    expect(jp({ 名前: '世界 👋' })).toBe('こんにちは 世界 👋!');
  });

  it('repeated placeholders render everywhere', () => {
    const twice = promptTemplate('{x} and {x}');
    expect(twice({ x: 'y' })).toBe('y and y');
  });
});

describe('templateVars', () => {
  it('extracts names in first-occurrence order, deduped, escapes skipped', () => {
    expect(templateVars('use {b} then {a} then {b}, not {{c}}')).toEqual(['b', 'a']);
  });

  it('round-trips: interpolation and extraction agree on any template (property)', () => {
    const literalArb = fc.string().map((s) => s.replace(/[{}]/g, ''));
    const nameArb = fc.stringMatching(/^[A-Za-z_][A-Za-z0-9_]{0,7}$/);
    fc.assert(
      fc.property(
        fc.array(fc.record({ lit: literalArb, name: nameArb }), { minLength: 1, maxLength: 8 }),
        literalArb,
        (parts, tail) => {
          const template = parts.map((p) => `${p.lit}{${p.name}}`).join('') + tail;
          const expectedNames: string[] = [];
          for (const p of parts) {
            if (!expectedNames.includes(p.name)) expectedNames.push(p.name);
          }
          expect(templateVars(template)).toEqual(expectedNames);

          const vars = Object.fromEntries(expectedNames.map((n, k) => [n, `V${k}`]));
          const rendered = renderTemplate(template, vars);
          const manual =
            parts.map((p) => `${p.lit}V${expectedNames.indexOf(p.name)}`).join('') + tail;
          expect(rendered).toBe(manual);
        },
      ),
    );
  });
});

describe('prompt (tagged) runtime', () => {
  it('interpolation names pull from the vars object', () => {
    const qa = prompt`Answer ${'question'} using ${'context'}.`;
    expect(qa({ question: 'why?', context: 'because' })).toBe('Answer why? using because.');
  });

  it('braces are literal — no escape rules in the tagged form', () => {
    const fixed = prompt`Return {"a": 1} verbatim.`;
    expect(fixed()).toBe('Return {"a": 1} verbatim.');
  });

  it('the same name may appear twice', () => {
    const twice = prompt`${'x'} and ${'x'}`;
    expect(twice({ x: 'y' })).toBe('y and y');
  });
});
