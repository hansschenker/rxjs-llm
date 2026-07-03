import { describe, expectTypeOf, it } from 'vitest';
import { prompt, promptTemplate, type ExtractVars } from '../../src/prompt/template';

// Review-note discipline: toEqualTypeOf ONLY — toMatchTypeOf would let a
// silent degradation to Record<string, string | number> pass undetected.

describe('promptTemplate types (D2.2)', () => {
  it('extracts the exact parameter type from a plain string literal — no as const', () => {
    const summarize = promptTemplate('Summarize {doc} in {n} bullets, plain language.');
    expectTypeOf(summarize)
      .parameter(0)
      .toEqualTypeOf<{ doc: string | number; n: string | number }>();
    expectTypeOf(summarize).returns.toEqualTypeOf<string>();
  });

  it('missing and extra keys are compile errors', () => {
    const summarize = promptTemplate('Summarize {doc} in {n} bullets.');
    // @ts-expect-error — 'n' is missing
    summarize({ doc: 'text' });
    // @ts-expect-error — 'extra' is not a placeholder
    summarize({ doc: 'text', n: 3, extra: true });
  });

  it('handles {{ escapes before the variable branch: {{foo}} demands no key', () => {
    expectTypeOf<ExtractVars<'all {{foo}} literal'>>().toEqualTypeOf<never>();
    const none = promptTemplate('all {{foo}} literal');
    expectTypeOf(none).toEqualTypeOf<() => string>();
    // @ts-expect-error — a fully-escaped template takes no argument
    none({ foo: 'x' });

    const mixed = promptTemplate('Return {{json}} with {value}');
    expectTypeOf(mixed).parameter(0).toEqualTypeOf<{ value: string | number }>();
  });

  it('an unclosed brace contributes no key', () => {
    expectTypeOf<ExtractVars<'broken {oops'>>().toEqualTypeOf<never>();
    expectTypeOf<ExtractVars<'{a} then {oops'>>().toEqualTypeOf<'a'>();
  });

  it('repeated placeholders collapse to one key', () => {
    const twice = promptTemplate('{x} and {x} again');
    expectTypeOf(twice).parameter(0).toEqualTypeOf<{ x: string | number }>();
  });
});

describe('prompt (tagged) types', () => {
  it('interpolations become the exact parameter type', () => {
    const qa = prompt`Answer ${'question'} using only ${'context'}.`;
    expectTypeOf(qa)
      .parameter(0)
      .toEqualTypeOf<{ question: string | number; context: string | number }>();
    // @ts-expect-error — 'context' is missing
    qa({ question: 'hi' });
  });

  it('no interpolations builds a zero-argument function; braces are literal', () => {
    const fixed = prompt`Respond with {json} braces kept literal.`;
    expectTypeOf(fixed).toEqualTypeOf<() => string>();
  });
});
