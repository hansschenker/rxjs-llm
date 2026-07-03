/**
 * Compile-time-checked prompt templates (decision D2.2, ADR-0007).
 *
 * Two forms, two exports — deliberately NOT one overloaded function
 * (ADR-0008; the streamTimeout overload incident is the precedent):
 *
 * - `promptTemplate('Summarize {doc} in {n} bullets')` — parsed form.
 *   Placeholder names are extracted at the type level, so the returned
 *   function demands exactly `{ doc, n }`. Braces escape as `{{` / `}}`.
 * - `prompt\`Summarize ${'doc'} in ${'n'} bullets\`` — tagged form.
 *   Interpolations ARE the placeholder names; braces need no escaping.
 *
 * Both are pure: (vars) => string, no I/O, no Observables (D2.3).
 */

/**
 * Type-level mirror of the runtime grammar: scan to the first `{`, then —
 * ORDER MATTERS — check the `{{` escape BEFORE the variable branch, so
 * `{{foo}}` contributes no key. An unclosed `{` is literal tail, no key.
 * The runtime scanner in renderTemplate implements exactly this grammar;
 * the property test pins the symmetry.
 */
export type ExtractVars<T extends string> = T extends `${infer _Pre}{${infer Tail}`
  ? Tail extends `{${infer Rest}`
    ? ExtractVars<Rest> // '{{' — escaped literal brace, not a variable
    : Tail extends `${infer Name}}${infer Rest}`
      ? Name | ExtractVars<Rest>
      : never // unclosed '{' — literal, no variable
  : never;

export type PromptVars<V extends string> = { [K in V]: string | number };

/** A template with no placeholders builds a zero-argument function. */
export type PromptFn<V extends string> = [V] extends [never]
  ? () => string
  : (vars: PromptVars<V>) => string;

/**
 * Runtime interpolation over the same grammar as ExtractVars:
 * `{name}` → String(vars[name]); `{{` → `{`; `}}` → `}`; unclosed `{` and
 * lone `}` are literal. Values are never re-scanned — braces in a value
 * pass through untouched.
 */
export function renderTemplate(
  template: string,
  vars: Record<string, string | number>,
): string {
  let out = '';
  let i = 0;
  while (i < template.length) {
    const ch = template[i]!;
    if (ch === '{') {
      if (template[i + 1] === '{') {
        out += '{';
        i += 2;
        continue;
      }
      const close = template.indexOf('}', i + 1);
      if (close === -1) {
        out += template.slice(i); // unclosed — literal tail
        break;
      }
      out += String(vars[template.slice(i + 1, close)]);
      i = close + 1;
    } else if (ch === '}') {
      out += '}';
      i += template[i + 1] === '}' ? 2 : 1; // '}}' collapses to '}'
    } else {
      out += ch;
      i += 1;
    }
  }
  return out;
}

/** Runtime twin of ExtractVars: placeholder names in first-occurrence order, deduped. */
export function templateVars(template: string): string[] {
  const names: string[] = [];
  let i = 0;
  while (i < template.length) {
    if (template[i] === '{') {
      if (template[i + 1] === '{') {
        i += 2;
        continue;
      }
      const close = template.indexOf('}', i + 1);
      if (close === -1) break;
      const name = template.slice(i + 1, close);
      if (!names.includes(name)) names.push(name);
      i = close + 1;
    } else {
      i += 1;
    }
  }
  return names;
}

/**
 * Parsed-string form. The `<T extends string>` generic is load-bearing:
 * it preserves the literal type without `as const`, which is what makes
 * ExtractVars<T> see the actual placeholder names.
 */
export function promptTemplate<T extends string>(template: T): PromptFn<ExtractVars<T>> {
  return ((vars?: Record<string, string | number>) =>
    renderTemplate(template, vars ?? {})) as PromptFn<ExtractVars<T>>;
}

/**
 * Tagged-literal form: interpolations are the placeholder NAMES.
 * `const` on the type parameter preserves them as a literal tuple.
 * No escape rules here — braces in the text are always literal.
 */
export function prompt<const Names extends readonly string[]>(
  strings: TemplateStringsArray,
  ...names: Names
): PromptFn<Names[number]> {
  return ((vars?: Record<string, string | number>) => {
    const v = vars ?? {};
    let out = strings[0] ?? '';
    for (let i = 0; i < names.length; i += 1) {
      out += String(v[names[i]!]) + (strings[i + 1] ?? '');
    }
    return out;
  }) as PromptFn<Names[number]>;
}
