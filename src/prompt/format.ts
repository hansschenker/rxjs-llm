/**
 * Output-format helpers (Module 2, Phase 3): pure string transformers that
 * append format instructions to a rendered prompt. Compose by nesting —
 * `noJargon()(asBullets(3)(text))` — or with any pipe utility; there is no
 * combinator framework here on purpose (D2.3, ADR-0009).
 */

export type FormatInstruction = (text: string) => string;

export function asBullets(n: number): FormatInstruction {
  return (text) => `${text}\n\nRespond with exactly ${n} bullet points, one line each.`;
}

export function noJargon(): FormatInstruction {
  return (text) =>
    `${text}\n\nUse plain language a non-specialist understands; avoid jargon.`;
}

/** The transformer keeps its schema so a later stage can parse against it. */
export interface JsonFormat extends FormatInstruction {
  schema: Record<string, unknown>;
}

/**
 * Renders the JSON Schema into the prompt and carries it on the returned
 * transformer for later parsing (Module 6 consumes `.schema`). Takes plain
 * JSON Schema — zod stays out of this package until Module 6's ADR admits
 * it; zod users pass their schema through zod-to-json-schema unchanged.
 */
export function asJson(schema: Record<string, unknown>): JsonFormat {
  return Object.assign(
    (text: string) =>
      `${text}\n\nRespond with ONLY valid JSON matching this JSON Schema — no prose, no code fences:\n${JSON.stringify(schema, null, 2)}`,
    { schema },
  );
}
