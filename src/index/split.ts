import { mergeMap, type OperatorFunction } from 'rxjs';
import type { Chunk, Doc, TextChunk } from './types';

/**
 * Token counting behind an interface (decision D4.2, ADR-0014). The default
 * is the classic chars/4 estimator — cheap, dependency-free, and good
 * enough for budget enforcement. A tiktoken-backed implementation is a
 * consumer-side drop-in; the interface is the seam.
 */
export interface Tokenizer {
  count(text: string): number;
}

export const charEstimator: Tokenizer = {
  count: (text) => Math.ceil(text.length / 4),
};

export interface SplitOptions {
  /** Hard budget per chunk, INCLUDING the overlap prefix. */
  maxTokens: number;
  /** Tokens of context repeated from the previous chunk. Default 0. */
  overlap?: number;
  tokenizer?: Tokenizer;
}

/** Coarse→fine boundaries: paragraph, line, sentence, word, code point. */
const SEPARATORS = ['\n\n', '\n', '. ', ' '] as const;

/**
 * Pure recursive splitter (decision D4.2). Lossless by construction: every
 * chunk owns an exact [start, end) span of the source and the owned spans
 * partition it — `chunks.map(c => source.slice(c.start, c.end)).join('')`
 * IS the source. `text` prepends up to `overlap` tokens of preceding
 * context to the owned span; budget checks include that prefix. No span
 * boundary ever lands inside a surrogate pair.
 */
export function splitText(source: string, options: SplitOptions): TextChunk[] {
  const overlap = options.overlap ?? 0;
  const tokenizer = options.tokenizer ?? charEstimator;
  if (!Number.isInteger(options.maxTokens) || options.maxTokens < 1) {
    throw new RangeError(`maxTokens must be a positive integer, got ${options.maxTokens}`);
  }
  if (overlap < 0 || overlap >= options.maxTokens) {
    throw new RangeError(`overlap must be in [0, maxTokens), got ${overlap}`);
  }
  if (source.length === 0) return [];

  const ownedBudget = options.maxTokens - overlap;
  const fragments = fragment(source, 0, ownedBudget, tokenizer);

  // Greedy packing. Sum-of-counts is intentionally conservative: for any
  // subadditive tokenizer (chars/4 included), count(a+b) ≤ count(a)+count(b),
  // so a packed span never exceeds the budget its parts passed.
  const spans: { start: number; end: number }[] = [];
  let start = 0;
  let length = 0;
  let tokens = 0;
  for (const piece of fragments) {
    const pieceTokens = tokenizer.count(piece);
    if (length > 0 && tokens + pieceTokens > ownedBudget) {
      spans.push({ start, end: start + length });
      start += length;
      length = 0;
      tokens = 0;
    }
    length += piece.length;
    tokens += pieceTokens;
  }
  if (length > 0) spans.push({ start, end: start + length });

  return spans.map((span, index) => ({
    index,
    start: span.start,
    end: span.end,
    text: source.slice(overlapStart(source, span.start, overlap, tokenizer), span.end),
  }));
}

/** Split into fragments each ≤ budget tokens, refining separator by level. */
function fragment(text: string, level: number, budget: number, tokenizer: Tokenizer): string[] {
  if (tokenizer.count(text) <= budget) return [text];
  const separator = SEPARATORS[level];
  if (separator === undefined) return splitByCodePoints(text, budget, tokenizer);
  const parts = splitKeepingSeparator(text, separator);
  if (parts.length === 1) return fragment(text, level + 1, budget, tokenizer);
  return parts.flatMap((part) => fragment(part, level + 1, budget, tokenizer));
}

/** Split on a separator, keeping it attached to the preceding part — lossless. */
function splitKeepingSeparator(text: string, separator: string): string[] {
  const parts: string[] = [];
  let from = 0;
  for (;;) {
    const at = text.indexOf(separator, from);
    if (at === -1) break;
    parts.push(text.slice(from, at + separator.length));
    from = at + separator.length;
  }
  if (from < text.length) parts.push(text.slice(from));
  return parts;
}

/** Last-resort split for budget-exceeding words: whole code points only. */
function splitByCodePoints(text: string, budget: number, tokenizer: Tokenizer): string[] {
  const pieces: string[] = [];
  let current = '';
  for (const codePoint of text) {
    if (current !== '' && tokenizer.count(current + codePoint) > budget) {
      pieces.push(current);
      current = '';
    }
    current += codePoint;
  }
  if (current !== '') pieces.push(current);
  return pieces;
}

/** Walk back from `start` gathering up to `overlap` tokens of context,
 * stepping by code points so no surrogate pair is ever split. */
function overlapStart(
  source: string,
  start: number,
  overlap: number,
  tokenizer: Tokenizer,
): number {
  if (overlap === 0 || start === 0) return start;
  let at = start;
  for (;;) {
    let step = 1;
    const previous = source.charCodeAt(at - 1);
    if (previous >= 0xdc00 && previous <= 0xdfff && at >= 2) step = 2; // low surrogate
    const candidate = at - step;
    if (candidate < 0) break;
    if (tokenizer.count(source.slice(candidate, start)) > overlap) break;
    at = candidate;
    if (at === 0) break;
  }
  return at;
}

/** The operator form (decision D4.2): Doc in, attributed Chunks out. */
export function splitDocs(options: SplitOptions): OperatorFunction<Doc, Chunk> {
  return mergeMap((doc: Doc) =>
    splitText(doc.text, options).map((chunk) => ({
      ...chunk,
      id: `${doc.id}#${chunk.index}`,
      docId: doc.id,
      metadata: doc.metadata,
    })),
  );
}
