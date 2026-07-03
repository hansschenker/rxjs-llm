# ADR-0014: The splitter is a pure, lossless transformer with offsets

**Status:** accepted · **Decision:** D4.2

## Context

Hand-rolled RAG splitters are where silent bugs live: dropped separators,
chunks over budget after overlap is added, split surrogate pairs that
corrupt UTF-8 on the way to an embedder.

## Decision

`splitText(source, opts): TextChunk[]` is pure and recursive — paragraph →
line → sentence → word → code point — with three enforced invariants,
each a fast-check property:

1. **Lossless with offsets.** Every chunk owns an exact `[start, end)`
   span; owned spans *partition* the source (separators stay attached to
   the preceding piece). Reconstruction is `slice(start, end).join('') ===
   source` — exact, not approximate — and offsets double as citation
   anchors.
2. **Budget includes overlap.** `text` = overlap prefix + owned span, and
   the whole thing must fit `maxTokens`; owned spans are packed to
   `maxTokens - overlap`. Greedy packing compares the *sum* of fragment
   counts, which is conservative for any subadditive tokenizer.
3. **No split inside a surrogate pair.** Code-point stepping at the
   force-split level and in the overlap walk-back.

Token counting sits behind `Tokenizer` (one method). Default:
`charEstimator` (chars/4) — dependency-free. **tiktoken is not shipped**:
the plan allowed it behind a dynamic import, but the interface is the seam
and a consumer-side tokenizer drops in without this package knowing;
shipping none keeps the zero-dependency core intact.

`splitDocs(opts)` is the operator form (`mergeMap`): `Doc` in, `Chunk` out
with `id = docId#index` and inherited metadata.

## Consequences

- Chunk ids are deterministic, so re-ingesting a document upserts over its
  previous chunks rather than duplicating them.
- The overlap prefix is context, not ownership — dedup/reconstruction
  logic uses offsets and never sees overlap twice.
