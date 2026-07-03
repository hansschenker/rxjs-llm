# ADR-0021: Token budget is a view concern

**Status:** accepted · **Decision:** D5.3

## Context

Fitting history into a context window is about what the model SEES, not
what the conversation IS. Trimming at record time would destroy history
that a later, larger-budget view could have used.

## Decision

`tokenBudgetView(budget, tokenizer?)` walks history newest-first, keeping
whole turns while they fit and stopping at the first that does not. Three
consequences, property-tested:

- rendered turns always fit the budget (sum of per-turn costs);
- eviction is oldest-first AND turn-atomic — the kept turns are always a
  *suffix* of history, and messages stay user/assistant paired (an
  orphaned user message or split turn never reaches the model);
- a single turn larger than the budget yields an empty history rather
  than a fragment.

Token counting reuses Module 4's `Tokenizer` seam (`charEstimator`
default) — one counting interface across splitter, retriever, and memory,
so a consumer's tiktoken drop-in fixes all three at once.

## Consequences

- The full history remains in the reducer regardless of the active view's
  budget; switching views recovers it.
- Stopping at the first non-fitting turn (rather than skipping it and
  trying older ones) is deliberate: history must stay contiguous —
  a gap in the middle of a conversation misleads the model more than a
  shorter window.
