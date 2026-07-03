# ADR-0022: Summary memory is an async fold, eventually consistent

**Status:** accepted · **Decision:** D5.2

## Context

Summarizing history costs an LLM call. If the conversation waits for it,
every Nth turn stalls by a full model round-trip; if folds overlap, two
summaries race and one clobbers the other's foldedness.

## Decision

`summaryView(model, prompt?, { foldAfter, keepRecent })` is a MemoryView
carrying its own fold state (`{ summary, folded }`) — which is why one
instance binds to exactly ONE memory (enforced). When the un-summarized
tail exceeds `foldAfter`, one `model.complete()` call (a Module 2 prompt)
folds all but the `keepRecent` newest turns into the running summary.

Concurrency semantics, each pinned by test:

- **Eventually consistent.** `view()` serves the raw tail while a fold is
  in flight; the summary lands when it lands. `pending$` exposes the
  window. The conversation NEVER blocks on summarization.
- **Never overlapping.** `exhaustMap` on the trigger stream — and the
  trigger is delivered via `observeOn(asap)`, because the post-fold state
  update fires while the fold's inner is still tearing down; a synchronous
  re-trigger would land inside exhaustMap's occupied window and be
  silently discarded, so a backlog would never re-fold. (Found by test.)
- **Failure-tolerant.** A fold error keeps the previous state: the view
  degrades to serving raw turns — never amnesia — and the next `record()`
  retries. `catchError(() => EMPTY)` is the whole policy.
- **Abortable.** `dispose()` completes the turn stream; `takeUntil` on the
  fold chain cancels the in-flight model call (its teardown fires) and
  `pending$` ends on `false` then completes. The final `false` is emitted
  by the complete handler, not the inner's `finalize` — takeUntil
  completes downstream before upstream teardown runs. (Also found by
  test.)

## Consequences

- Turns recorded during a fold are simply un-summarized tail; they render
  verbatim until the next fold sweeps them.
- The summary renders as a leading system message
  (`Summary of the earlier conversation: …`) ahead of the verbatim tail —
  the shape `withHistory()` splices without special cases.
