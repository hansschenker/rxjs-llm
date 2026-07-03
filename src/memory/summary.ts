import {
  asapScheduler,
  BehaviorSubject,
  catchError,
  combineLatest,
  EMPTY,
  endWith,
  exhaustMap,
  filter,
  finalize,
  ignoreElements,
  map,
  observeOn,
  takeUntil,
  type Observable,
} from 'rxjs';
import { promptTemplate } from '../prompt/template';
import type { ChatModel } from '../types';
import type { MemoryView, Turn } from './core';
import { turnsToMessages } from './views';

export interface SummaryViewOptions {
  /** Fold when more than this many turns are un-summarized. Default 8. */
  foldAfter?: number;
  /** Recent turns always kept verbatim (never folded). Default 2. */
  keepRecent?: number;
}

export type SummaryPrompt = (vars: { summary: string; turns: string }) => string;

const defaultPrompt: SummaryPrompt = promptTemplate(
  'You maintain a running summary of a conversation.\n\n' +
    'Current summary:\n{summary}\n\n' +
    'New conversation turns:\n{turns}\n\n' +
    'Write the updated summary. Preserve facts, names, decisions, and open ' +
    'questions; be concise. Respond with ONLY the summary text.',
);

/**
 * Summary memory is an async fold (decision D5.2, ADR-0022). When the
 * un-summarized tail exceeds `foldAfter`, one LLM call folds it into the
 * running summary. The fold is:
 *
 * - EVENTUALLY CONSISTENT: view() serves the raw tail while a fold is in
 *   flight; `pending$` exposes the window. The conversation never blocks
 *   on summarization.
 * - NEVER OVERLAPPING: exhaustMap ignores triggers while a fold runs; the
 *   state update re-evaluates the threshold, so a backlog folds again
 *   immediately after — sequentially.
 * - FAILURE-TOLERANT: a fold error keeps the previous state, so the view
 *   keeps serving the raw turns — degraded to fullView, never amnesia.
 *   The next record() retries.
 * - ABORTABLE: memory.dispose() completes the turn stream, which cancels
 *   an in-flight fold (the model call's teardown fires) and completes
 *   `pending$`.
 *
 * One summaryView instance binds to ONE memory — it carries fold state.
 */
export function summaryView(
  model: ChatModel,
  prompt: SummaryPrompt = defaultPrompt,
  options: SummaryViewOptions = {},
): MemoryView & { pending$: Observable<boolean> } {
  const foldAfter = options.foldAfter ?? 8;
  const keepRecent = options.keepRecent ?? 2;
  if (!Number.isInteger(foldAfter) || foldAfter < 1) {
    throw new RangeError(`foldAfter must be ≥ 1, got ${foldAfter}`);
  }
  if (!Number.isInteger(keepRecent) || keepRecent < 0 || keepRecent > foldAfter) {
    throw new RangeError(`keepRecent must be in [0, foldAfter], got ${keepRecent}`);
  }

  const pending = new BehaviorSubject<boolean>(false);
  let bound = false;

  const view: MemoryView = (turns$) => {
    if (bound) throw new Error('a summaryView binds to one memory; create another instance');
    bound = true;

    const state = new BehaviorSubject<{ summary: string; folded: number }>({
      summary: '',
      folded: 0,
    });
    const done$ = turns$.pipe(ignoreElements(), endWith(true));

    combineLatest([turns$, state])
      .pipe(
        // async trigger delivery: the post-fold state update fires while
        // the fold's inner is still tearing down, and a synchronous
        // re-trigger would land inside exhaustMap's "occupied" window and
        // be silently discarded — the backlog would never re-fold
        observeOn(asapScheduler),
        filter(([turns, s]) => turns.length - s.folded > foldAfter),
        exhaustMap(([turns, s]) => {
          const upTo = turns.length - keepRecent;
          const tail = turns.slice(s.folded, upTo);
          pending.next(true);
          return model
            .complete([
              {
                role: 'user',
                content: prompt({
                  summary: s.summary === '' ? '(none yet)' : s.summary,
                  turns: renderTurns(tail),
                }),
              },
            ])
            .pipe(
              map((completion) => ({ summary: completion.text.trim(), folded: upTo })),
              catchError(() => EMPTY), // keep previous state; view stays raw
              finalize(() => pending.next(false)),
            );
        }),
        takeUntil(done$), // dispose() aborts the in-flight fold
      )
      .subscribe({
        next: (next) => state.next(next),
        complete: () => {
          state.complete();
          // takeUntil completes downstream BEFORE upstream teardown runs
          // the inner's finalize — emit the final false ourselves, or
          // pending$ would end on true after a mid-fold dispose
          pending.next(false);
          pending.complete();
        },
      });

    return combineLatest([turns$, state]).pipe(
      map(([turns, s]) => {
        const messages = turnsToMessages(turns.slice(s.folded));
        if (s.summary === '') return messages;
        return [
          {
            role: 'system' as const,
            content: `Summary of the earlier conversation:\n${s.summary}`,
          },
          ...messages,
        ];
      }),
    );
  };

  return Object.assign(view, { pending$: pending.asObservable() });
}

function renderTurns(turns: readonly Turn[]): string {
  return turns.map((turn) => `User: ${turn.user}\nAssistant: ${turn.assistant}`).join('\n\n');
}
