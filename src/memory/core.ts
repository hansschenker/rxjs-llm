import {
  BehaviorSubject,
  scan,
  shareReplay,
  Subject,
  Subscription,
  type Observable,
} from 'rxjs';
import type { ChatMessage } from '../types.js';
import { fullView } from './views.js';

/** One conversational exchange — the unit the reducer folds over. */
export interface Turn {
  user: string;
  assistant: string;
}

/**
 * A view is a stream transform (decision D5.1, ADR-0020): it projects the
 * accumulated turn state into the ChatMessage[] a prompt's history slot
 * wants. Pure views are a `map`; the summary view builds an async fold on
 * the same input. Swapping strategies never touches the reducer.
 */
export type MemoryView = (turns$: Observable<readonly Turn[]>) => Observable<ChatMessage[]>;

/** Serializable state: the turns are the truth; views are projections. */
export interface MemorySnapshot {
  turns: Turn[];
}

export interface Memory {
  record(turn: Turn): void;
  /** Reactive: existing subscribers receive an updated view per record(). */
  view(): Observable<ChatMessage[]>;
  snapshot(): MemorySnapshot;
  /** Ends the memory: completes the view and aborts any in-flight fold. */
  dispose(): void;
}

export interface MemoryOptions {
  /** Default: fullView(). */
  view?: MemoryView;
  /** Start from a snapshot instead of empty (D5.4 — hosts persist however they like). */
  restore?: MemorySnapshot;
}

/**
 * Memory is a reducer plus a view (decision D5.1): a `scan` over the turn
 * stream accumulates state; the strategy projects it. The view pipeline is
 * primed at creation with an internal keep-alive subscription so async
 * views (the summary fold) make progress even while nothing observes them
 * — that is what "never block the conversation on summarization" costs.
 */
export function createMemory(options: MemoryOptions = {}): Memory {
  const viewFn = options.view ?? fullView();
  const input = new Subject<Turn>();
  const initial: readonly Turn[] = options.restore?.turns.slice() ?? [];
  const state = new BehaviorSubject<readonly Turn[]>(initial);

  const lifetime = new Subscription();
  lifetime.add(
    input
      .pipe(scan((turns, turn) => [...turns, turn], initial))
      .subscribe((turns) => state.next(turns)),
  );

  const messages$ = viewFn(state.asObservable()).pipe(
    shareReplay({ bufferSize: 1, refCount: false }),
  );
  lifetime.add(messages$.subscribe()); // prime + keep alive

  return {
    record: (turn) => input.next(turn),
    view: () => messages$,
    snapshot: () => ({ turns: [...state.value] }),
    dispose: () => {
      state.complete(); // views see completion; summary folds take this as abort
      input.complete();
      lifetime.unsubscribe();
    },
  };
}
