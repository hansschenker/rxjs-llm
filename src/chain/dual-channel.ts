import {
  asapScheduler,
  Observable,
  ReplaySubject,
  Subject,
  subscribeOn,
  type Subscription,
} from 'rxjs';

export interface DualChannel<Out, Event> {
  result$: Observable<Out>;
  progress$: Observable<Event>;
}

export interface DualChannelConfig<Out, Event> {
  /** Build the execution; `emit` feeds progress$. Called at most once. */
  work: (emit: (event: Event) => void) => Observable<Out>;
  terminal: {
    complete: () => Event;
    error: (error: unknown) => Event;
  };
}

/**
 * The dual-channel contract (D3.3, ADR-0006), extracted so chains and
 * agents (D6.4, ADR-0026) share ONE audited implementation:
 *
 * 1. Passivity — progress$ is a plain Subject fed by whatever execution
 *    result$ drives; subscribing to it alone triggers nothing.
 * 2. Lifecycle — natural termination pushes exactly one terminal event
 *    onto progress$ (complete | error variant, a next not an error
 *    notification) and then completes it; the error OBJECT travels on
 *    result$ only. Cancellation completes both channels silently.
 * 3. Unobserved progress events are dropped (no replay — a UI channel).
 * 4. One work() = one logical execution, outcome latched permanently.
 *    The latch is hand-rolled (share()'s reset flags cannot express
 *    abort-on-abandonment AND latch-every-outcome; ADR-0006 §4) and
 *    first-writer-wins: `settled` guards both directions.
 *
 * The cancellation DECISION is deferred one microtask: firstValueFrom-
 * style consumers unsubscribe synchronously inside the final value's
 * delivery — between the source's next and complete — and cancelling
 * there would abort an execution that is mid-completion, misreporting
 * success as cancellation.
 *
 * Work is subscribed on the asap scheduler: even a fully-synchronous
 * execution cannot emit in the first subscriber's call frame (no Zalgo).
 */
export function dualChannel<Out, Event>(config: DualChannelConfig<Out, Event>): DualChannel<Out, Event> {
  const progress = new Subject<Event>();
  const output = new ReplaySubject<Out>(1);
  let subscriberCount = 0;
  let started = false;
  let settled = false;
  let execution: Subscription | undefined;

  const start = (): void => {
    started = true;
    execution = config
      .work((event) => progress.next(event))
      .pipe(subscribeOn(asapScheduler))
      .subscribe({
        next: (value) => output.next(value),
        error: (error: unknown) => {
          settled = true;
          progress.next(config.terminal.error(error));
          progress.complete();
          output.error(error);
        },
        complete: () => {
          settled = true;
          progress.next(config.terminal.complete());
          progress.complete();
          output.complete();
        },
      });
  };

  const result$ = new Observable<Out>((subscriber) => {
    subscriberCount += 1;
    const delivery = output.subscribe(subscriber);
    if (!started) start();
    return () => {
      subscriberCount -= 1;
      delivery.unsubscribe();
      if (subscriberCount === 0 && !settled) {
        queueMicrotask(() => {
          if (subscriberCount === 0 && !settled) {
            settled = true; // cancelled: latch so nothing ever re-executes
            execution?.unsubscribe(); // aborts in-flight work
            progress.complete(); // silent — no terminal event (ADR-0005)
            output.complete(); // late subscribers: immediate empty completion
          }
        });
      }
    };
  });

  return { result$, progress$: progress.asObservable() };
}
