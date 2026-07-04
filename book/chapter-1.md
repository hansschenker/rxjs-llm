<!-- Draft 1 — generated from the v0.6.0 history. The commit log is the outline:
     git log --reverse --oneline -->

# One Interface, Two Channels

*LangChain in 3,100 lines — part I*

---

## The bet

Every LLM orchestration framework is an async coordination library in disguise.

Strip the branding from LangChain and look at what it actually does: it streams
tokens from an HTTP response, retries failed requests with backoff, times out
stalled connections, cancels in-flight work, fans out parallel calls, folds
streams of deltas into final values, accumulates conversation state, and runs a
recursive tool-calling loop. Every one of those is an async coordination
problem — and every one of them was solved, tested, and hardened in RxJS years
before anyone streamed a token from a language model.

So here is the bet these two chapters make: if you build LLM orchestration
*directly on RxJS* — no `Runnable` abstraction, no callback manager, no
framework layer between you and the streams — the entire feature set of a
LangChain-class library fits in about 3,100 non-blank lines of strict
TypeScript, with exactly two runtime dependencies: `rxjs` and `zod`.

The result is `rxjs-llm`: six modules, 280 tests, 27 recorded design decisions,
and one capstone test that composes all six modules into a single pipeline over
real HTTP. These chapters walk the build in the order it happened, because the
repository was written to be read that way — each implementation phase is one
commit, and the history is the tutorial:

```
$ git log --reverse --oneline
6264c39 Module 1, Phase 1: scaffold, StreamEvent taxonomy, error taxonomy
01cfed4 Module 1, Phase 2: transport — fetchStream and the SSE parser
67191ad Module 1, Phase 3: Anthropic adapter — full Messages API event mapping
7c0be52 Module 1, Phase 4: OpenAI + Ollama adapters, NDJSON framing
40a76fc Module 1, Phase 5: resilience operators — retry, timeouts, rate limit
0a714e4 Module 1, Phase 6: mock provider server, integration tests, README — v0.1.0
b25f319 Plans: ADR checklist for D3.3 (progress$ channel review points)
5761aa2 Plans: resolve D3.3 checklist points 2 and 4, add the pinning test
e956ddb Plans: latch all three share() reset flags in D3.3 — retry() must not re-run
765c0b9 Module 3, D3.3 pulled forward: the dual-channel run() — { result$, progress$ }
b6d424d D3.3 audit: pin the latch race and success-value identity
4bb3808 Module 2, Phase 1: placeholder type machinery — ExtractVars, two prompt forms
558115c Module 2, Phase 2: message builders, messagePrompt, the withHistory slot
48f1fcf Module 2, Phase 3: format helpers — asBullets, noJargon, asJson
72f0186 Module 2, Phase 4: README prompt section, STATUS — v0.2.0
8fc9f12 Module 3, Phase 1 wrap-up: ADRs for context threading and stage-as-operator
370f85e Module 3, Phase 2: traced() + TraceSink — and the third latch race
40a8e21 Module 3, Phase 4: stages.parallel and stage.when
4241a29 Module 3, Phase 5: error policies — fail | skip | fallback, stage tagging
17fadbb Module 3, Phase 6: end-to-end proof, README — v0.3.0
f28a76c Module 4, Phase 1: the splitter — pure, lossless, offset-carrying
6dfbd3f Module 4, Phase 2: in-memory vector store + the store contract suite
f86fafa Module 4, Phase 3: Embedder interface, adapters, and the batching operator
a2e2361 Module 4, Phase 4: loaders — text-file, web, json
02eab7b Module 4, Phase 5: PGlite + Drizzle store, contract suite generalized
efdbd47 Module 4, Phase 6: retrieveContext, ingest pipeline, e2e — v0.4.0
3a12a9a Module 5, Phases 1+2: memory core, full/window views, token-budget view
f4f4475 Module 5, Phase 3: the summary view — an async fold, eventually consistent
818f927 Module 5, Phase 4: snapshot/restore, README — v0.5.0
51c6eec Module 6, Phase 1: Zod-defined tools — validation, self-correction, safe execution
b75be41 Module 6, Phase 2: the expand() loop, and the dual channel extracted
9f260ed Module 6, Phase 3: safety rails — budget, timeout, cap, and the cancellation matrix
f863b8e Module 6, Phase 4: the capstone — six modules, one pipeline, real HTTP
2a79d18 Module 6, Phase 5: README walkthrough, governance — v0.6.0
```

Notice something odd in that log: Module 3's dual-channel `run()` lands *before*
Module 2 starts. The history is honest — a design review mid-project decided
that one decision could not wait, and we will get to why. But the log also
shows the discipline that made the project small: every design decision marked
**D-n** in the plan got an Architecture Decision Record before the code
shipped, a `NON_GOALS.md` file said no to everything that wasn't orchestration,
and no runtime dependency entered the tree without an ADR justifying it. Only
one ever did.

One sentence of orientation per module:

1. **Model interface** — one `ChatModel` for every provider, with laws.
2. **Prompts** — templates whose placeholders the *compiler* checks.
3. **Chains** — stages are operators; chains are `pipe()`.
4. **Indexes/RAG** — retrieval as a pipeline of small pure parts.
5. **Memory** — a reducer plus swappable views; ~150 lines.
6. **Agents** — the tool loop is `expand()`; safety as values, not exceptions.

This chapter builds the foundation everything else stands on: the model
interface and its laws, and the two-channel contract that every module from 3
onward will consume. The next chapter builds the remaining five modules and
composes all six into one pipeline.

---

## Module 1 — One interface for every model

Everything downstream depends on one idea: *a streaming LLM response is an
Observable*. Not "can be wrapped in one" — it structurally *is* one. It's a
sequence of typed events over time that either completes, errors, or gets
cancelled, produced lazily by a subscription that maps one-to-one onto an HTTP
request. That is the definition of a cold Observable.

So the uniform interface is two methods, and the interesting part is the
contract in the comment:

```ts
// src/types.ts
/**
 * Contract — enforced by law tests, not convention:
 * - cold: each subscribe issues exactly one HTTP request
 * - lazy: no fetch before subscribe, nothing emits in the subscribe call frame
 * - unicast: subscribers never share a request
 * - teardown-complete: unsubscribe aborts the underlying fetch
 * - cancellation is silent teardown; it never surfaces on the error channel
 */
export interface ChatModel {
  stream(messages: ChatMessage[], options?: ChatOptions): Observable<StreamEvent>;
  complete(messages: ChatMessage[], options?: ChatOptions): Observable<ChatCompletion>;
}
```

"Enforced by law tests, not convention" is the phrase to hold onto. Every
Observable-returning API in this codebase is covered by the same battery:
subscribe once and assert exactly one request left the building; construct the
Observable and assert *nothing* happened before subscribe; subscribe
synchronously and assert nothing was emitted in the same call frame (the
classic Zalgo test); unsubscribe mid-stream and assert the `AbortSignal` on the
server side actually fired. When Module 4's `Embedder` and Module 6's tool
execution show up later, they inherit this battery wholesale. Laws you test
once per contract are laws you never debug in production.

### The normalization boundary

Providers do not agree on anything. Anthropic streams Server-Sent Events with
`content_block_delta` frames; OpenAI streams SSE with delta fragments and a
literal `[DONE]` sentinel; Ollama streams newline-delimited JSON. The adapter
layer's whole job is to translate all of that into one discriminated union —
and after Module 1, no other line of the codebase ever sees a wire format
again:

```ts
// src/types.ts
export type StreamEvent =
  | { type: 'message_start'; model: string }
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_delta'; id: string; name?: string; argsDelta: string }
  | { type: 'usage'; input: number; output: number }
  | { type: 'message_stop'; stopReason: StopReason };
```

Two deliberate choices are hiding in that union. First, `tool_call_delta` is
there from day one, five modules before agents exist — because Anthropic and
OpenAI encode tool-call deltas *incompatibly*, and retrofitting them into a
taxonomy that all of Modules 3–6 already consume would have meant touching
everything. Second, tool-call arguments arrive as `argsDelta: string`
fragments, not parsed objects, because that is the truth of the wire: the model
streams JSON *text*, and pretending otherwise pushes parsing failures into the
wrong layer.

### Transport: bytes → frames → events

Under the adapters sits a small transport stack, and its layering was validated
by an accident of the provider landscape. `fetchStream` turns a `fetch` into
`Observable<Uint8Array>`, wiring the `AbortController` to teardown. Above it
sit two framing strategies: an SSE parser for Anthropic and OpenAI, and an
NDJSON parser for Ollama. Needing a *second* framing strategy is what proves
the layer boundary is real — if SSE parsing had been fused into the fetch
layer, Ollama would have forced a rewrite.

The SSE parser's test suite is deliberately hostile. Real HTTP chunk boundaries
fall wherever the network pleases, so the fixtures split frames mid-`data:`
prefix, mid-UTF-8 codepoint (a multi-byte character sliced in half across
chunks), mix CRLF and LF line endings, and spread one event across multiple
`data:` lines. Streaming parsers that survive that fixture set survive
production.

### Errors that know whether to retry

The error taxonomy is small, but each type answers one question — *should the
caller try again?*

```
LlmError (base, carries provider + requestId)
├── TransportError       # DNS, TLS, socket reset            → retryable
├── HttpError            # non-2xx; 429/5xx retryable, other 4xx not
│     └── RateLimitError # 429, carries retryAfterMs
├── ParseError           # malformed SSE/JSON                → not retryable
├── ProviderError        # in-stream error events            → per provider code
└── TimeoutError         # phase: 'first-byte' retryable, 'idle' not
```

A single `isRetryable` predicate over this taxonomy drives `retryWithBackoff`
(jittered exponential, honoring `retryAfterMs` when the provider sent a
`Retry-After`). Note the two timeout phases — that is ADR-0003, and it matters:
a connection that opens but never delivers a first byte means the provider is
overloaded, and retrying will likely land on a healthier node. A stream that
stalls *mid-generation* means this specific generation wedged, and a retry
costs you everything already generated. Same operator, `streamTimeout({
firstByte, idle })`, two failure modes, two retryability verdicts.

And one line of the taxonomy is a design position, recorded as ADR-0005:
**`AbortError` is not on the list, because cancellation is not an error.**
Unsubscription is silent teardown. Nothing may surface on the error channel
because a consumer walked away — no `catchError` should ever have to filter out
"the user closed the tab." This single rule radiates through every module that
follows, and the agents of part II will pay it the most attention.

Module 1 closes with a mock provider server speaking all three wire dialects,
so every integration test in the repository runs against real HTTP with zero
API keys. Tag: `v0.1.0`.

---

## Interlude — the dual channel, or: why the history is out of order

The next four commits in the log belong to Module 3, which had not started.
Here is what happened.

Every module from 3 onward has the same shape of consumer problem: a chain (or
an agent) produces *one* final value, but a UI wants to render the token stream
*while it runs*. Two audiences, two channels:

```ts
const { result$, progress$ } = pipeline.run(input);
```

`result$` delivers the final value (or the error). `progress$` carries tagged
stream and lifecycle events for rendering. It looks obvious. It is not — this
became decision D3.3, the most-reviewed design in the repository, and the
review found enough sharp edges that the implementation was pulled forward,
before Module 2, so nothing downstream would build on an unaudited contract.

The contract that survived review, in four rules:

1. **Passivity.** Subscribing to `progress$` alone triggers *nothing*. It is a
   window onto whatever execution `result$` drives — a UI channel, never an
   engine.
2. **Terminal events are data.** When the run ends, `progress$` receives one
   final `run_complete` or `run_failed` event *as a `next` notification*, then
   completes. The error *object* travels on `result$` only. A progress renderer
   should never need a `catchError` to draw a red banner.
3. **Unobserved events are dropped.** `progress$` does not replay. If nobody is
   watching, deltas fall on the floor — that is what "UI channel" means.
4. **One `run()` = one execution.** However many subscribers arrive, in
   whatever order, at whatever time — exactly one HTTP request is made, and the
   outcome is *latched permanently*. Late subscribers to `result$` get the
   settled value, by identity.

Rule 4 sounds like `share()`. The commit log shows the exact path by which it
is not. The first attempt used `share({ resetOnRefCountZero: false })` plus a
replay for late subscribers. Then the audit asked one question: *what does
`retry()` do to this?* — and the answer was a hole. `share()`'s `resetOnError`
flag defaults to `true`, so an error resets the shared connection, and a
consumer innocently writing `result$.pipe(retry(2))` would *re-subscribe
through the back door and fire a second HTTP request* — silently violating
one-run-per-call. Latch every reset flag, then, and a second audit fixture: a
mock server whose response keeps dribbling in (the straggler) while a second
subscriber arrives late, pinning that no combination of arrival timing produces
a second request.

The share-flag matrix has one more problem: it cannot express *abort on
abandonment* — if every subscriber leaves mid-run, the HTTP request must be
aborted (that money is real) — *and* latch-every-outcome at the same time. So
the final implementation hand-rolls the latch. It is small enough to read in
full, and it is the most load-bearing fifty lines in the repository:

```ts
// src/chain/dual-channel.ts
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
            settled = true;          // cancelled: latch so nothing ever re-executes
            execution?.unsubscribe(); // aborts in-flight work
            progress.complete();      // silent — no terminal event (ADR-0005)
            output.complete();        // late subscribers: immediate empty completion
          }
        });
      }
    };
  });

  return { result$, progress$: progress.asObservable() };
}
```

Read the teardown closely, because the `queueMicrotask` is not decoration — it
is the third latch race, found weeks later while building Module 3's tracing.
`firstValueFrom`, the single most common way anyone consumes `result$`,
unsubscribes *synchronously inside the delivery of the final value* — after the
source's `next`, before its `complete`. At that instant, `subscriberCount` is
zero and `settled` is still false. A naive teardown reads "everyone abandoned
an unfinished run," aborts the execution, and completes `progress$` *without a
terminal event* — misreporting a perfectly successful run as cancelled. The fix
is to defer the cancellation *decision* (not the bookkeeping) by one microtask
and re-check: if the run settled in the meantime, it was a completion, not an
abandonment. `subscribeOn(asapScheduler)` on the work handles the mirror-image
problem at the start of life: even a fully synchronous execution cannot emit
in the first subscriber's call frame.

Three races — the `retry()` back door, the straggler, the `firstValueFrom`
teardown — each found by an audit question or a failing trace, each pinned by a
regression test. This is why the interlude happened before Module 2: contracts
this subtle must be audited before anything builds on them. And the reward
comes in the next chapter's final module, where agents get this entire audited
contract — every rule, every race fix — by calling one function.

---

## Two artifacts

That is the foundation, and it is worth pausing to see how little of it is
about language models.

Part I produced exactly two artifacts. The first is a **model interface with
laws**: one `ChatModel` for every provider, a normalized event union that no
downstream line ever looks past, an error taxonomy whose types answer "should I
retry?", and a test battery — cold, lazy, unicast, teardown-complete, no
Zalgo — that every Observable-returning contract in the project will inherit.
The second is a **two-channel execution contract**: `{ result$, progress$ }`,
passive progress, terminal events as data, outcomes latched exactly once, and
cancellation that is always silence, never an error.

Neither artifact knows what a prompt is. Neither has heard of retrieval,
memory, or agents. That is the point: the next chapter builds all five of
those, and every one of them will turn out to be a small arrangement of
ordinary RxJS operators sitting on top of these two contracts — a `concatMap`
here, a `scan` there, and, for the agent loop itself, one well-chosen
`expand()`.
