<!-- Draft 1 — generated from the v0.6.0 history. The commit log is the outline:
     git log --reverse --oneline -->

# Everything Composes

*LangChain in 3,100 lines — part II*

---

## Where we left off

Part I ended with two artifacts and a promise. The artifacts: a `ChatModel`
interface with laws — cold, lazy, unicast, teardown-complete, cancellation as
silent teardown — behind a normalized `StreamEvent` union that hides every
provider wire format; and the dual-channel contract, `{ result$, progress$ }`,
with its hand-rolled latch and its three pinned races. The promise: that
prompts, chains, retrieval, memory, and agents would each turn out to be a
small arrangement of ordinary RxJS operators on top of those two contracts.

This chapter keeps the promise. The remaining history:

```
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

---

## Module 2 — Prompts the compiler checks

After part I's interlude, a palate cleanser: Module 2 contains no Observables
at all. A prompt template is a pure function from variables to a string, and
the only interesting question is: *can the compiler catch a missing variable?*

In TypeScript, yes — template-literal types can parse the placeholder grammar
at the type level:

```ts
// src/prompt/template.ts
export type ExtractVars<T extends string> = T extends `${infer _Pre}{${infer Tail}`
  ? Tail extends `{${infer Rest}`
    ? ExtractVars<Rest> // '{{' — escaped literal brace, not a variable
    : Tail extends `${infer Name}}${infer Rest}`
      ? Name | ExtractVars<Rest>
      : never // unclosed '{' — literal, no variable
  : never;
```

`ExtractVars<'Summarize {doc} in {n} bullets'>` evaluates to the union
`'doc' | 'n'`, so:

```ts
const ask = promptTemplate('Summarize {doc} in {n} bullets');
ask({ doc: text, n: 3 });        // ✓
ask({ doc: text });               // compile error: 'n' is missing
ask({ doc: text, n: 3, x: 1 });   // compile error: 'x' does not exist
```

The type has teeth because of two details. The branch order checks the `{{`
escape *before* the variable branch — swap them and `{{foo}}` extracts a
phantom `foo` key that the runtime will never fill. And the runtime scanner in
`renderTemplate` implements *exactly* the same grammar, with a property test
pinning the symmetry: for arbitrary templates, the names the type would extract
and the names the runtime extracts are identical. When your type system and
your runtime both parse the same little language, they must be tested as
mirror images, or they will drift.

There are two exports — `promptTemplate('...')` for the parsed form and
prompt\`Summarize ${'doc'}\` as a tagged literal where interpolations *are* the
placeholder names — deliberately not one overloaded function. That is ADR-0008,
and the reasoning is a scar from Module 1: an overloaded signature on the
`streamTimeout` operator once sent TypeScript's overload resolution somewhere
dark, and the lesson stuck. Two behaviors, two names.

Beyond templates, the module adds message builders (`system`, `user`,
`assistant`) and `messagePrompt`, whose one structural feature is a declared
**history slot** — a marked position in the message list where conversation
memory can splice itself in. Module 5 does not exist yet, but its socket does.
Prompts stay pure the whole way (ADR-0009): no I/O, no Observables. Streams
enter only when a chain hands a rendered prompt to a `ChatModel`. Tag:
`v0.2.0`.

---

## Module 3 — Chains are pipes

Here is the whole bet in one sentence of code. LangChain built `Runnable` — a
bespoke composition abstraction with `invoke`/`batch`/`stream` variants and a
callback system threaded through it. RxJS already *has* a composition
abstraction, and it is the most battle-tested one in the JavaScript ecosystem:

```ts
// a stage IS an operator; a chain IS a pipe
function stageFn<Ctx extends object, P extends object>(
  name: string,
  fn: StageFn<Ctx, P>,
  options?: StageOptions<Ctx, P>,
): OperatorFunction<Ctx, Ctx & P> {
  const policy = options?.onError ?? 'fail';
  return concatMap((ctx: Ctx) =>
    applyPolicy(name, mergePatches(runBody(name, fn, ctx), ctx), ctx, policy),
  );
}
```

A stage body receives the accumulated context and returns a *patch* — just the
keys it adds. The operator merges the patch in, so the pipeline's type
*accumulates*: `OperatorFunction<Ctx, Ctx & P>`. Sequential composition is
`pipe(stage(...), stage(...))`, and a downstream stage can only reference keys
an upstream stage provably produced — a chain that reads `ctx.retrieved` before
the retrieval stage ran is a compile error, not a runtime surprise (ADR-0010,
ADR-0011).

Two combinators cover branching and fan-out without inventing anything.
`stage.when(name, predicate, fn)` is an `if` inside `concatMap` — and when the
predicate is false, the output type is honestly `Ctx & Partial<P>`; the
compiler *makes* downstream code handle the keys' absence, because nothing
ran. `stages.parallel({ a, b })` is a `forkJoin` — every branch runs against
the same pre-join context, no branch can see a sibling's patch (enforced at the
type level with a `UnionToIntersection` over the branch patches), and the
joined patch is the intersection of all of them.

The `run()` method wraps the pipe in part I's dual channel, and each stage's
`emit` feeds tagged model deltas into `progress$` — the plumbing rides *inside
the context object itself*, under hidden `symbol` keys that survive every merge
and are stripped before delivery. `traced()` adds a tap-based lifecycle seam
(`stage_start`, `stage_complete`, `stage_error`, one correlation id per run)
with no framework: a `TraceSink` is an interface with one method, and the
collector used in tests is fifteen lines.

Two lessons from this module's tests are worth stealing. First, the third latch
race described in part I was *found here* — the trace output showed a
successful run ending without its terminal event, and pulling that thread led
to the `firstValueFrom` teardown. Traces built for users debug the framework
first. Second, a subtler ordering bug: a stage that emits its value before its
`complete` notification lets the *next* stage start while the first is still
finishing, and the trace interleaves as `first:start → second:start →
second:complete → first:complete`. The fix — buffer each stage's output with
`toArray()` and merge patches only after the body completes — is the kind of
lifecycle honesty that only shows up when you assert on event *order*, not just
event presence.

Error handling is a per-stage policy: `'fail'` (default) propagates, `'skip'`
drops the patch and flows on (type: `Partial<P>` again), or a fallback function
supplies a substitute patch. And the failing stage's name reaches the consumer
by *tagging the error object with a non-enumerable symbol* rather than wrapping
it in a `StageError` class — because the dual channel's latch re-delivers
errors **by identity**, and consumers match with `instanceof` and
`isRetryable`. Wrapping would have broken all three. Constraints compose:
decisions made in Module 1's taxonomy and the interlude's latch reach forward
and dictate the shape of Module 3's error reporting. Tag: `v0.3.0`.

---

## Module 4 — Retrieval is a pipeline

RAG frameworks tend to arrive as monoliths: a "document pipeline" object with
loaders, splitters, embedders, and stores fused together. Module 4 is the same
functionality as four small parts, each independently testable, composed with
ordinary operators — because ingestion *is* just a pipeline:

```
loader (Observable<Doc>) → split (operator) → embed (batched) → store.upsert
```

**The splitter** is a pure function from a document to chunks, and its
signature carries the design: every chunk records its `[start, end)` offsets
into the source text. That makes the splitter *lossless* — a property test
(fast-check, arbitrary documents) reconstructs the original text from the
chunks' offsets and asserts identity. Chunking bugs are silent data corruption
in most RAG stacks; here they are a failed property. Token counting hides
behind a one-method `Tokenizer` interface with a heuristic default — tiktoken
was *pre-approved* as a dependency and deliberately not taken (ADR-0014). The
interface is the seam; a host that needs exact counts plugs one in.

**The `Embedder`** is Module 1's pattern replayed: a one-method interface
returning a cold Observable, obeying the same laws, tested by the same
battery. A batching operator groups texts into provider-sized requests.
The point is not novelty — it is that the *second* instance of the pattern
costs almost nothing, because the laws and their tests already exist.

**The `VectorStore`** is a small interface — upsert, query by vector with a
`k` and a metadata filter, delete — and the design decision that pays longest
is that its semantics live in a *contract test suite*: one parameterized set of
tests (ranking against hand-computed cosine fixtures, id-collision replacement,
filter-before-k, laziness laws, and a property test that any stored vector's
nearest neighbor is itself) that every implementation must pass. The in-memory
store — brute-force cosine, fine to ~50k vectors — passes it. Three phases
later, a PGlite + Drizzle store (real Postgres with pgvector, embedded, no
server) passes *the same suite unchanged*, and that is the whole proof of
interchangeability. The generalization surfaced exactly one real friction:
pgvector columns have fixed dimensionality, so the suite's store factory takes
`dimensions` up front — a contract adjustment discovered by making a second
implementation pass, which is precisely what contract suites are for. PGlite
ships as an opt-in subpath (`rxjs-llm/pglite`), never a core dependency
(ADR-0018).

At the top sits the module's public face, and it is one operator (ADR-0019):

```ts
query$.pipe(retrieveContext(store, embedder, k))
// → { query, hits, context }   with source-attributed chunks
```

Embed the query, search the store, assemble an attributed context block. In a
chain it is a stage body; in Module 6 it will become something better. Tag:
`v0.4.0`.

---

## Module 5 — Memory is a scan

Module 5 is the smallest module, and it opens with the observation that makes
it small: conversation memory is a *fold*. A conversation is a stream of turns;
the accumulated history is `scan` over that stream; and everything frameworks
call a "memory type" — buffer memory, window memory, token-budget memory,
summary memory — is not a different memory at all. It is a different
**projection of the same state**. So: one reducer, swappable views.

```ts
// src/memory/core.ts (the heart of it)
export type MemoryView = (turns$: Observable<readonly Turn[]>) => Observable<ChatMessage[]>;

const state = new BehaviorSubject<readonly Turn[]>(initial);

lifetime.add(
  input
    .pipe(scan((turns, turn) => [...turns, turn], initial))
    .subscribe((turns) => state.next(turns)),
);

const messages$ = viewFn(state.asObservable()).pipe(
  shareReplay({ bufferSize: 1, refCount: false }),
);
```

`fullView()` and `windowView(n)` are a `map`. `tokenBudgetView(budget)` is a
`map` that walks turns newest-first until the budget is spent — token budgeting
is a *view* concern (ADR-0021), because the turns are the truth and the budget
only shapes the projection. The view's output plugs straight into Module 2's
history slot. Snapshot and restore are trivial *because* of the same split:
serialize the turns, only the turns — views are recomputed, not persisted — and
a fast-check property pins that restore-then-view equals never-having-left.
There is deliberately no persistence layer (ADR-0023); hosts get a plain JSON
value and persist it however they like.

The one hard problem is the summary view: fold older turns into a running
summary *using a ChatModel*, without ever blocking the conversation on the
summarizer. That makes it an **async fold, eventually consistent** (ADR-0022):
the view serves what it has immediately — summary-so-far plus recent verbatim
turns — while `exhaustMap` runs at most one summarization at a time in the
background. `exhaustMap` and not `switchMap`, deliberately: turns arriving
mid-summarization must not *cancel* the fold (that would waste the tokens
already spent), they must queue behind it.

And that choice bought this module's war story. `exhaustMap` *ignores*
emissions that arrive while its inner Observable is active — so when the fold
completed and synchronously pushed the updated state, the push arrived while
the inner stream was still tearing down, and `exhaustMap` dropped it: the
re-trigger for the *next* summarization vanished, and the fold silently stopped
folding. The fix is one operator — `observeOn(asapScheduler)` on the trigger
path, deferring the re-trigger until the inner stream has fully settled. If
part I's lesson was "audit your latches," this module's is its scheduling
twin: **synchronous re-entry during teardown is where flat-mapping operators
keep their sharp edges**. A `pending$` signal rounds out the API so hosts can
render "summarizing…", and dispose ends everything cleanly — mid-flight folds
included. Tag: `v0.5.0`.

---

## Module 6 — An agent is a recursion

Strip the mythology from "agent" and what remains is a loop: call the model;
if it requested tools, run them, append the results, and go again; if it
answered, stop. A loop whose next input depends on the previous output is a
*recursion*, and RxJS has an operator whose entire purpose is recursive
expansion:

```ts
// src/agent/loop.ts (the shape of it)
const step = (state: LoopState): Observable<LoopState> => {
  if (state.outcome !== undefined) return EMPTY;        // terminal — stop expanding
  if (state.iteration >= maxIterations) {
    return of({ ...state, outcome: { type: 'max_iterations', ... } });
  }
  return model.stream(state.messages, callOptions).pipe(
    tap((event) => emit({ type: 'model_event', iteration, event })),
    collectCompletion(),
    concatMap((completion) =>
      completion.toolCalls.length === 0
        ? of({ ...advance(state, completion), outcome: { type: 'complete', ... } })
        : runTools(completion.toolCalls, iteration).pipe(
            map((toolMessages) => advance(state, completion, toolMessages)),
          ),
    ),
  );
};

return of(initial).pipe(
  expand(step, 1),
  last(),
  map((state) => state.outcome!),
);
```

`expand(step, 1)` *is* the agent loop — sequential recursion, no scaffolding,
no ReAct prompt engineering (a non-goal: modern providers reason natively over
tool-use events). Each expansion streams one model turn — with every delta
`tap`ped out to `progress$` *before* `collectCompletion()` folds the stream, so
the UI watches the agent think in real time — then either terminates or runs
the tools and recurses. Notice what `runAgent` returns: the `dualChannel` from
part I's interlude, called as a function. One `runAgent()` = one execution,
outcome latched, terminal events as data, all three race fixes included — for
free. The extraction (ADR-0026: *reuse by extraction, not imitation*) happened
in this module's Phase 2: the audited machinery was lifted out of `chain.ts`
verbatim, the chain suite proved the refactor changed nothing, and agents
plugged in.

**Tools** are Zod-defined, and Zod is the one runtime dependency the whole
project added — ADR-0024, and even that was cheaper than planned: Zod v4's
native `z.toJSONSchema` meant the planned `zod-to-json-schema` bridge was never
needed. A tool couples its schema to its executor:

```ts
const searchDocs = tool({
  name: 'search_docs',
  description: 'Search the ingested knowledge base',
  input: z.object({ query: z.string() }),
  execute: ({ query }, { signal }) => /* Observable | Promise */,
  timeoutMs: 5_000,
});
```

The same schema is compiled to JSON Schema for the provider and used to
*validate the model's arguments at runtime* — which sets up this module's
central safety decision (ADR-0027): **tool execution never errors.**
`executeToolCall` always produces a result. Malformed JSON from the model,
schema-invalid arguments, an executor that throws, a timeout — every one
becomes `{ content: 'Error: ...', isError: true }`, appended to the transcript
as an ordinary tool message. Why: the model *reads the transcript*. Feed the
validation failure back as text and the model corrects itself on the next
iteration — self-correction, tested with a scripted scenario where the model
sends broken arguments on turn one and fixed ones on turn two. Throw instead,
and one hallucinated argument kills a nine-step run. The error channel is
reserved for infrastructure failures — transport, provider — where retrying
the *loop* makes sense; a bad tool call is conversation, not infrastructure.

The same values-not-exceptions stance shapes the iteration budget (ADR-0025).
Hitting `maxIterations` is *an answer*, not a failure:

```ts
export type AgentOutcome =
  | { type: 'complete';       text: string; completion: ChatCompletion; ... }
  | { type: 'max_iterations'; messages: ChatMessage[]; iterations: number; ... };
```

The transcript is intact and paid for; the caller decides whether to surface a
partial result, resume with a bigger budget, or apologize. An exception would
have destroyed exactly the information needed to make that choice.

The rest of the rails: per-tool timeouts reuse Module 1's `streamTimeout` —
resilience operators written for HTTP streams apply unchanged to tool
executions, because everything is an Observable. Parallel tool calls are real
(`mergeMap` under a concurrency cap) but results append in *call order*
regardless of completion order, so transcripts are deterministic. And the
module's crown-jewel test is the **cancellation matrix**: unsubscribe during
model streaming, during tool execution, and between iterations — in all three,
every `AbortSignal` fires, both channels complete silently (ADR-0005, one last
time), and *no unhandled rejections escape*. That last clause found the
module's sneakiest bug: when a tool's Promise is abandoned mid-flight and
later rejects, nobody is subscribed — and RxJS reports the rejection to
`reportUnhandledError`, which becomes an uncaught exception on the host. The
fix is a sentinel catch that swallows rejections *after* abort (and only
then), pinned by a test that would otherwise crash the process. Property
tests seal the loop's algebra: message lists grow strictly per iteration, and
every `tool_use` id is answered by exactly one `tool_result`.

---

## The capstone — six modules, one pipeline

The repository's definition of done was written down before most of the code
existed: one test that composes *all six modules* into a single pipeline,
running over real HTTP against the mock server. Here it is, abridged — every
annotation names the module doing the work:

```ts
// ---- Module 4: ingest the corpus, build the retrieval plumbing
const store = memoryStore();
await firstValueFrom(
  ingest(textFileLoader(corpusDir), { split: { maxTokens: 60, overlap: 8 }, embedder, store })
    .pipe(last()),
);

// ---- Module 5: conversation memory
const memory = createMemory({ view: windowView(10) });

// ---- Module 1: the real adapter, wired to the mock server
const model = anthropic({ apiKey: 'test', model: 'mock-model', baseUrl: `${server.url}/anthropic` });

// ---- Module 6: a tool whose body IS Module 4's retriever
const searchDocs = tool({
  name: 'search_docs',
  description: 'Search the ingested knowledge base',
  input: z.object({ query: z.string() }),
  execute: ({ query }) =>
    of(query).pipe(retrieveContext(store, embedder, 3), map((r) => r.context)),
  timeoutMs: 5_000,
});

// ---- Module 2: the question prompt
const ask = promptTemplate('Answer using the knowledge base: {question}');

// ---- Module 3: the chain — an agent is just a stage
const pipeline = chain<{ question: string }>({ trace: sink }).pipe(
  stage('agent', (ctx, emit) => /* runAgent(...); forward its deltas via emit */),
  stage('remember', (ctx) => {
    memory.record({ user: ctx.question, assistant: ctx.answer });
    return of({ remembered: true });
  }),
);

const { result$, progress$ } = pipeline.run({ question });
```

The scripted scenario has the model request `search_docs` on its first turn and
answer on its second, so the assertions can be surgical. Exactly two model
requests cross the wire. The second request's body *contains Module 4's
retrieval output* as the tool result — source-attributed, `[source:
espresso.md]`, fetched from a store that a real loader-splitter-embedder
pipeline populated. The chain's `progress$` carries the agent's token deltas,
and joining the `text_delta` events reproduces the final answer character for
character — six modules deep, the streaming path never broke. Memory holds the
exchange. The trace shows both stages, in order, under one correlation id. And
the last event on `progress$` is `{ type: 'run_complete' }` — the dual
channel's terminal event, closing the loop on a contract designed in an
interlude thirty commits earlier.

Look once more at the middle of that pipeline. `retrieveContext` — a chain
operator — is the *body of a tool*, handed to an agent, which is itself a
*stage in a chain*, whose deltas flow out of the same `progress$` as everything
else. No adapters, no glue layers, no impedance mismatch. Everything composes
because everything is the same kind of thing. Tag: `v0.6.0`.

---

## What it cost, what it bought

The final accounting: roughly **3,100 non-blank lines** of strict TypeScript
across six modules. **Two runtime dependencies** — `rxjs` and `zod` — with
PGlite/Drizzle opt-in behind a subpath. **280 tests** in 37 files: law tests,
marble tests with injected schedulers, fast-check properties, adversarial
byte-level fixtures, and integration tests over real HTTP with no API keys.
**27 ADRs**, one per design decision, written when the decision was made.
**34 commits**, each one a phase, readable end to end.

Part of the smallness is subtraction, and `NON_GOALS.md` deserves explicit
credit: no `Runnable` reinvention, no ReAct scaffolding, no document-
understanding integrations, no external vector databases, no persistence
layer, no new dependencies without an ADR. Every "no" in that file is a
thousand lines that were never written. A reference implementation stays
reference-sized only if refusing features is a governed act, not a vibe.

But the deeper claim is the bet from the opening of part I. These were never
really chapters about LLMs. Take stock of what did the actual work: `concatMap`
made chains sequential; `forkJoin` made stages parallel; `scan` was memory;
`exhaustMap` made summarization non-blocking; `expand` *was* the agent;
`tap` was tracing; a `Subject` and a `ReplaySubject` and fifty careful lines
were the dual channel. The LLM-specific code — wire formats, taxonomies,
prompt grammar, cosine similarity — is a thin, boring layer at the edges.
Orchestration, the thing the frameworks sell, was already sitting in a library
that predates the transformer paper.

And the patterns that made it trustworthy travel even further than the
operators: normalize at one boundary and let everything downstream consume the
union; write laws as tests and make every implementation of a contract pass
the same suite; treat cancellation as teardown, never as an error; return
outcomes as values and reserve the error channel for infrastructure; put a
latch under anything that promises "exactly once," and then audit the latch,
because `share()` will not save you and `firstValueFrom` is coming for your
teardown logic.

The bet paid. Streams all the way down.
