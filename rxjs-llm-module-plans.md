Plan: rxjs-llm — Module 1, Uniform Model Interface

Design decisions to review first
D1 — Relationship to llm-stream-adapter. You already own the SSE frame-boundary parsing, error taxonomy, and cancellation work. Options: (a) rxjs-llm depends on it as a package, (b) we fold that code into this repo as the transport layer, (c) clean re-implementation here as the canonical home. My recommendation: (c) or (b) — this repo becomes the reference implementation and the book artifact; a fresh, audited implementation is worth more than a dependency edge. But you know the current state of that package best.
D2 — Chunk taxonomy. I propose a normalized discriminated union all adapters must emit:
tstype StreamEvent =
  | { type: 'message_start'; model: string }
  | { type: 'text_delta'; text: string }
  | { type: 'tool_call_delta'; id: string; name?: string; argsDelta: string }
  | { type: 'usage'; input: number; output: number }
  | { type: 'message_stop'; stopReason: StopReason };
Tool-call events included from day one even though agents are Module 6 — retrofitting them into the taxonomy later is painful because Anthropic and OpenAI encode tool deltas completely differently.
D3 — Two timeouts, not one. First-byte timeout (connection established but no data — provider overloaded) and inter-chunk idle timeout (stream stalls mid-generation) are distinct failure modes with distinct retryability. timeout({ first, each }) maps to this directly.
D4 — Runtime/tooling. Bun + Vitest + rxjs@7.8 + strict TypeScript, matching your rxjs-remix setup. ESM only.
Package structure
rxjs-llm/
├── src/
│   ├── types.ts            # ChatMessage, ChatOptions, StreamEvent, ChatModel
│   ├── errors.ts           # error taxonomy + isRetryable predicate
│   ├── transport/
│   │   ├── fetch-stream.ts # fetch → Observable<Uint8Array>, AbortController ↔ teardown
│   │   └── sse.ts          # Observable<Uint8Array> → Observable<SseEvent>
│   ├── adapters/
│   │   ├── anthropic.ts
│   │   ├── openai.ts
│   │   └── ollama.ts
│   ├── operators/
│   │   ├── retry-backoff.ts   # jittered exponential, respects Retry-After
│   │   ├── stream-timeout.ts  # first-byte + idle
│   │   └── rate-limit.ts      # token bucket
│   └── index.ts
├── test/                   # incl. adversarial SSE chunk-split fixtures
├── STATUS.md, DEFINITION.md, NON_GOALS.md, decisions/
└── package.json, tsconfig.json
The core interface
tsinterface ChatModel {
  stream(messages: ChatMessage[], options?: ChatOptions): Observable<StreamEvent>;
  complete(messages: ChatMessage[], options?: ChatOptions): Observable<ChatCompletion>; // stream + reduce
}
Contract (these become law tests): the Observable is cold (each subscribe = one HTTP request), unicast, lazy (no fetch before subscribe — no Zalgo, no eager side effects), and teardown-complete (unsubscribe aborts the underlying fetch, verified by asserting the AbortSignal fired).
Error taxonomy
LlmError (base, carries provider + requestId)
├── TransportError      # DNS, TLS, socket reset          → retryable
├── HttpError           # non-2xx; 429/5xx retryable, 4xx not
│     └── RateLimitError # 429, carries retryAfterMs
├── ParseError          # malformed SSE/JSON              → not retryable
├── ProviderError       # in-stream error events           → per provider code
└── (AbortError is NOT an error — unsubscription is silent teardown)
That last line is the design point worth an ADR: cancellation must never surface on the error channel.
Implementation phases

Scaffold + types + errors — compiles, taxonomy tests pass.
Transport — fetchStream with teardown test; SSE parser tested against fixtures where frames split mid-data:, mid-UTF-8-codepoint, CRLF vs LF, multi-line data: fields.
Anthropic adapter — full event mapping including content_block_delta/tool-use blocks, in-stream error events → ProviderError.
OpenAI + Ollama adapters — OpenAI's [DONE] sentinel and delta-fragment tool calls; Ollama's NDJSON (not SSE — the transport layer needs a second framing strategy, which validates the layering).
Resilience operators — retryWithBackoff (marble-testable with injected scheduler), streamTimeout, rateLimit.
Governance docs + README with a runnable example per provider.

Each phase = one commit, so the history reads as the tutorial.
Test strategy highlights
Adversarial chunk splits as the SSE parser's main fixture set; marble tests for the operators via injected TestScheduler; a mock provider server (plain Bun.serve) for integration tests so CI needs no API keys; explicit Zalgo test (subscribe synchronously, assert nothing emitted in the same microtask).

Review points for you: D1 (reuse vs. re-implement llm-stream-adapter), D2 (taxonomy shape — anything missing for your rxjs-full integration, e.g. thinking_delta?), and whether Ollama makes the cut for phase 4 or gets deferred. Give me the word — and whether you want the zip route or you'll take this plan into Claude Code — and I'll start with Phase 1.

# rxjs-llm — Module Plans 2–6

Companion to the Module 1 plan (Uniform Model Interface). Each module below is written
as a self-contained brief you can paste into Claude Code. All modules live in the same
repo: `github.com/hansschenker/rxjs-llm`, local path `C:\Users\hanss\Web\Hans\rxjs-llm`.

**Shared conventions (state these once per Claude Code session):**

- Runtime/tooling: Bun, Vitest, `rxjs@7.8`, strict TypeScript, ESM only.
- Every Observable-returning API is cold, lazy, unicast, teardown-complete. No Zalgo:
  nothing emits synchronously in the subscribe call frame.
- Cancellation is silent teardown — it never surfaces on the error channel.
- Each phase = one commit; commit history should read as a tutorial.
- Governance: keep `STATUS.md` current, file an ADR in `decisions/` for every design
  decision marked **D-n** below, respect `NON_GOALS.md`.
- Marble tests via injected `TestScheduler` for every custom operator; law/property
  tests for anything claiming algebraic structure.
- No new runtime dependencies without an ADR justifying them.

---

## Module 2 — Prompts

### Goal
Typed prompt construction with zero framework machinery: tagged template literals,
compile-time-checked placeholders, few-shot formatting, and message-list builders that
produce `ChatMessage[]` for Module 1.

### Design decisions to review

**D2.1 — Two prompt forms, not one.** A *string prompt* (single user turn) and a
*message prompt* (full `ChatMessage[]` including system, few-shot pairs, history slot).
LangChain conflates these; we keep them distinct because they compose differently.

**D2.2 — Compile-time placeholder safety.** Use TypeScript template-literal types to
extract placeholder names from the template string and require an exactly-matching
variables object. `prompt('Summarize {doc} in {n} bullets')` yields a function whose
parameter type is `{ doc: string; n: string | number }`. Missing or extra keys are
compile errors. This is the module's headline feature — it must have type-level tests.

**D2.3 — Prompts are pure.** No I/O, no Observables in this module. A prompt template
is a pure function `(vars) => string | ChatMessage[]`. Streams enter only when a chain
(Module 3) feeds the result to a `ChatModel`. Keeping this module synchronous and pure
is what makes it trivially testable and lawful.

### Package structure (additions)

```
src/prompt/
├── template.ts       # prompt() tagged/parsed template, placeholder type extraction
├── messages.ts       # system(), user(), assistant(), fewShot() builders
├── format.ts         # output-format helpers: asJson(schema), asBullets(n), noJargon()
└── index.ts
test/prompt/
├── template.test.ts        # runtime behavior
├── template.types.test.ts  # type-level tests via expectTypeOf (Vitest)
└── messages.test.ts
```

### Core interface

```ts
// String form — placeholders extracted at the type level
const summarize = prompt('Summarize {doc} in {n} bullets, plain language.');
summarize({ doc: text, n: 5 }); // => string; { doc } alone is a compile error

// Message form
const qa = messagePrompt({
  system: 'Answer only from the provided context. Say "unknown" otherwise.',
  fewShot: [
    { user: 'Q: capital of France? Context: ...', assistant: 'Paris' },
  ],
  user: 'Q: {question}\nContext: {context}',
});
qa({ question, context }); // => ChatMessage[]
```

### Phases
1. **Placeholder type machinery** — `ExtractVars<T>` type, runtime interpolation,
   escaping rules (`{{` literal brace). Type-level tests first.
2. **Message builders** — `system/user/assistant/fewShot`, `messagePrompt` composition,
   history slot (a marker position where Module 5 memory splices in).
3. **Format helpers** — small combinators appending output-format instructions;
   `asJson(zodSchema)` renders the JSON Schema into the prompt and returns the schema
   for later parsing (used by Module 6).
4. **Docs + README section** with the compile-error screenshots/examples.

### Test strategy
- `expectTypeOf` tests asserting exact inferred parameter types, including the
  compile-failure cases via `@ts-expect-error`.
- Property test: for random variable maps, interpolation then extraction round-trips.
- Escaping edge cases: adjacent placeholders, braces in values, unicode.

---

## Module 3 — Chains (composition)

### Goal
Sequential/parallel/conditional LLM workflow composition using native RxJS operators —
no `Runnable` reinvention. The one genuinely new piece: a typed context object that
accumulates intermediate results as it flows through stages, plus a tracing seam.

### Design decisions to review

**D3.1 — Context threading via accumulating record.** A chain's unit of flow is
`Ctx extends Record<string, unknown>`. Each stage reads what it needs and returns a
*patch*; the framework merges the patch into the context. Type-level: each stage
widens the context type, so downstream stages can only reference keys that upstream
stages provably produced. This is the typed answer to "raw Observables only carry the
latest value."

**D3.2 — A stage is an operator.** `stage(name, fn)` where
`fn: (ctx: Ctx) => ObservableInput<Patch>` returns an
`OperatorFunction<Ctx, Ctx & Patch>` implemented with `concatMap`. Sequential
composition is then just `pipe(stage(...), stage(...), ...)` — chains ARE pipes.
Parallel fan-out is `stages.parallel({ a: fnA, b: fnB })` implemented with `forkJoin`;
branching is `stage.when(predicate, fn)` — a conditional inside `concatMap`, not a
new abstraction.

**D3.3 — Streaming stages are first-class.** A stage whose body calls
`model.stream()` must be able to (a) surface `text_delta` events to an outer observer
for UI streaming while (b) contributing the reduced final text to the context. Design:
stages receive a second argument `emit: (e: StreamEvent) => void` wired to a
`progress$` channel on the chain. The chain's return type is
`{ result$: Observable<Ctx>, progress$: Observable<TaggedStreamEvent> }`.
This is the design decision most worth an ADR — it is where LangChain's callback
system lives, and where yours will be cleaner.

**ADR checklist for D3.3** (review points, captured 2026-07-03 — the ADR must
answer all four explicitly, with a test per answer):

1. *Passivity.* `progress$` never triggers execution. Subscribing to it alone
   does nothing; only `result$` drives the work. Test: subscribe `progress$`,
   assert zero stage invocations. (The wrong wiring — deriving both channels
   from one shared pipeline where either subscription starts it — is a
   re-execution bug waiting to happen.)
2. *Lifecycle coupling.* `progress$` completes/tears down when `result$`
   completes, errors, or is unsubscribed — all three. The error itself
   travels on `result$` only; `progress$` completes quietly (one failure must
   not fire two error handlers). Unsubscribe produces no error on either
   channel — cancellation stays silent, per the Module 1 contract (ADR-0005).
3. *No-subscriber case.* Progress events with nobody listening are **dropped**
   (plain Subject semantics) — correct for a UI channel, but it is a stated
   decision, not an accident: no buffering, no replay. Subscribe to
   `progress$` before subscribing `result$` to see everything.
4. *Re-run semantics.* If `result$` is cold, a second subscription re-runs the
   chain and `progress$` would interleave events from two executions. The ADR
   must pick: one `run()` call = one logical execution (multicast `result$`
   within the run), or cold-multi with correlation-id-tagged progress events.
   Reconcile explicitly with the Module 1 cold/unicast law — chains are a
   different API surface, and the ADR should say why.

**D3.4 — Tracing as an operator, not a framework.** `traced(name)` — a `tap`-based
operator attaching correlation IDs, timestamps, and stage names to a pluggable sink
(console, OpenTelemetry span, test collector). Every `stage()` applies it internally;
the sink is injected via chain options. This is the LangSmith replacement seam.

### Package structure (additions)

```
src/chain/
├── stage.ts        # stage(), stage.when(), stages.parallel()
├── chain.ts        # chain() factory: options, progress$ wiring, run()
├── trace.ts        # traced(), TraceSink interface, consoleSink, collectorSink
└── index.ts
test/chain/
```

### Core interface

```ts
const summarizeAndAnswer = chain<{ url: string; question: string }>()
  .pipe(
    stage('fetch',     ctx => fetchPage(ctx.url)),                    // + { page }
    stage('summarize', (ctx, emit) =>
      model.stream(summarizePrompt({ doc: ctx.page })).pipe(collectText(emit))), // + { summary }
    stage('answer',    (ctx, emit) =>
      model.stream(qaPrompt({ question: ctx.question, context: ctx.summary }))
        .pipe(collectText(emit))),                                     // + { answer }
  );

const { result$, progress$ } = summarizeAndAnswer.run({ url, question });
```

### Phases
1. **`stage()` + context merge types** — sequential only; type tests proving key
   accumulation and rejection of unknown keys.
2. **`traced()` + TraceSink** — collector sink used in tests to assert stage order,
   timing fields present, correlation ID stable across stages.
3. **`progress$` streaming channel** — `collectText(emit)` helper (reduce deltas to
   final string while forwarding events); backpressure note in ADR.
4. **Parallel + conditional** — `forkJoin`-based `stages.parallel`, `stage.when`.
5. **Error semantics** — per-stage `catchError` policy option (`fail | skip |
   fallback(fn)`); errors carry the stage name; retry composition with Module 1's
   `retryWithBackoff` demonstrated in tests.
6. **End-to-end example** against the mock provider server from Module 1.

### Test strategy
- Marble tests for stage sequencing and cancellation mid-chain (unsubscribe from
  `result$` must abort the in-flight provider request — assert via mock AbortSignal).
- Type-level tests: downstream stage referencing a key produced only in a parallel
  sibling branch before the join must not compile.
- Law-adjacent property: `stage(f)` then `stage(g)` over pure patch functions equals
  `stage(g ∘ f)` up to trace events (associativity of the merge).

---

## Module 4 — Indexes / RAG plumbing

### Goal
Document loaders, a text splitter, an embeddings interface, a vector store interface
with two implementations (in-memory; PGlite + pgvector-style via Drizzle), and a
retriever that composes them. Biggest module by LOC; keep every piece bounded.

### Design decisions to review

**D4.1 — Loaders emit documents as a stream.** `Loader = (source) =>
Observable<Doc>` where `Doc = { id, text, metadata }`. Directory loads, paginated
APIs, and crawls are naturally incremental — a stream, not a promised array. Ship
three loaders only: `textFileLoader` (file/directory), `webLoader` (fetch +
readability extraction), `jsonLoader`. Docling/IDR integration is explicitly a
NON_GOAL for this repo (belongs to `rxjs-rag`); note it in `NON_GOALS.md`.

**D4.2 — Splitter is a pure transformer + an operator.** Core:
`splitText(text, opts): Chunk[]` — pure, recursive splitting on paragraph →
sentence → character boundaries with token budget and overlap. Wrapped as
`splitDocs(opts): OperatorFunction<Doc, Chunk>` via `mergeMap`. Token counting
behind a `Tokenizer` interface; default a cheap `chars/4` estimator, optional
`tiktoken`-backed one behind a dynamic import (ADR: dependency).

**D4.3 — Embeddings mirror the ChatModel pattern.**
`Embedder = { embed(texts: string[]): Observable<Float32Array[]> }` with provider
adapters reusing Module 1's transport, error taxonomy, and retry operators. Batching
operator: `bufferCount(n)` + `concatMap` with the rate-limit operator applied —
a showcase of Module 1 operators composing.

**D4.4 — VectorStore is a small interface, not a database abstraction.**
`upsert(entries)`, `query(vector, k, filter?)`, `delete(ids)` — all
Observable-returning. In-memory implementation: brute-force cosine over
`Float32Array[]` (a dozen lines, fine to ~50k vectors — state that bound in the
README). PGlite implementation via Drizzle mirroring your Fitness Assistant port.
No HNSW, no external services in this repo.

**D4.5 — Retriever is one operator.** `retrieveContext(store, embedder, k)` :
`OperatorFunction<string, RetrievedContext>` — embed the query, top-K, format
chunks with source metadata into a context block sized to a token budget. Optional
rerank hook `(query, chunks) => Observable<Chunk[]>` for later.

### Package structure (additions)

```
src/index/            # 'index' as in RAG indexes
├── loaders/{text-file,web,json}.ts
├── split.ts          # splitText + splitDocs operator + Tokenizer
├── embed/{types,anthropic-voyage,openai,ollama}.ts
├── store/{types,memory,pglite}.ts
├── retrieve.ts       # retrieveContext operator, context formatting
├── ingest.ts         # convenience pipeline: load → split → embed → upsert
└── index.ts
test/index/
```

### Core interface

```ts
// Ingestion — the whole pipeline is one pipe
textFileLoader('./docs').pipe(
  splitDocs({ maxTokens: 400, overlap: 40 }),
  embedBatched(embedder, { batchSize: 64 }),
  upsertInto(store),
).subscribe();

// Retrieval — inside a chain stage
stage('retrieve', ctx => of(ctx.question).pipe(retrieveContext(store, embedder, 6)))
```

### Phases
1. **Splitter** (pure core first) — golden-file tests + property tests: every chunk
   ≤ maxTokens; concatenation minus overlaps reconstructs the source; no split
   inside a UTF-8 codepoint or surrogate pair.
2. **In-memory vector store** — cosine correctness against hand-computed fixtures;
   property: querying with a stored vector returns itself first.
3. **Embedder interface + one adapter (OpenAI shape) + batching operator** — mock
   server; assert batch boundaries and rate-limit interaction with marble tests.
4. **Loaders** — text-file (recursive dir, glob filter), web (fetch + extraction),
   json. Each streams incrementally; cancellation stops mid-directory.
5. **PGlite store** — schema via Drizzle, same test suite run against both store
   implementations (shared contract test file — the store "law tests").
6. **`retrieveContext` + `ingest` pipeline + end-to-end test**: ingest fixture
   corpus → ask question → assert the known-relevant chunk is in the context.

### Test strategy
- Shared contract test suite parameterized over store implementations.
- Property tests (fast-check) for the splitter — this is where hand-rolled RAG
  usually has silent bugs.
- Ingestion cancellation test: unsubscribe mid-pipeline leaves no dangling embedder
  request (AbortSignal assertion, same pattern as Module 1).

---

## Module 5 — Memory

### Goal
Conversation memory as fold, not framework: buffer, windowed, token-budget, and
summary memory — each a small strategy object that a chain splices into the message
list. Target ≈150 lines of source; the value is the design, not the volume.

### Design decisions to review

**D5.1 — Memory is a reducer + a view.**
`Memory = { record(turn: Turn): void; view(): Observable<ChatMessage[]> }` backed by
your `rxjs-reactive-state` patterns: internally a `scan` over a turn stream. All
strategies share the reducer; they differ only in the *view* (how history is
projected into messages). This makes strategies swappable mid-conversation.

**D5.2 — Summary memory is an async fold.** When the un-summarized tail exceeds a
threshold, fold it into a running summary via one LLM call:
`concatMap` on a trigger stream, using Module 1's model and a Module 2 prompt.
Design point for the ADR: summarization is *eventually consistent* — `view()` may
serve the pre-summary window while a fold is in flight; a `pending$` signal exposes
this. Never block the conversation on summarization.

**D5.3 — Token budget is a view concern.** `tokenBudgetView(n, tokenizer)`: walk
history newest-first, evict oldest whole turns until under budget, never split a
turn. Reuses Module 4's `Tokenizer`.

**D5.4 — Persistence is out of scope**, except a `snapshot()/restore()` pair
(serializable state) so hosts can persist however they like. ADR + NON_GOALS entry.

### Package structure (additions)

```
src/memory/
├── core.ts         # Turn, Memory, createMemory (scan-based reducer)
├── views.ts        # fullView, windowView(n), tokenBudgetView(budget, tok)
├── summary.ts      # summaryView(model, prompt, opts) — the async fold
└── index.ts
test/memory/
```

### Core interface

```ts
const memory = createMemory({ view: summaryView(model, summarizePrompt, { foldAfter: 12 }) });

// In a chain: splice memory into the message-prompt history slot (Module 2, D2 history marker)
stage('answer', (ctx, emit) =>
  memory.view().pipe(
    take(1),
    concatMap(history =>
      model.stream(qa({ question: ctx.question }).withHistory(history)).pipe(collectText(emit)),
    ),
    tap(() => memory.record({ user: ctx.question, assistant: ctx.answerSoFar })),
  ))
```

### Phases
1. **Core reducer + full/window views** — marble tests for `view()` reactivity
   (recording a turn emits an updated view to existing subscribers).
2. **Token-budget view** — property test: rendered messages always ≤ budget;
   eviction is oldest-first and turn-atomic.
3. **Summary view** — mock model; tests for the eventual-consistency window,
   fold-failure fallback (on summarizer error, fall back to window view — never
   lose the conversation), no overlapping folds (`exhaustMap` on the trigger).
4. **snapshot/restore + docs.**

### Test strategy
- The summary fold is the concurrency hotspot: marble tests for trigger while a
  fold is in flight (must not double-fold), teardown mid-fold.
- Property: for any turn sequence, `restore(snapshot(m))` yields an equivalent view.

---

## Module 6 — Agents / tool use

### Goal
The tool-call loop as an `expand()` recursion over Module 1's native tool-use
events, a Zod-backed tool registry, and hard safety rails (max iterations, per-tool
timeout, whole-loop cancellation). No ReAct prompt scaffolding — providers reason
natively; that part of LangChain is legacy.

### Design decisions to review

**D6.1 — The loop is `expand()`.** State =
`{ messages, iteration, pendingToolCalls }`. Each expansion: call
`model.complete()` (Module 1) → if the completion contains tool calls, execute
them (`mergeMap` with a concurrency cap — parallel tool calls are real), append
tool results to messages, recurse; else terminate. `takeWhile(iteration <= max)`
with a distinguishable `MaxIterationsExceeded` outcome (a result variant, not an
error — the ADR: exceeding the budget is an *answer*, not a failure).

**D6.2 — Tools are Zod-defined.**
`tool({ name, description, input: zodSchema, execute })` — JSON Schema for the
provider derived via `zod-to-json-schema` (ADR: the one new dependency), runtime
validation of the model's arguments before execute. Invalid arguments produce a
tool-result error message *returned to the model* (it can self-correct), not a
thrown error — this is the key robustness trick.

**D6.3 — Tool execution safety.** Per-tool `timeout` and `retryable` flags mapping
onto Module 1's operators; a tool result is always produced (success, validated
error, or timeout notice) so the loop never stalls waiting on a missing
`tool_result`. Unsubscribing from the agent aborts the in-flight model call AND
all in-flight tool executions (tools receive an `AbortSignal`).

**D6.4 — Agent streaming reuses D3.3.** The agent exposes
`{ result$, progress$ }`; `progress$` interleaves model deltas and tool lifecycle
events (`tool_start`, `tool_result`), tagged by iteration. An agent is just a
chain stage from the outside — `stage('agent', ...)` composes it into Module 3
pipelines with no special casing.

### Package structure (additions)

```
src/agent/
├── tool.ts       # tool(), ToolRegistry, zod→JSON Schema, arg validation
├── loop.ts       # runAgent(): expand()-based loop, iteration state, outcomes
├── events.ts     # AgentEvent taxonomy for progress$
└── index.ts
test/agent/
```

### Core interface

```ts
const weather = tool({
  name: 'get_weather',
  description: 'Current weather for a city',
  input: z.object({ city: z.string() }),
  execute: ({ city }, { signal }) => fetchWeather(city, signal),
  timeoutMs: 5_000,
});

const { result$, progress$ } = runAgent(model, {
  tools: [weather],
  messages: [user('Do I need an umbrella in Rapperswil tomorrow?')],
  maxIterations: 8,
  toolConcurrency: 4,
});
```

### Phases
1. **Tool definition + registry** — zod validation round-trip tests; malformed-args
   → self-correction message shape.
2. **The loop, happy path** — mock provider scripted to emit tool calls then a
   final answer; assert message-list growth and termination.
3. **Safety rails** — max-iterations outcome, per-tool timeout, parallel tool
   calls with concurrency cap (marble test), loop cancellation aborts everything
   (multi-AbortSignal assertion).
4. **`progress$` events + chain integration** — agent-as-stage end-to-end test:
   retrieve (Module 4) → agent with tools (Module 6) → memory record (Module 5),
   run against the mock server. This test is the repo's capstone.
5. **README: the full "LangChain in ~2,500 lines" walkthrough** linking every
   module — this doubles as the book-chapter skeleton.

### Test strategy
- Scripted mock provider (extend Module 1's mock server with a scenario DSL:
  "on turn 1 emit these tool calls; on turn 2 emit final text").
- The cancellation matrix is the crown jewel: unsubscribe during (a) model
  streaming, (b) tool execution, (c) between iterations — all three must tear
  down cleanly with no unhandled rejections and all AbortSignals fired.
- Property test on the loop state: `messages` length strictly increases per
  iteration; every `tool_use` id has exactly one matching `tool_result`.

---

## Suggested Claude Code session order

1. One module per session; start each session with: "Read STATUS.md, PRINCIPLES.md,
   NON_GOALS.md and decisions/, then implement Module N per this plan" + paste the
   module section.
2. After each module: update `STATUS.md`, file the ADRs for its D-decisions, tag
   `v0.N.0`, push.
3. Modules 2 and 5 are small — good candidates to pair into one session if Module 1
   went smoothly. Module 4 is the largest; consider splitting at phase 3/4 boundary.
4. The Module 6 capstone test (agent + retrieval + memory + chain, all against the
   mock server) is the repo's definition of done — and the demo for the book chapter.
