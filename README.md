# rxjs-llm

LLM orchestration primitives built directly on RxJS — the useful core of
LangChain in roughly 3,100 lines of pure TypeScript, with no framework
machinery. Streams are Observables, workflows are pipes, resilience is
operators, memory is a fold, and the agent loop is an `expand()` recursion.

All six modules are complete. Two runtime dependencies: `rxjs` and `zod`.

```
bun install rxjs-llm   # or npm / pnpm — ESM only
```

## The contract

```ts
interface ChatModel {
  stream(messages: ChatMessage[], options?: ChatOptions): Observable<StreamEvent>;
  complete(messages: ChatMessage[], options?: ChatOptions): Observable<ChatCompletion>;
}
```

Every Observable this package returns obeys four laws, enforced by tests:

1. **Cold** — each subscribe issues exactly one HTTP request.
2. **Lazy** — no fetch before subscribe, and nothing emits synchronously in
   the subscribe call frame.
3. **Unicast** — subscribers never share a request.
4. **Teardown-complete** — unsubscribe aborts the underlying fetch.
   Cancellation is silent: it never surfaces on the error channel
   (`decisions/0005`).

All adapters emit one event union, whatever their wire format:

```ts
type StreamEvent =
  | { type: 'message_start'; model: string }
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_delta'; id: string; name?: string; argsDelta: string }
  | { type: 'usage'; input: number; output: number }
  | { type: 'message_stop'; stopReason: StopReason };
```

## Quick start — Anthropic

```ts
import { anthropic } from 'rxjs-llm';

const model = anthropic({
  apiKey: process.env['ANTHROPIC_API_KEY']!,
  model: 'claude-sonnet-4-6',
});

model
  .stream([{ role: 'user', content: 'Why are RxJS streams cold?' }])
  .subscribe((event) => {
    if (event.type === 'text_delta') process.stdout.write(event.text);
  });
```

## OpenAI

```ts
import { openai } from 'rxjs-llm';
import { firstValueFrom } from 'rxjs';

const model = openai({ apiKey: process.env['OPENAI_API_KEY']!, model: 'gpt-4o' });

const completion = await firstValueFrom(
  model.complete([{ role: 'user', content: 'One sentence on token buckets.' }]),
);
console.log(completion.text, completion.usage);
```

## Ollama (local, NDJSON — not SSE)

```ts
import { ollama } from 'rxjs-llm';

const model = ollama({ model: 'llama3.2' }); // http://localhost:11434

model
  .stream([{ role: 'user', content: 'Hei from Rapperswil!' }])
  .subscribe((event) => {
    if (event.type === 'text_delta') process.stdout.write(event.text);
  });
```

## Resilience is composition

The operators know the error taxonomy; you just pipe them:

```ts
import { retryWithBackoff, streamTimeout } from 'rxjs-llm';

model.stream(messages).pipe(
  streamTimeout({ firstByteMs: 10_000, idleMs: 30_000 }),
  retryWithBackoff({ maxRetries: 3, baseMs: 500 }),
);
```

- `retryWithBackoff` — jittered exponential; honors `Retry-After` on 429s;
  retries only what `isRetryable` says is worth retrying.
- `streamTimeout` — two timeouts, not one: *first-byte* (provider overloaded —
  retryable, nothing was generated) vs *idle* (stalled mid-generation — not
  retryable by default, tokens were already billed). `decisions/0003`.
- `rateLimit` — a token bucket: bursts pass instantly, the queue drains at
  the sustained rate.

Cancelling everything — the HTTP request included — is just `unsubscribe()`:

```ts
const sub = model.stream(messages).subscribe(render);
sub.unsubscribe(); // aborts the fetch; no error fires anywhere
```

## Typed prompts

Placeholders are extracted **at the type level** — missing or extra
variables are compile errors, not runtime surprises:

```ts
import { promptTemplate, prompt } from 'rxjs-llm';

const summarize = promptTemplate('Summarize {doc} in {n} bullets, plain language.');
summarize({ doc: text, n: 5 });   // => string
summarize({ doc: text });         // ✗ compile error: 'n' is missing
summarize({ doc, n: 5, x: 1 });   // ✗ compile error: 'x' is not a placeholder
```

Two forms, two escape rules: the parsed form above uses `{{`/`}}` for
literal braces; the tagged form takes placeholder names from the
interpolations and needs no escapes at all:

```ts
const qa = prompt`Answer ${'question'} using only ${'context'}.`;
qa({ question, context }); // exact same compile-time checking
```

Message prompts assemble a full `ChatMessage[]` — system turn, few-shot
examples, a history slot, and the user turn:

```ts
import { messagePrompt } from 'rxjs-llm';

const ask = messagePrompt({
  system: 'Answer only from the provided context. Say "unknown" otherwise.',
  fewShot: [{ user: 'Q: capital of France? Context: ...', assistant: 'Paris' }],
  user: 'Q: {question}\nContext: {context}',
});

model.stream(ask({ question, context }));                       // plain ChatMessage[]
model.stream(ask({ question, context }).withHistory(history));  // memory splices in
```

`withHistory` is pure and splices between the few-shot block and the final
user turn — examples stay pinned to the system prompt, the question stays
last. Format helpers are plain string transformers:

```ts
import { asBullets, asJson, noJargon } from 'rxjs-llm';

noJargon()(asBullets(3)(summarize({ doc, n: 3 })));
const format = asJson(schema); // renders the JSON Schema into the prompt,
format(extractPrompt);         // carries .schema for parsing later
```

Everything in the prompt layer is pure — no I/O, no Observables. Streams
enter only when a chain feeds a prompt's output to a `ChatModel`.

## Chains are pipes

No `Runnable`, no callback manager. A stage is an RxJS operator; a chain is
a pipe of them, with a typed context accumulating each stage's patch —
downstream stages can only reference keys upstream stages provably produced
(a missing key is a compile error):

```ts
import { chain, stage, stages, collectText } from 'rxjs-llm';

const pipeline = chain<{ url: string; question: string }>({ trace: consoleSink }).pipe(
  stage('fetch', ctx => fetchPage(ctx.url)),                       // + { page }
  stages.parallel({                                                // forkJoin fan-out
    summary:  (ctx, emit) => model.stream([user(summarize({ page: ctx.page }))])
      .pipe(collectText(emit), map(summary => ({ summary }))),
    keywords: (ctx, emit) => model.stream([user(`Keywords: ${ctx.page}`)])
      .pipe(collectText(emit), map(keywords => ({ keywords }))),
  }),
  stage.when('clarify', ctx => ctx.question.endsWith('?'),         // conditional
    ctx => of({ clarified: ctx.question.replace('?', '') })),
  stage('answer', (ctx, emit) => model
    .stream([user(qa({ question: ctx.question, context: ctx.summary }))])
    .pipe(retryWithBackoff({ maxRetries: 2 }),                     // Module 1 composes inside
          collectText(emit), map(answer => ({ answer })))),
);

const { result$, progress$ } = pipeline.run({ url, question });
```

Two channels per run: `result$` drives the work and delivers the final
context; `progress$` passively streams stage-tagged model events for UIs,
ending in exactly one terminal event (`run_complete` | `run_failed`). One
`run()` call is one execution — the outcome is latched, so `retry()` on
`result$` re-delivers the same error rather than silently re-running three
LLM calls (whole-chain retry is an explicit new `run()`; see
`decisions/0006`). Per-stage failure policy is `onError: 'fail' | 'skip' |
fallback`, errors carry their stage via `stageOf(error)`, and `traced()`
reports stage lifecycle to any `TraceSink` — the LangSmith seam is one
interface with one method.

## RAG is a pipe

Loaders stream documents, the splitter is pure and lossless (chunks carry
exact source offsets), embeddings mirror the `ChatModel` pattern, and the
whole ingestion pipeline is one subscription:

```ts
import { textFileLoader, splitDocs, embedBatched, upsertInto, memoryStore,
         openaiEmbedder, retrieveContext } from 'rxjs-llm';

const store = memoryStore(); // brute-force cosine, honest to ~50k vectors
const embedder = openaiEmbedder({ apiKey, model: 'text-embedding-3-small' });

textFileLoader('./docs').pipe(
  splitDocs({ maxTokens: 400, overlap: 40 }),
  embedBatched(embedder, { batchSize: 64, requestsPerInterval: 10, intervalMs: 1000 }),
  upsertInto(store),
).subscribe();       // unsubscribe = stop the walk, abort the in-flight request

// Retrieval is one operator — drop it into a chain stage:
stage('retrieve', ctx =>
  of(ctx.question).pipe(retrieveContext(store, embedder, 6, { tokenBudget: 1500 })))
```

The `VectorStore` interface has two implementations verified by one shared
contract suite: in-memory, and PGlite (WASM Postgres + pgvector via
Drizzle) behind the opt-in `rxjs-llm/pglite` subpath — importing it is what
adds those dependencies; the core stays rxjs-only.

## Memory is a fold

One reducer (a `scan` over the turn stream), swappable views. ~150 lines —
the value is the design:

```ts
import { createMemory, summaryView, tokenBudgetView, windowView } from 'rxjs-llm';

const summary = summaryView(model, undefined, { foldAfter: 12, keepRecent: 2 });
const memory = createMemory({ view: summary });

memory.record({ user: question, assistant: answer });
memory.view();        // Observable<ChatMessage[]> — reactive, updates per record
summary.pending$;     // true while a summarization fold is in flight

// splice into a message prompt's history slot (Module 2):
model.stream(qa({ question }).withHistory(history));
```

Views: `fullView()`, `windowView(n)`, `tokenBudgetView(n)` (whole turns,
oldest-first eviction), and `summaryView(model)` — an async fold that is
eventually consistent (the conversation never blocks on summarization),
never overlaps folds, degrades to raw turns on summarizer failure, and
aborts cleanly on `dispose()`. Persistence is `snapshot()`/`restore` only:
turns are the truth, views are projections, hosts persist however they like.

## Agents are a recursion

The tool-call loop is `expand()` over Module 1's native tool-use events —
no ReAct scaffolding, providers reason natively. Tools are Zod-defined:
one schema validates the model's arguments at runtime, derives the
provider JSON Schema (zod v4's `z.toJSONSchema`), and types the handler:

```ts
import { runAgent, tool } from 'rxjs-llm';
import { z } from 'zod';

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

The robustness trick: invalid arguments, unknown tools, execution
failures, and timeouts all become tool-result messages **returned to the
model** — it self-corrects on the next turn, and the loop can never stall
waiting for a missing result. Exceeding `maxIterations` is an **outcome**
(`{ type: 'max_iterations', messages, … }`), not an error: a budget is
policy, and policy outcomes are data. Unsubscribing aborts the in-flight
model call and every in-flight tool (each receives an `AbortSignal`) —
the cancellation matrix (mid-stream, mid-tool, between iterations) is
pinned by test.

`progress$` interleaves model deltas and tool lifecycle events, tagged by
iteration, with the same audited channel contract as chains — literally
the same implementation (`dual-channel.ts`), which is why an agent
composes into a chain as an ordinary stage.

## The whole thing, end to end

The capstone test (`test/agent/capstone.test.ts`) is the library in one
pipeline — six modules, real HTTP, no API keys:

```ts
const searchDocs = tool({                                   // Module 6
  name: 'search_docs',
  input: z.object({ query: z.string() }),
  execute: ({ query }) => of(query).pipe(
    retrieveContext(store, embedder, 3),                    // Module 4
    map(r => r.context)),
});

const pipeline = chain<{ question: string }>({ trace }).pipe(   // Module 3
  stage('agent', (ctx, emit) => defer(() => {
    const agent = runAgent(model, {                         // Modules 6 + 1
      tools: [searchDocs],
      messages: [user(ask({ question: ctx.question }))],    // Module 2
      maxIterations: 5,
    });
    const forward = agent.progress$.subscribe(e => {
      if (e.type === 'model_event') emit(e.event);          // deltas flow up
    });
    return agent.result$.pipe(
      finalize(() => forward.unsubscribe()),
      map(o => ({ answer: o.type === 'complete' ? o.text : '(budget exceeded)' })));
  })),
  stage('remember', ctx => {
    memory.record({ user: ctx.question, assistant: ctx.answer }); // Module 5
    return of({ remembered: true });
  }),
);

const { result$, progress$ } = pipeline.run({ question });
```

Model deltas stream from inside the agent, through the chain's
`progress$`, to your UI; the tool's body is the RAG retriever; the answer
lands in conversation memory; unsubscribing anywhere tears the whole
stack down to the socket. `git log --reverse --oneline` reads as the
tutorial that builds this up from `fetch`.

## Errors are typed

```
LlmError (provider, requestId)
├── TransportError      DNS, TLS, socket reset          → retryable
├── HttpError           non-2xx; 429/5xx retryable, 4xx not
│     └── RateLimitError  429, carries retryAfterMs
├── ParseError          malformed SSE/NDJSON            → not retryable
├── ProviderError       in-stream error events          → per provider code
└── TimeoutError        first-byte vs idle              → per phase
```

There is deliberately no `AbortError`: unsubscription is teardown, not failure.

## Testing without API keys

`test/helpers/mock-server.ts` speaks all three wire formats over real HTTP
(node:http, so it runs under Bun and Node). The integration suite — including
the test proving that unsubscribe aborts the request server-side — runs with
zero credentials. The SSE parser is additionally tested at **every possible
two-chunk split position** of an adversarial fixture (frames split
mid-`data:`, mid-UTF-8-codepoint, between CR and LF).

## Roadmap

| Module | Contents | Status |
|--------|----------|--------|
| 1 — Models | `ChatModel`, adapters, transport, resilience operators | ✅ v0.1.0 |
| 2 — Prompts | typed templates, compile-time-checked placeholders | ✅ v0.2.0 |
| 3 — Chains | stages as operators, typed accumulating context | ✅ v0.3.0 |
| 4 — Indexes | loaders, splitter, embeddings, vector stores, retriever | ✅ v0.4.0 |
| 5 — Memory | conversation memory as fold + views | ✅ v0.5.0 |
| 6 — Agents | tool loop as `expand()`, Zod tools, safety rails | ✅ v0.6.0 |

Design decisions live in `decisions/` (27 ADRs); scope boundaries in
`NON_GOALS.md`; the working conventions in `PRINCIPLES.md`. The commit
history is written to read as a tutorial — `git log --reverse --oneline`
is the table of contents, from `fetch` to the capstone.

## Development

```
bun install
bun run test        # vitest, includes integration tests (no keys needed)
bun run typecheck   # strict TypeScript
```

MIT
