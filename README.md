# rxjs-llm

LLM orchestration primitives built directly on RxJS — the useful core of
LangChain in pure TypeScript, with no framework machinery. Streams are
Observables, workflows are pipes, resilience is operators.

**Module 1 (this release): the Uniform Model Interface.** One `ChatModel`
contract over Anthropic, OpenAI, and Ollama, a normalized streaming event
taxonomy, a typed error taxonomy, and composable resilience operators.

```
bun install rxjs-llm   # or npm / pnpm — ESM only, rxjs@7.8 is the one dependency
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
| 3 — Chains | stages as operators, typed accumulating context | dual-channel `run()` shipped (ADR-0006); rest planned |
| 4 — Indexes | loaders, splitter, embeddings, vector stores, retriever | planned |
| 5 — Memory | conversation memory as fold + views | planned |
| 6 — Agents | tool loop as `expand()`, Zod tools, safety rails | planned |

Design decisions live in `decisions/`; scope boundaries in `NON_GOALS.md`;
the working conventions in `PRINCIPLES.md`. The commit history is written to
read as a tutorial — `git log --reverse --oneline` is the table of contents.

## Development

```
bun install
bun run test        # vitest, includes integration tests (no keys needed)
bun run typecheck   # strict TypeScript
```

MIT
