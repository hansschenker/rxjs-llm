# DEFINITION

`rxjs-llm` is a reference implementation of LLM orchestration primitives built
directly on RxJS — the useful core of LangChain in roughly 2,500 lines of pure
TypeScript, with no framework machinery. It doubles as the artifact for a book
chapter: the commit history is written to read as a tutorial.

## What it provides

1. **Uniform model interface** — one `ChatModel` contract over Anthropic,
   OpenAI, and Ollama, streaming a normalized `StreamEvent` union.
2. **Prompts** — typed templates with compile-time-checked placeholders.
3. **Chains** — workflow composition where stages are RxJS operators.
4. **Indexes / RAG** — loaders, splitter, embeddings, vector stores, retriever.
5. **Memory** — conversation memory as a fold with swappable views.
6. **Agents** — the tool-call loop as an `expand()` recursion with safety rails.

## Definition of done

The Module 6 capstone test: retrieve (Module 4) → agent with tools (Module 6)
→ memory record (Module 5), composed as a chain (Module 3), running against
the mock provider server (Module 1) with no API keys.
