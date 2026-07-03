import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';

/**
 * A real-HTTP mock of all three providers, built on node:http so the suite
 * runs under Bun and plain Node (ADR-0004). Each endpoint echoes the last
 * user message as `echo: <content>` in its provider's genuine wire format:
 *
 *   POST /anthropic/v1/messages        → Anthropic SSE events
 *   POST /openai/v1/chat/completions   → OpenAI chunks + [DONE]
 *   POST /ollama/api/chat              → NDJSON lines
 *
 * A user message containing `[slow]` stalls the stream after the first text
 * delta — the hook the cancellation test uses to unsubscribe mid-response.
 */
/** One scripted model turn for the scenario DSL (Module 6's agent tests). */
export interface ScenarioTurn {
  text?: string;
  toolCalls?: { id: string; name: string; args: Record<string, unknown> }[];
}

export interface MockServer {
  url: string;
  /** URLs of requests the client aborted before the response finished. */
  aborted: string[];
  /** URL of every request received, in arrival order — the D3.3 pinning counter. */
  requests: string[];
  /** Parsed body of every request, index-aligned with `requests`. */
  bodies: unknown[];
  /**
   * Scenario DSL: turn N answers the Nth Anthropic request — "on turn 1
   * emit these tool calls; on turn 2 emit final text". Unscripted servers
   * echo the last user message.
   */
  script(turns: ScenarioTurn[]): void;
  close(): Promise<void>;
}

export async function startMockServer(): Promise<MockServer> {
  const aborted: string[] = [];
  const requests: string[] = [];
  const bodies: unknown[] = [];
  let scenario: ScenarioTurn[] | undefined;
  let scenarioTurn = 0;

  const server = createServer((req, res) => {
    requests.push(req.url ?? '');
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      res.on('close', () => {
        if (!res.writableFinished) aborted.push(req.url ?? '');
      });
      const request = JSON.parse(body === '' ? '{}' : body) as {
        model?: string;
        messages?: { role: string; content: unknown }[];
      };
      bodies.push(request);

      if (scenario !== undefined && req.url === '/anthropic/v1/messages') {
        const turn = scenario[scenarioTurn] ?? { text: '(scenario exhausted)' };
        scenarioTurn += 1;
        void serveScenarioTurn(res, request.model ?? 'mock-model', turn);
        return;
      }

      const prompt = lastUserContent(request);
      const reply = `echo: ${prompt.replace('[slow] ', '').replace('[straggler] ', '')}`;
      const slow = prompt.includes('[slow]');
      // [straggler]: stall like [slow], but when the client aborts, attempt
      // to write the tail frames anyway — the latch-race fixture variant.
      const straggler = prompt.includes('[straggler]');
      const model = request.model ?? 'mock-model';

      void (req.url === '/anthropic/v1/messages'
        ? serveAnthropic(res, model, reply, slow || straggler, straggler)
        : req.url === '/openai/v1/chat/completions'
          ? serveOpenAi(res, model, reply, slow)
          : req.url === '/ollama/api/chat'
            ? serveOllama(res, model, reply, slow)
            : void res.writeHead(404).end());
    });
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    aborted,
    requests,
    bodies,
    script: (turns) => {
      scenario = turns;
      scenarioTurn = 0;
    },
    close: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

function lastUserContent(request: { messages?: { role: string; content: unknown }[] }): string {
  const last = [...(request.messages ?? [])].reverse().find((m) => m.role === 'user');
  return typeof last?.content === 'string' ? last.content : '';
}

/** Unref'd so a stalled [slow] response never holds the process open. */
function delay(ms: number, res: ServerResponse<IncomingMessage>): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref();
    res.on('close', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

const STALL_MS = 30_000;

/** Serve one scripted turn in Anthropic's genuine wire format. */
async function serveScenarioTurn(
  res: ServerResponse<IncomingMessage>,
  model: string,
  turn: ScenarioTurn,
): Promise<void> {
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const sse = (event: string, data: unknown): void => {
    try {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    } catch {
      /* client gone */
    }
  };

  sse('message_start', {
    type: 'message_start',
    message: { model, usage: { input_tokens: 5 } },
  });

  let blockIndex = 0;
  if (turn.text !== undefined && turn.text !== '') {
    sse('content_block_start', {
      type: 'content_block_start',
      index: blockIndex,
      content_block: { type: 'text', text: '' },
    });
    const half = Math.ceil(turn.text.length / 2);
    for (const piece of [turn.text.slice(0, half), turn.text.slice(half)]) {
      if (piece === '') continue;
      sse('content_block_delta', {
        type: 'content_block_delta',
        index: blockIndex,
        delta: { type: 'text_delta', text: piece },
      });
    }
    sse('content_block_stop', { type: 'content_block_stop', index: blockIndex });
    blockIndex += 1;
  }

  for (const toolCall of turn.toolCalls ?? []) {
    sse('content_block_start', {
      type: 'content_block_start',
      index: blockIndex,
      content_block: { type: 'tool_use', id: toolCall.id, name: toolCall.name },
    });
    const args = JSON.stringify(toolCall.args);
    const half = Math.ceil(args.length / 2);
    for (const piece of [args.slice(0, half), args.slice(half)]) {
      if (piece === '') continue;
      sse('content_block_delta', {
        type: 'content_block_delta',
        index: blockIndex,
        delta: { type: 'input_json_delta', partial_json: piece },
      });
    }
    sse('content_block_stop', { type: 'content_block_stop', index: blockIndex });
    blockIndex += 1;
  }

  await delay(2, res);
  sse('message_delta', {
    type: 'message_delta',
    delta: { stop_reason: (turn.toolCalls?.length ?? 0) > 0 ? 'tool_use' : 'end_turn' },
    usage: { output_tokens: 7 },
  });
  sse('message_stop', { type: 'message_stop' });
  try {
    res.end();
  } catch {
    /* already torn down */
  }
}

async function serveAnthropic(
  res: ServerResponse<IncomingMessage>,
  model: string,
  reply: string,
  slow: boolean,
  attemptTailAfterAbort = false,
): Promise<void> {
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const sse = (event: string, data: unknown): void => {
    try {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    } catch {
      /* socket already gone — the straggler frame goes nowhere */
    }
  };
  const half = Math.ceil(reply.length / 2);

  sse('message_start', {
    type: 'message_start',
    message: { model, usage: { input_tokens: 5 } },
  });
  sse('content_block_start', {
    type: 'content_block_start',
    index: 0,
    content_block: { type: 'text', text: '' },
  });
  sse('content_block_delta', {
    type: 'content_block_delta',
    index: 0,
    delta: { type: 'text_delta', text: reply.slice(0, half) },
  });
  await delay(slow ? STALL_MS : 2, res);
  if (res.destroyed && !attemptTailAfterAbort) return;
  sse('content_block_delta', {
    type: 'content_block_delta',
    index: 0,
    delta: { type: 'text_delta', text: reply.slice(half) },
  });
  sse('content_block_stop', { type: 'content_block_stop', index: 0 });
  sse('message_delta', {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn' },
    usage: { output_tokens: 7 },
  });
  sse('message_stop', { type: 'message_stop' });
  try {
    res.end();
  } catch {
    /* already torn down */
  }
}

async function serveOpenAi(
  res: ServerResponse<IncomingMessage>,
  model: string,
  reply: string,
  slow: boolean,
): Promise<void> {
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const sse = (data: unknown): void => {
    if (!res.destroyed) {
      res.write(`data: ${typeof data === 'string' ? data : JSON.stringify(data)}\n\n`);
    }
  };
  const half = Math.ceil(reply.length / 2);

  sse({ model, choices: [{ index: 0, delta: { role: 'assistant' }, finish_reason: null }] });
  sse({ model, choices: [{ index: 0, delta: { content: reply.slice(0, half) } }] });
  await delay(slow ? STALL_MS : 2, res);
  if (res.destroyed) return;
  sse({ model, choices: [{ index: 0, delta: { content: reply.slice(half) } }] });
  sse({ model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] });
  sse({ model, choices: [], usage: { prompt_tokens: 5, completion_tokens: 7 } });
  sse('[DONE]');
  res.end();
}

async function serveOllama(
  res: ServerResponse<IncomingMessage>,
  model: string,
  reply: string,
  slow: boolean,
): Promise<void> {
  res.writeHead(200, { 'content-type': 'application/x-ndjson' });
  const line = (data: unknown): void => {
    if (!res.destroyed) res.write(`${JSON.stringify(data)}\n`);
  };
  const half = Math.ceil(reply.length / 2);

  line({ model, message: { role: 'assistant', content: reply.slice(0, half) }, done: false });
  await delay(slow ? STALL_MS : 2, res);
  if (res.destroyed) return;
  line({ model, message: { role: 'assistant', content: reply.slice(half) }, done: false });
  line({
    model,
    message: { role: 'assistant', content: '' },
    done: true,
    done_reason: 'stop',
    prompt_eval_count: 5,
    eval_count: 7,
  });
  res.end();
}
