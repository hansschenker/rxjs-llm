import type { StreamEvent } from '../types';

/**
 * The agent's progress$ taxonomy (decision D6.4): model deltas and tool
 * lifecycle interleaved, tagged by iteration (1-based model-call count).
 * Same channel contract as chains (ADR-0006 via dual-channel.ts): exactly
 * one terminal event on natural termination, silence on cancellation.
 * Note that max_iterations terminates via `agent_complete` — exceeding the
 * budget is an ANSWER on result$, not a failure (ADR-0025).
 */
export type AgentEvent =
  | { type: 'model_event'; iteration: number; event: StreamEvent }
  | { type: 'tool_start'; iteration: number; id: string; tool: string; args: string }
  | {
      type: 'tool_result';
      iteration: number;
      id: string;
      tool: string;
      content: string;
      isError: boolean;
    }
  | { type: 'agent_complete' }
  | { type: 'agent_failed'; message: string };
