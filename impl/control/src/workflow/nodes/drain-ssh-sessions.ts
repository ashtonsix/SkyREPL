// workflow/nodes/drain-ssh-sessions.ts - Drain SSH Sessions Node
// Stub: SSH not yet implemented. Will terminate active SSH sessions in future.

import type { NodeExecutor, NodeContext } from "../engine.types";
import type { DrainSshSessionsOutput } from "../../intent/terminate-instance.types";

// =============================================================================
// Node Executor
// =============================================================================

export const drainSshSessionsExecutor: NodeExecutor<unknown, DrainSshSessionsOutput> = {
  name: "drain-ssh-sessions",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<DrainSshSessionsOutput> {
    ctx.log("info", "SSH draining not implemented, skipping");
    return { sessionsTerminated: 0 };
  },
};
