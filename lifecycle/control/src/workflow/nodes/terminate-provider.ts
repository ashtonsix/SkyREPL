// workflow/nodes/terminate-provider.ts - Terminate Provider Node
// Calls the provider API to terminate the actual VM/instance.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getProvider } from "../../provider/registry";
import type { ProviderName } from "../../provider/types";
import type {
  TerminateInstanceInput,
  ValidateInstanceOutput,
  TerminateProviderOutput,
} from "../../intent/terminate-instance";

// =============================================================================
// Node Executor
// =============================================================================

export const terminateProviderExecutor: NodeExecutor<unknown, TerminateProviderOutput> = {
  name: "terminate-provider",
  idempotent: true,

  // NOT compensatable — termination is irreversible

  async execute(ctx: NodeContext): Promise<TerminateProviderOutput> {
    const validateOutput = ctx.getNodeOutput("validate-instance") as ValidateInstanceOutput;

    const { provider: providerName, providerId } = validateOutput;

    if (!providerId) {
      ctx.log("info", "No provider ID, skipping provider termination", {
        instanceId: validateOutput.instanceId,
      });
      return { terminated: true, providerId: "" };
    }

    try {
      const provider = await getProvider(providerName as ProviderName);
      await provider.terminate(providerId);

      ctx.log("info", "Provider instance terminated", {
        provider: providerName,
        providerId,
      });

      return { terminated: true, providerId };
    } catch (err) {
      // Handle already-terminated gracefully
      const errMsg = err instanceof Error ? err.message : String(err);

      // If the provider says the instance is already gone, treat as success
      if (
        errMsg.includes("not found") ||
        errMsg.includes("NOT_FOUND") ||
        errMsg.includes("does not exist") ||
        errMsg.includes("already terminated")
      ) {
        ctx.log("info", "Provider instance already terminated", {
          provider: providerName,
          providerId,
          reason: errMsg,
        });
        return { terminated: true, providerId };
      }

      // Re-throw for genuine errors
      throw err;
    }
  },
};
