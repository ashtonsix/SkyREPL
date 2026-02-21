// workflow/nodes/cleanup-features.ts - Cleanup Features Node
//
// Detaches all feature providers installed on the instance being terminated.
// Currently wires only the Tailscale feature provider; future providers
// (WandB, VS Code Server, etc.) follow the same FeatureProvider.detach() pattern.
//
// The node is idempotent: calling detach() on an instance with no Tailscale
// record is a silent no-op (see TailscaleFeatureProvider.detach).
//
// Spec ref: §8.4 (Feature Provider Interface).

import type { NodeExecutor, NodeContext } from "../engine.types";
import type { CleanupFeaturesOutput, TerminateInstanceInput } from "../../intent/terminate-instance";
import { TailscaleFeatureProvider } from "../../provider/feature/tailscale-provider";
import { TailscaleApiClient } from "../../provider/feature/tailscale-api";
import { TailscaleConfigError } from "../../provider/feature/tailscale-api";

// =============================================================================
// Helpers
// =============================================================================

/**
 * Build a TailscaleFeatureProvider from environment config.
 * Returns null if Tailscale is not configured (env vars absent) — the node
 * treats unconfigured Tailscale as "nothing to clean up".
 */
function makeTailscaleProvider(): TailscaleFeatureProvider | null {
  const apiKey = process.env.TAILSCALE_API_KEY;
  const tailnet = process.env.TAILSCALE_TAILNET;

  if (!apiKey || !tailnet) {
    return null;
  }

  const apiClient = new TailscaleApiClient({ apiKey, tailnet });
  return new TailscaleFeatureProvider(apiClient);
}

// =============================================================================
// Node Executor
// =============================================================================

export const cleanupFeaturesExecutor: NodeExecutor<unknown, CleanupFeaturesOutput> = {
  name: "cleanup-features",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<CleanupFeaturesOutput> {
    const input = ctx.workflowInput as TerminateInstanceInput;
    const instanceId = String(input.instanceId);

    let featuresCleanedUp = 0;

    // --- Tailscale -----------------------------------------------------------

    const tailscale = makeTailscaleProvider();

    if (!tailscale) {
      ctx.log("info", "Tailscale not configured, skipping Tailscale cleanup");
    } else {
      try {
        // status() is a cheap DB-only query (no API call when provider absent).
        const tailscaleState = await tailscale.status(instanceId);

        if (tailscaleState) {
          ctx.log("info", "Detaching Tailscale from instance", {
            instanceId,
            deviceId: tailscaleState.deviceId,
            ip: tailscaleState.ip,
          });

          await tailscale.detach(instanceId);
          featuresCleanedUp++;

          ctx.log("info", "Tailscale detached successfully", { instanceId });
        } else {
          ctx.log("debug", "No Tailscale installation found, skipping", { instanceId });
        }
      } catch (err) {
        // Non-fatal: log the error but don't block instance termination.
        // The provider logs the full error; we just note it here.
        ctx.log("warn", "Tailscale cleanup failed (non-fatal, continuing termination)", {
          instanceId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    // Future feature providers would be added here following the same pattern.

    return { featuresCleanedUp };
  },
};
