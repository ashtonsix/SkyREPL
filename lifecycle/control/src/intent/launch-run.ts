// intent/launch-run.ts - Launch Run Intent

import { TIMING } from "@skyrepl/contracts";
import { createRunRecord } from "../resource/run";
import { getWorkflow, type Workflow } from "../material/db";
import type { WorkflowBlueprint, NodeExecutor, NodeContext } from "../workflow/engine.types";
import { submit, registerBlueprint, registerNodeExecutor } from "../workflow/engine";
import {
  LaunchRunWorkflowInputSchema,
  NODE_OUTPUT_SCHEMAS,
} from "./launch-run.schema";

// =============================================================================
// Re-exports from schema (all node output types + workflow input)
// =============================================================================

export type {
  LaunchRunWorkflowInput,
  CheckBudgetOutput,
  ResolveInstanceOutput,
  ClaimAllocationOutput,
  SpawnInstanceOutput,
  WaitForBootOutput,
  CreateAllocationOutput,
  StartRunOutput,
  WaitCompletionOutput,
  FinalizeOutput,
} from './launch-run.schema';

// =============================================================================
// Launch Run Input
// =============================================================================

export interface LaunchRunInput {
  /** DB ID for the run record */
  runId: number;

  /** Command to execute */
  command: string;

  /** Instance specification (e.g., "gpu-a100-80gb") */
  spec: string;

  /** Whether to check warm pool first (default: true) */
  preferWarmPool?: boolean;

  /** Try spot, fall back to on-demand (default: true) */
  allowSpotFallback?: boolean;

  /** Specific provider, or let system choose */
  provider?: string;

  /** Specific region, or let system choose */
  region?: string;

  /** Override workdir (default: auto-assigned) */
  workdir?: string;

  /** Environment variables */
  env?: Record<string, string>;

  /** Timeout in milliseconds (default: 24 hours) */
  maxDurationMs?: number;

  /** Checksum for warm pool matching */
  initChecksum?: string;

  /** Files to sync */
  files?: Array<{ path: string; checksum: string; sizeBytes?: number }>;

  /** Glob patterns for artifact collection */
  artifactPatterns?: string[];

  /** Hold period after run in ms (default: 5 min) */
  holdDurationMs?: number;

  /** Create snapshot after run (default: false) */
  createSnapshot?: boolean;

  /** For duplicate submission prevention */
  idempotencyKey?: string;

  /** Tenant ID (from auth context) */
  tenantId?: number;

  /** Root disk size in GB (default: AMI default, typically 8GB) */
  diskSizeGb?: number;
}

// =============================================================================
// Launch Run Output
// =============================================================================

export interface LaunchRunOutput {
  workflowId: number;
  runId: number;
  instanceId: number;
  allocationId: number;
  fromWarmPool: boolean;
  exitCode: number | null;
  spotInterrupted: boolean;
  snapshotId?: number;
  artifacts?: Array<{ id: number; path: string; sizeBytes: number }>;
}

// Node executor imports
import { resolveInstanceExecutor } from "../workflow/nodes/resolve-instance";
import { claimAllocationExecutor } from "../workflow/nodes/claim-allocation";
import { spawnInstanceExecutor } from "../workflow/nodes/spawn-instance";
import { waitForBootExecutor } from "../workflow/nodes/wait-for-boot";
import { createAllocationExecutor } from "../workflow/nodes/create-allocation";
import { startRunExecutor } from "../workflow/nodes/start-run";
import { waitCompletionExecutor } from "../workflow/nodes/wait-completion";
import { finalizeExecutor } from "../workflow/nodes/finalize";

// =============================================================================
// Spec Resolution (D4)
// =============================================================================

import { resolveSpecViaOrbital, type OrbitalResolveResult } from "../provider/orbital";
import { isProviderRegistered } from "../provider/registry";
import { SkyREPLError } from "@skyrepl/contracts";

/**
 * Validate and resolve spec at submission time.
 *
 * Rules:
 * - "provider:spec" prefix bypasses orbital (power-user override)
 * - Zero results → throw 400 with suggestions
 * - Exactly one result → proceed (potentially rewrite provider/spec)
 * - Multiple results + no provider → return all for now (dead-simple)
 * - Orbital down → passthrough to existing behaviour
 */
export async function validateSpec(input: LaunchRunInput): Promise<LaunchRunInput> {
  const spec = input.spec;

  // Power-user bypass: "provider:spec" prefix (e.g. "aws:p4d.24xlarge")
  if (spec.includes(":") && !spec.includes("/")) {
    const colonIdx = spec.indexOf(":");
    const prefix = spec.slice(0, colonIdx);
    if (isProviderRegistered(prefix)) {
      // Region comes from provider config when not specified by user.
      // Orbital resolution is skipped for explicit provider:spec syntax.
      return {
        ...input,
        provider: prefix,
        spec: spec.slice(colonIdx + 1),
      };
    }
  }

  // Call orbital for resolution
  const result = await resolveSpecViaOrbital(spec, input.provider, input.region);

  // Orbital down → passthrough
  if (!result) return input;

  // Zero results → 400
  if (!result.results || result.results.length === 0) {
    throw new SkyREPLError(
      "SPEC_NOT_FOUND",
      `No matching instance type found for spec "${spec}". Try a provider-native spec (e.g. "p4d.24xlarge", "gpu_1x_a100") or a GPU name (e.g. "a100", "h100").`,
      "validation",
    );
  }

  // Use scored results if available (orbital now returns scored specs).
  // Scored results are pre-sorted by composite score (match + price + capability).
  // Falls back to raw results sorted by price if scoring unavailable.
  const scored = result.scored;

  if (scored && scored.length > 0) {
    // Filter by explicit provider if given
    const candidates = input.provider
      ? scored.filter(s => s.resolved.provider === input.provider)
      : scored;

    if (candidates.length > 0) {
      const best = candidates[0]!;
      return {
        ...input,
        provider: input.provider ?? best.resolved.provider,
        spec: best.resolved.spec,
        region: input.region ?? best.resolved.instance.regions[0],
      };
    }
  }

  // Fallback: use raw results (scoring unavailable or no scored matches)
  let candidates = result.results;
  if (input.provider) {
    const forProvider = candidates.filter(r => r.provider === input.provider);
    if (forProvider.length > 0) candidates = forProvider;
  }

  // Exactly one result → use it
  if (candidates.length === 1) {
    const resolved = candidates[0]!;
    return {
      ...input,
      provider: input.provider ?? resolved.provider,
      spec: resolved.spec,
      region: input.region ?? resolved.instance.regions[0],
    };
  }

  // Multiple results — sort by price as fallback
  const sorted = candidates.sort((a, b) =>
    (a.instance.onDemandHourly ?? Infinity) - (b.instance.onDemandHourly ?? Infinity)
  );
  return {
    ...input,
    provider: input.provider ?? sorted[0]!.provider,
    spec: sorted[0]!.spec,
    region: input.region ?? sorted[0]!.instance.regions[0],
  };
}

// =============================================================================
// Entry Point
// =============================================================================

export async function launchRun(input: LaunchRunInput): Promise<Workflow> {
  // Validate and resolve spec before workflow submission (D4)
  const resolvedInput = await validateSpec(input);

  // Create a Run record in the database
  const run = createRunRecord({
    command: resolvedInput.command,
    workdir: resolvedInput.workdir ?? "/workspace",
    max_duration_ms: resolvedInput.maxDurationMs ?? TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS,
    workflow_state: "launch-run:pending",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: resolvedInput.initChecksum ?? null,
    create_snapshot: resolvedInput.createSnapshot ? 1 : 0,
    spot_interrupted: 0,
    started_at: null,
    finished_at: null,
  }, resolvedInput.tenantId);

  // Submit the workflow
  const result = await submit({
    type: "launch-run",
    input: { ...resolvedInput, runId: run.id } as unknown as Record<string, unknown>,
    idempotencyKey: resolvedInput.idempotencyKey,
    tenantId: resolvedInput.tenantId,
  });

  // Return the workflow record
  return getWorkflow(result.workflowId)!;
}

// =============================================================================
// Workflow Blueprint
// =============================================================================

export const launchRunBlueprint: WorkflowBlueprint = {
  type: "launch-run",
  entryNode: "check-budget",
  inputSchema: LaunchRunWorkflowInputSchema,
  nodeOutputSchemas: NODE_OUTPUT_SCHEMAS,
  nodes: {
    "check-budget": {
      type: "check-budget",
    },
    "resolve-instance": {
      type: "resolve-instance",
      dependsOn: ["check-budget"],
    },
    "claim-warm-allocation": {
      type: "claim-allocation",
      dependsOn: ["resolve-instance"],
    },
    "spawn-instance": {
      type: "spawn-instance",
      dependsOn: ["resolve-instance"],
    },
    "wait-for-boot": {
      type: "wait-for-boot",
      dependsOn: ["spawn-instance"],
      timeout: TIMING.INSTANCE_BOOT_TIMEOUT_MS,
    },
    "create-allocation": {
      type: "create-allocation",
      dependsOn: ["claim-warm-allocation", "wait-for-boot"],
    },
    "sync-files": {
      type: "start-run",
      dependsOn: ["create-allocation"],
      timeout: TIMING.SYNC_TIMEOUT_MS,
    },
    "await-completion": {
      type: "wait-completion",
      dependsOn: ["sync-files"],
      timeout: TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS,
    },
    "finalize-run": {
      type: "finalize",
      dependsOn: ["await-completion"],
    },
  },
};

// =============================================================================
// Error Handling Map
// =============================================================================

// =============================================================================
// Registration
// =============================================================================

export function registerLaunchRun(): void {
  registerBlueprint(launchRunBlueprint);
  registerNodeExecutor(checkBudgetExecutor);
  registerNodeExecutor(resolveInstanceExecutor);
  registerNodeExecutor(claimAllocationExecutor);
  registerNodeExecutor(spawnInstanceExecutor);
  registerNodeExecutor(waitForBootExecutor);
  registerNodeExecutor(createAllocationExecutor);
  registerNodeExecutor(startRunExecutor);
  registerNodeExecutor(waitCompletionExecutor);
  registerNodeExecutor(finalizeExecutor);
}

// =============================================================================
// Check Budget Executor
// =============================================================================

export const checkBudgetExecutor: NodeExecutor<unknown, { budgetOk: boolean }> = {
  name: "check-budget",
  idempotent: true,
  async execute(_ctx: NodeContext): Promise<{ budgetOk: boolean }> {
    // Slice 1: no budget enforcement
    return { budgetOk: true };
  },
};
