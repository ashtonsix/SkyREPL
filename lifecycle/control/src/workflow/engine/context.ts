// workflow/engine/context.ts - Build NodeContext for node execution

import type {
  NodeContext,
  SubworkflowHandle,
  SubworkflowResult,
} from "../engine.types";
import {
  getWorkflow,
  addResourceToManifest,
  queryOne,
  type WorkflowNode,
} from "../../material/db";
import { getControlId } from "../../material/control-id";
import { getDatabase } from "../../material/db";
import {
  applyConditionalBranch,
  applyRetryWithAlternative,
  applyInsertAndReconverge,
  applyParallelFanOut,
  type InsertAndReconvergeConfig,
  type ParallelFanOutConfig,
} from "../patterns";
import { sleep, MAX_SUBWORKFLOW_DEPTH, POLL_INTERVAL_MS } from "./helpers";

export function buildNodeContext(
  workflowId: number,
  node: WorkflowNode
): NodeContext {
  const workflow = getWorkflow(workflowId);
  const manifestId = workflow?.manifest_id ?? null;
  const controlId = getControlId(getDatabase());

  const ctx: NodeContext = {
    workflowId,
    nodeId: node.node_id,
    input: node.input_json ? JSON.parse(node.input_json) : {},
    workflowInput: workflow?.input_json ? JSON.parse(workflow.input_json) : {},
    manifestId,
    controlId,
    tenantId: workflow?.tenant_id ?? 1,

    emitResource(type: string, id: number | string, cleanupPriority: number): void {
      if (manifestId === null) {
        console.error("[workflow] emitResource skipped: no manifest", { workflowId, nodeId: node.node_id });
        return;
      }
      try {
        addResourceToManifest(manifestId, type, String(id), {
          cleanupPriority,
        });
      } catch (err) {
        console.error("[workflow] emitResource failed", {
          workflowId,
          nodeId: node.node_id,
          type,
          id,
          error: err,
        });
      }
    },

    async claimResource(
      _targetManifestId: number,
      resourceType: string,
      resourceId: string
    ): Promise<boolean> {
      if (manifestId === null) {
        console.error("[workflow] claimResource skipped: no manifest", { workflowId, nodeId: node.node_id });
        return false;
      }
      const { claimResourceAtomic } = await import("../../resource/manifest");
      return claimResourceAtomic(manifestId, resourceType, resourceId);
    },

    applyPattern(pattern: string, config: unknown): void {
      // Delegate to pattern functions
      switch (pattern) {
        case "conditional-branch":
          applyConditionalBranch(
            workflowId,
            config as Parameters<typeof applyConditionalBranch>[1]
          );
          break;
        case "retry-with-alternative":
          {
            const rwaConfig = config as {
              failedNodeId: string;
              alternativeNode: Parameters<typeof applyRetryWithAlternative>[2];
            };
            applyRetryWithAlternative(
              workflowId,
              rwaConfig.failedNodeId,
              rwaConfig.alternativeNode
            );
          }
          break;
        case "insert-and-reconverge":
          applyInsertAndReconverge(
            workflowId,
            config as InsertAndReconvergeConfig
          );
          break;
        case "parallel-fan-out":
          applyParallelFanOut(
            workflowId,
            config as ParallelFanOutConfig
          );
          break;
        default:
          console.warn("[workflow] Unknown pattern", { pattern });
      }
    },

    getNodeOutput(nodeId: string): unknown {
      const targetNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [workflowId, nodeId]
      );
      if (!targetNode || !targetNode.output_json) return null;
      return JSON.parse(targetNode.output_json);
    },

    log(
      level: "info" | "warn" | "error" | "debug",
      message: string,
      data?: Record<string, unknown>
    ): void {
      // Console-based logging for now; OTel deferred
      const logFn =
        level === "error"
          ? console.error
          : level === "warn"
            ? console.warn
            : level === "debug"
              ? console.debug
              : console.log;
      logFn(`[workflow:${node.node_type}] ${message}`, {
        workflowId,
        nodeId: node.node_id,
        ...data,
      });
    },

    checkCancellation(): void {
      const current = getWorkflow(workflowId);
      if (current && (current.status === "cancelled" || current.status === "cancelling")) {
        throw Object.assign(new Error("Workflow cancelled"), {
          code: "CANCELLED",
          category: "internal" as const,
        });
      }
    },

    async sleep(ms: number): Promise<void> {
      await sleep(ms);
    },

    async spawnSubworkflow(
      type: string,
      input: unknown
    ): Promise<SubworkflowHandle> {
      const wf = getWorkflow(workflowId);
      const depth = (wf?.depth ?? 0) + 1;
      if (depth > MAX_SUBWORKFLOW_DEPTH) {
        throw new Error(
          `Max subworkflow depth (${MAX_SUBWORKFLOW_DEPTH}) exceeded`
        );
      }

      const parentId = workflowId; // capture parent workflow ID to avoid shadowing

      // Dynamic import to break circular dependency: context -> lifecycle -> executor -> context
      const { submit, cancelWorkflow } = await import("./lifecycle");

      const result = await submit({
        type,
        input: input as Record<string, unknown>,
        parentWorkflowId: parentId,
      });

      return {
        workflowId: result.workflowId,
        async wait(): Promise<SubworkflowResult> {
          // Poll until subworkflow completes, with parent cancellation awareness
          while (true) {
            // Check if parent was cancelled or failed
            const parent = getWorkflow(parentId);
            if (parent && (parent.status === "cancelled" || parent.status === "cancelling" || parent.status === "failed")) {
              await cancelWorkflow(result.workflowId, "parent_cancelled");
            }

            const sub = getWorkflow(result.workflowId);
            if (!sub) {
              return { status: "failed", error: "Subworkflow not found" };
            }
            if (
              sub.status === "completed" ||
              sub.status === "failed" ||
              sub.status === "cancelled"
            ) {
              return {
                status: sub.status as SubworkflowResult["status"],
                output: sub.output_json
                  ? JSON.parse(sub.output_json)
                  : undefined,
                error: sub.error_json ?? undefined,
                manifestId: sub.manifest_id ?? undefined,
              };
            }
            await sleep(POLL_INTERVAL_MS);
          }
        },
      };
    },
  };

  return ctx;
}
