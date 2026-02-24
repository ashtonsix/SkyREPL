// ─────────────────────────────────────────────────────────────────────────────
// RAW DB LAYER — workflows table
// Business code should use materializeWorkflow() from resource/workflow.ts,
// not these functions directly. @see resource/workflow.ts
// DB operations below — add new queries here, not at call sites.
// ─────────────────────────────────────────────────────────────────────────────
// db/workflows.ts - Workflow and WorkflowNode CRUD

import { type SQLQueryBindings } from "bun:sqlite";
import { getDatabase, queryOne, queryMany } from "./helpers";

export interface Workflow {
  id: number;
  tenant_id: number;
  type: string;
  parent_workflow_id: number | null;
  depth: number;
  status:
    | "pending"
    | "running"
    | "paused"
    | "completed"
    | "failed"
    | "cancelled"
    | "rolling_back";
  current_node: string | null;
  input_json: string;
  output_json: string | null;
  error_json: string | null;
  manifest_id: number | null;
  trace_id: string | null;
  idempotency_key: string | null;
  created_by: number | null;
  timeout_ms: number | null;
  timeout_at: number | null;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  updated_at: number;
}

export interface WorkflowNode {
  id: number;
  workflow_id: number;
  node_id: string;
  node_type: string;
  status: "pending" | "running" | "completed" | "failed" | "skipped";
  input_json: string;
  output_json: string | null;
  error_json: string | null;
  depends_on: string | null;
  attempt: number;
  retry_reason: string | null;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  updated_at: number;
}

/** @see resource/workflow.ts — use materializeWorkflow() for business reads */
export function getWorkflow(id: number): Workflow | null {
  return queryOne<Workflow>("SELECT * FROM workflows WHERE id = ?", [id]);
}

export function createWorkflow(
  data: Omit<Workflow, "id" | "created_at" | "tenant_id" | "created_by">,
  tenantId: number = 1
): Workflow {
  const now = Date.now();
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO workflows (tenant_id, type, parent_workflow_id, depth, status, current_node, input_json, output_json, error_json, manifest_id, trace_id, idempotency_key, timeout_ms, timeout_at, created_at, started_at, finished_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    tenantId,
    data.type,
    data.parent_workflow_id,
    data.depth,
    data.status,
    data.current_node,
    data.input_json,
    data.output_json,
    data.error_json,
    data.manifest_id,
    data.trace_id,
    data.idempotency_key,
    data.timeout_ms,
    data.timeout_at,
    now,
    data.started_at,
    data.finished_at,
    data.updated_at
  );

  return getWorkflow(result.lastInsertRowid as number)!;
}

export function updateWorkflow(
  id: number,
  updates: Partial<Workflow>
): Workflow {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  if (!fields.includes("updated_at")) {
    fields.push("updated_at");
    (updates as Record<string, unknown>).updated_at = Date.now();
  }
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof Workflow]);

  db.prepare(`UPDATE workflows SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return getWorkflow(id)!;
}

/** @see resource/workflow.ts — use materializeWorkflow() for business reads */
export function getWorkflowNodes(workflowId: number): WorkflowNode[] {
  return queryMany<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ?",
    [workflowId]
  );
}

/** Get a specific workflow node by workflow ID and node ID. */
export function getWorkflowNode(workflowId: number, nodeId: string): WorkflowNode | null {
  return queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, nodeId]
  );
}

export function createWorkflowNode(
  data: Omit<WorkflowNode, "id" | "created_at">
): WorkflowNode {
  const db = getDatabase();
  const now = Date.now();

  const stmt = db.prepare(`
    INSERT INTO workflow_nodes (workflow_id, node_id, node_type, status, input_json, output_json, error_json, depends_on, attempt, retry_reason, created_at, started_at, finished_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    data.workflow_id,
    data.node_id,
    data.node_type,
    data.status,
    data.input_json,
    data.output_json,
    data.error_json,
    data.depends_on,
    data.attempt,
    data.retry_reason,
    now,
    data.started_at,
    data.finished_at,
    data.updated_at
  );

  return queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [result.lastInsertRowid as number])!;
}

export function updateWorkflowNode(
  id: number,
  updates: Partial<WorkflowNode>
): WorkflowNode {
  const db = getDatabase();

  const fields = Object.keys(updates).filter(k => k !== "id");
  if (!fields.includes("updated_at")) {
    fields.push("updated_at");
    (updates as Record<string, unknown>).updated_at = Date.now();
  }
  const setClause = fields.map(f => `${f} = ?`).join(", ");
  const values = fields.map(f => updates[f as keyof WorkflowNode]);

  db.prepare(`UPDATE workflow_nodes SET ${setClause} WHERE id = ?`)
    .run(...([...values, id] as SQLQueryBindings[]));

  return queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [id])!;
}

export function findReadyNodes(workflowId: number): WorkflowNode[] {
  const nodes = getWorkflowNodes(workflowId);
  const completedOrSkipped = new Set(
    nodes
      .filter(n => n.status === 'completed' || n.status === 'skipped')
      .map(n => n.node_id)
  );
  return nodes.filter(node => {
    if (node.status !== 'pending') return false;
    if (!node.depends_on || node.depends_on === '[]') return true;
    const deps = JSON.parse(node.depends_on) as string[];
    return deps.every(depId => completedOrSkipped.has(depId));
  });
}

export function findActiveWorkflows(): Workflow[] {
  return queryMany<Workflow>("SELECT * FROM workflows WHERE status IN ('pending', 'running')");
}

export function deleteWorkflow(id: number): void {
  const db = getDatabase();

  db.transaction(() => {
    db.prepare("DELETE FROM workflow_nodes WHERE workflow_id = ?").run(id);
    db.prepare("DELETE FROM workflows WHERE id = ?").run(id);
  })();
}
