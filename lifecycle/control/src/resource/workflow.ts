// resource/workflow.ts - Workflow Materializer (DB-authoritative)

import type { Workflow } from "../material/db";
import type { Materialized, MaterializeOptions } from "@skyrepl/contracts";
import { getWorkflow, queryMany } from "../material/db";
import { stampMaterialized } from "./materializer";

export function materializeWorkflow(id: number, _opts?: MaterializeOptions): Materialized<Workflow> | null {
  const record = getWorkflow(id);
  if (!record) return null;
  return stampMaterialized(record);
}

export function materializeWorkflowBatch(ids: number[], _opts?: MaterializeOptions): Materialized<Workflow>[] {
  if (ids.length === 0) return [];
  const placeholders = ids.map(() => "?").join(", ");
  const records = queryMany<Workflow>(`SELECT * FROM workflows WHERE id IN (${placeholders})`, ids);
  return records.map(stampMaterialized);
}
