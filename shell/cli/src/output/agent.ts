// cli/src/output/agent.ts — Agent consumer output formatter
//
// When consumer=agent, print structured natural-language output and EXIT
// IMMEDIATELY without blocking on completion. The agent can follow up using
// the listed actions or watch the file-based status.
//
// This is intentional: agents should not be blocked waiting for spinners or
// interactive prompts. They can poll the status file or run `repl run list`.

import { join } from 'path';
import { homedir } from 'os';
const REPL_DIR = join(homedir(), '.repl');

export interface AgentRunContext {
  runId: number;
  workflowId: number;
  provider: string;
  spec: string;
  region?: string;
  command: string;
  runSlug: string;
  workflowSlug: string;
  /** False when the run artifact directory could not be created (E11). */
  runDirAvailable?: boolean;
}

/**
 * Print agent-friendly NL output for `repl run` and exit immediately.
 * Does NOT block on workflow completion.
 */
export function formatAgentRunOutput(ctx: AgentRunContext): void {
  const location = ctx.region ? `${ctx.spec} in ${ctx.region}` : ctx.spec;
  const runDir = join(REPL_DIR, 'runs', ctx.runSlug);

  console.log(`Launching run on ${ctx.provider} ${location}...`);
  console.log(`Run ID: ${ctx.runSlug}  Workflow: ${ctx.workflowSlug}`);
  console.log('');
  console.log('Follow-up actions:');
  console.log(`  - Check status:  repl run list`);
  console.log(`  - Stream logs:   repl run attach ${ctx.runSlug}`);
  console.log(`  - Cancel:        repl run cancel ${ctx.workflowSlug}`);
  console.log(`  - SSH:           ssh ${ctx.runSlug}`);
  console.log('');
  if (ctx.runDirAvailable !== false) {
    console.log('File references (written by daemon):');
    console.log(`  Live status: ${join(runDir, 'status.json')}`);
    console.log(`  Live logs:   ${join(runDir, 'output.log')}`);
  }
}

/**
 * Print agent-friendly plain-text output for a general data response.
 * Used for list commands when consumer=agent.
 *
 * - Array of objects → tab-separated table (no box-drawing, no ANSI)
 * - Single object    → key=value pairs
 * - Empty array      → "No <label> found."
 */
export function formatAgentData(label: string, data: unknown): void {
  if (Array.isArray(data)) {
    if (data.length === 0) {
      console.log(`No ${label} found.`);
      return;
    }

    const objects = data.filter(item => item && typeof item === 'object');
    if (objects.length === 0) {
      // Primitive array — one per line
      for (const item of data) {
        console.log(String(item));
      }
      return;
    }

    // Collect all keys that appear across all objects
    const keys: string[] = [];
    const keySet = new Set<string>();
    for (const obj of objects) {
      for (const k of Object.keys(obj as Record<string, unknown>)) {
        if (!keySet.has(k)) {
          keySet.add(k);
          keys.push(k);
        }
      }
    }

    // Header row
    console.log(keys.join('\t'));

    // Data rows
    for (const obj of objects) {
      const row = keys.map(k => {
        const v = (obj as Record<string, unknown>)[k];
        if (v === null || v === undefined) return '';
        if (typeof v === 'object') return JSON.stringify(v);
        return String(v);
      });
      console.log(row.join('\t'));
    }

    console.log('');
    console.log('Follow-up actions:');
    console.log(`  - Refresh:  repl ${label} list`);
    return;
  }

  if (data && typeof data === 'object') {
    for (const [k, v] of Object.entries(data as Record<string, unknown>)) {
      const val = v === null || v === undefined ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);
      console.log(`${k}=${val}`);
    }
    return;
  }

  // Scalar fallback
  console.log(String(data));
}
