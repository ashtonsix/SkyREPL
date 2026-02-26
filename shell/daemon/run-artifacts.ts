// daemon/run-artifacts.ts — Per-run artifact directory management
//
// Manages ~/.repl/runs/<slug>/ directories for agent consumers.
//
// Provides:
//   - createRunDir(slug)        — create dir on workflow start
//   - writeRunStatus(slug, status) — write status.json from control plane SSE
//   - appendRunLog(slug, data)  — append to output.log
//   - cleanupOldRuns()          — retention-based cleanup (last N runs or 7 days)
//
// All operations are fire-and-forget; errors are swallowed.

import { existsSync, mkdirSync, writeFileSync, appendFileSync, readdirSync, statSync, rmSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';

const HOME = homedir();
const REPL_DIR = join(HOME, '.repl');
const RUNS_DIR = join(REPL_DIR, 'runs');

// Retention policy
const MAX_RUNS = 50;
const MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

// =============================================================================
// Directory lifecycle
// =============================================================================

/**
 * Create the run directory for a given slug.
 * Called when a workflow starts.
 * Returns true on success, false if the directory could not be created
 * (caller should omit file paths from agent output in that case).
 */
export function createRunDir(slug: string): boolean {
  try {
    const dir = runDir(slug);
    mkdirSync(dir, { recursive: true });
    writeRunStatus(slug, { state: 'pending', created_at: Date.now() });
    return true;
  } catch {
    return false;
  }
}

/**
 * Initialize status.json immediately after submission with run metadata.
 * Called by the CLI right after launchRun() returns, before exiting.
 * Provides agents with runId/workflowId from the start, rather than waiting
 * for the first terminal SSE event.
 */
export function initRunStatus(slug: string, meta: {
  runId: number;
  workflowId: number;
  submittedAt: number;
}): void {
  writeRunStatus(slug, {
    state: 'submitted',
    run_id: meta.runId,
    workflow_id: meta.workflowId,
    submitted_at: meta.submittedAt,
  });
}

/**
 * Write a status.json snapshot to the run directory.
 * Called on each relevant SSE event from the control plane.
 */
export function writeRunStatus(slug: string, status: Record<string, unknown>): void {
  try {
    const dir = runDir(slug);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    writeFileSync(
      join(dir, 'status.json'),
      JSON.stringify({ ...status, updated_at: Date.now() }, null, 2) + '\n',
      { mode: 0o600 },
    );
  } catch {
    // Non-fatal
  }
}

/**
 * Append data to the run's output.log.
 * Called when log lines arrive via WebSocket.
 */
export function appendRunLog(slug: string, data: string): void {
  try {
    const dir = runDir(slug);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    appendFileSync(join(dir, 'output.log'), data);
  } catch {
    // Non-fatal
  }
}

// =============================================================================
// Cleanup
// =============================================================================

/**
 * Remove old run directories by retention policy.
 * Keeps the most recent MAX_RUNS runs, and removes anything older than MAX_AGE_MS.
 *
 * Called periodically by the daemon.
 */
export function cleanupOldRuns(): void {
  try {
    if (!existsSync(RUNS_DIR)) return;

    const entries = readdirSync(RUNS_DIR, { withFileTypes: true })
      .filter(e => e.isDirectory())
      .map(e => {
        const dir = join(RUNS_DIR, e.name);
        try {
          const st = statSync(dir);
          return { name: e.name, dir, mtimeMs: st.mtimeMs };
        } catch {
          return null;
        }
      })
      .filter((e): e is NonNullable<typeof e> => e !== null)
      .sort((a, b) => b.mtimeMs - a.mtimeMs); // newest first

    const now = Date.now();
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      const tooOld = (now - entry.mtimeMs) > MAX_AGE_MS;
      const overLimit = i >= MAX_RUNS;

      if (tooOld || overLimit) {
        try {
          rmSync(entry.dir, { recursive: true, force: true });
        } catch {
          // Non-fatal
        }
      }
    }
  } catch {
    // Non-fatal
  }
}

// =============================================================================
// Helpers
// =============================================================================

function runDir(slug: string): string {
  return join(RUNS_DIR, slug);
}
