// cli/src/output/program.ts — Program consumer JSON envelope formatter
//
// When consumer=program (non-TTY, CI, or --json flag), emit a structured
// JSON envelope with status, data, follow_up actions, and file references.
//
// Exit codes:
//   0 — success
//   1 — workflow failed / command error
//   2 — CLI error (bad args, connection refused, etc.)

import { join } from 'path';
import { homedir } from 'os';

const REPL_DIR = join(homedir(), '.repl');

export interface ProgramEnvelope {
  status: 'ok' | 'error';
  data?: unknown;
  follow_up?: Array<{ action: string; command: string }>;
  files?: Record<string, string>;
  error?: string;
}

/**
 * Emit a JSON envelope to stdout.
 * Handles BigInt values (converts to Number) and circular structures (falls back to error envelope).
 */
export function emitEnvelope(envelope: ProgramEnvelope): void {
  try {
    console.log(JSON.stringify(envelope, (_, v) => typeof v === 'bigint' ? Number(v) : v, 2));
  } catch (err) {
    console.log(JSON.stringify({ status: 'error', error: `Failed to serialize response: ${err instanceof Error ? err.message : String(err)}` }));
  }
}

/**
 * Emit a success envelope with optional data.
 */
export function emitOk(data?: unknown, opts?: { follow_up?: ProgramEnvelope['follow_up']; files?: ProgramEnvelope['files'] }): void {
  const envelope: ProgramEnvelope = { status: 'ok' };
  if (data !== undefined) envelope.data = data;
  if (opts?.follow_up) envelope.follow_up = opts.follow_up;
  if (opts?.files) envelope.files = opts.files;
  emitEnvelope(envelope);
}

/**
 * Emit an error envelope and exit with code 2 (CLI error).
 */
export function emitCliError(message: string): never {
  emitEnvelope({ status: 'error', error: message });
  process.exit(2);
}

export interface ProgramRunContext {
  runId: number;
  workflowId: number;
  provider: string;
  spec: string;
  region?: string;
  command: string;
  runSlug: string;
  workflowSlug: string;
}

/**
 * Print program JSON envelope for `repl run` and exit immediately.
 * Does NOT block on workflow completion — same as agent path.
 */
export function formatProgramRunOutput(ctx: ProgramRunContext): void {
  const runDir = join(REPL_DIR, 'runs', ctx.runSlug);

  emitOk(
    {
      run_id: ctx.runSlug,
      workflow_id: ctx.workflowSlug,
      provider: ctx.provider,
      spec: ctx.spec,
      ...(ctx.region ? { region: ctx.region } : {}),
    },
    {
      follow_up: [
        { action: 'check_status', command: 'repl run list' },
        { action: 'stream_logs', command: `repl run attach ${ctx.runSlug}` },
        { action: 'cancel', command: `repl run cancel ${ctx.workflowSlug}` },
        { action: 'ssh', command: `ssh ${ctx.runSlug}` },
      ],
      files: {
        status: join(runDir, 'status.json'),
        logs: join(runDir, 'output.log'),
      },
    }
  );
}

/**
 * Print a program JSON envelope for a list operation.
 * Used for list commands when consumer=program.
 */
export function formatProgramData(data: unknown): void {
  emitOk(data);
}
