import { readFileSync, existsSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { getConsumer } from './consumer';
import { formatAgentData } from './output/agent';
import { formatProgramData } from './output/program';
import { idToSlug } from '@skyrepl/contracts';

// ─── Output Mode (CLI-01) ────────────────────────────────────────────────────

export type OutputMode = 'normal' | 'json' | 'quiet';
let currentOutputMode: OutputMode = 'normal';

export function setOutputMode(mode: OutputMode): void {
  currentOutputMode = mode;
}

export function getOutputMode(): OutputMode {
  return currentOutputMode;
}

/**
 * Output data respecting the current consumer type and output mode.
 *
 * Priority:
 *   1. --json flag / consumer=program → JSON envelope via formatProgramData
 *   2. consumer=agent                 → plain-text via formatAgentData
 *   3. --quiet flag                   → IDs only (one per line for arrays)
 *   4. human (default)               → call humanFormat() if provided
 */
export function output(data: unknown, humanFormat?: () => void, agentLabel?: string): void {
  const mode = getOutputMode();
  const consumer = getConsumer();

  if (mode === 'json' || consumer === 'program') {
    formatProgramData(data);
  } else if (consumer === 'agent') {
    formatAgentData(agentLabel ?? 'results', data);
  } else if (mode === 'quiet') {
    // Arrays: print slug of each item's id on its own line
    if (Array.isArray(data)) {
      for (const item of data) {
        if (item && typeof item === 'object' && 'id' in item) {
          console.log(idToSlug((item as any).id));
        }
      }
    } else if (typeof data === 'object' && data !== null && 'id' in data) {
      console.log(idToSlug((data as any).id));
    }
  } else {
    // Human mode: delegate to the caller's formatter
    if (humanFormat) humanFormat();
  }
}

/**
 * Load a KEY=VALUE env file into process.env (lowest priority — won't overwrite existing vars).
 * Blank lines and lines starting with # are ignored. No shell expansion.
 */
export function loadEnvFile(filePath: string): void {
  if (!existsSync(filePath)) return;
  const content = readFileSync(filePath, 'utf-8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let value = trimmed.slice(eqIdx + 1).trim();
    // Strip optional quotes
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    // Don't overwrite existing env vars
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

/**
 * Load a KEY=VALUE env file, always overwriting existing values.
 * Used for authoritative config (e.g. ~/.repl/control.env) that must win
 * over Bun's automatic .env loading from the workspace root.
 */
export function forceLoadEnvFile(filePath: string): void {
  if (!existsSync(filePath)) return;
  const content = readFileSync(filePath, 'utf-8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let value = trimmed.slice(eqIdx + 1).trim();
    // Strip optional quotes
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    // Always overwrite — this is the authoritative config source
    process.env[key] = value;
  }
}

/** Path to the ~/.repl directory. */
export const REPL_DIR = join(homedir(), '.repl');

/**
 * Get control plane URL from environment or config file.
 *
 * Resolution order:
 * 1. SKYREPL_CONTROL_PLANE_URL environment variable (includes cli.env, loaded at startup)
 * 2. Default: http://localhost:3000
 */
export function getControlPlaneUrl(): string {
  if (process.env.SKYREPL_CONTROL_PLANE_URL) {
    return process.env.SKYREPL_CONTROL_PLANE_URL;
  }
  return 'http://localhost:3000';
}

/**
 * Get the API key from ~/.repl/api-key.
 * Returns null if the file doesn't exist (control plane not started yet).
 *
 * The file may contain a second line with the expiry timestamp (ms since epoch).
 * If the key will expire within 14 days, a warning is printed to stderr.
 */
export function getApiKey(): string | null {
  const keyFile = join(REPL_DIR, 'api-key');
  if (!existsSync(keyFile)) return null;
  const contents = readFileSync(keyFile, 'utf-8').trim().split('\n');
  const key = contents[0].trim() || null;
  if (key && contents[1]) {
    const expiresAt = parseInt(contents[1].trim(), 10);
    if (!isNaN(expiresAt)) {
      const msRemaining = expiresAt - Date.now();
      const daysRemaining = Math.ceil(msRemaining / (24 * 60 * 60 * 1000));
      if (daysRemaining <= 14 && daysRemaining > 0) {
        process.stderr.write(`Warning: API key expires in ${daysRemaining} day${daysRemaining === 1 ? '' : 's'}. Run \`repl auth rotate\` to generate a new key.\n`);
      }
    }
  }
  return key;
}

/** Check if an error is a connection-refused error (works with both Bun and Node). */
export function isConnectionRefused(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const code = (err as any).code;
  return code === 'ConnectionRefused' || code === 'ECONNREFUSED' ||
    err.message.includes('ECONNREFUSED') || err.message.includes('Unable to connect');
}

/** Print a table with padded columns. */
export function printTable(headers: string[], rows: string[][], widths: number[]): void {
  const fmt = (row: string[]) => row.map((v, i) => v.padEnd(widths[i])).join('  ');
  console.log(fmt(headers));
  console.log(widths.map(w => '-'.repeat(w)).join('  '));
  for (const row of rows) {
    console.log(fmt(row));
  }
}

/** Map internal workflow_state to a short display string. */
export function displayState(state: string): string {
  if (state.includes('finalized')) return 'completed';
  if (state.includes('executing') || state.includes('syncing')) return 'running';
  if (state.includes('spawning')) return 'spawning';
  if (state.includes('compensat')) return 'cancelling';
  if (state.includes('failed')) return 'failed';
  if (state.includes('pending')) return 'pending';
  if (state.includes('complete')) return 'completed';
  return state;
}
