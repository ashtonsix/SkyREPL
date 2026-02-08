import { ApiClient } from '../client';

/**
 * Run command handler for Slice 1.
 *
 * Minimal implementation: accepts command string and launches run.
 * Full implementation will include:
 * - Profile resolution
 * - File collection and upload
 * - Preflight checks
 * - Progress streaming
 * - SSH config generation
 *
 * For Slice 1, this is a stub that throws "not implemented".
 *
 * @param args Command line arguments after "run"
 */
export async function runCommand(args: string[]): Promise<void> {
  throw new Error('runCommand not implemented');
}

/**
 * Run command options (full set from L2 spec).
 * For Slice 1, most of these are unused.
 */
export interface RunCommandOptions {
  profile?: string;
  command?: string;
  timeout?: string;
  workdir?: string;
  spot?: boolean;
  noSpot?: boolean;
  hold?: string;
  snapshot?: boolean;
  dryRun?: boolean;
  noStream?: boolean;
  noPreflight?: boolean;
  yes?: boolean;
  env?: string[];
  json?: boolean;
  quiet?: boolean;
}
