import { getControlPlaneUrl, isConnectionRefused } from '../config';
import { ApiClient } from '../client';
import { parseDuration } from '../project';
import { idToSlug, parseInputId } from '@skyrepl/contracts';
import { regenerateSSHConfig } from '../ssh';

const DEFAULT_EXTEND_DURATION = '30m';

/**
 * `repl extend <allocation-id> [duration]`
 *
 * Extends the debug hold on a COMPLETE allocation.
 * Duration defaults to 30m if not specified.
 * Supported suffixes: s, m, h (bare numbers treated as milliseconds).
 */
export async function extendCommand(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl extend <allocation-id> [duration]');
    console.error('');
    console.error('Examples:');
    console.error('  repl extend 1a2b         # extend by 30m (default)');
    console.error('  repl extend 1a2b 1h      # extend by 1 hour');
    console.error('  repl extend 1a2b 5m      # extend by 5 minutes');
    process.exit(2);
  }

  const allocationSlug = args[0];
  let allocationId: number;
  try {
    allocationId = parseInputId(allocationSlug);
  } catch {
    console.error(`Invalid allocation ID: ${allocationSlug}`);
    process.exit(2);
  }

  const durationStr = args[1] ?? DEFAULT_EXTEND_DURATION;
  let durationMs: number;
  try {
    durationMs = parseDuration(durationStr);
  } catch {
    console.error(`Invalid duration: "${durationStr}". Use a value like "30m", "1h", or "45s".`);
    process.exit(2);
  }

  const baseUrl = getControlPlaneUrl();
  const client = new ApiClient(baseUrl);

  try {
    await client.extendAllocation(allocationId, durationMs);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${baseUrl}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to extend allocation: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  try {
    await regenerateSSHConfig(client);
  } catch {
    // Non-fatal: SSH config regeneration failure should not fail the command
  }

  // Format duration for display
  const displayDuration = formatDuration(durationMs);
  console.log(`Extended allocation ${idToSlug(allocationId)} hold by ${displayDuration}`);
}

/**
 * `repl release <allocation-id>`
 *
 * Clears the debug hold on a COMPLETE allocation, allowing the instance
 * to be reclaimed immediately.
 */
export async function releaseCommand(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl release <allocation-id>');
    console.error('');
    console.error('Examples:');
    console.error('  repl release 1a2b');
    process.exit(2);
  }

  const allocationSlug = args[0];
  let allocationId: number;
  try {
    allocationId = parseInputId(allocationSlug);
  } catch {
    console.error(`Invalid allocation ID: ${allocationSlug}`);
    process.exit(2);
  }

  const baseUrl = getControlPlaneUrl();
  const client = new ApiClient(baseUrl);

  try {
    await client.releaseAllocation(allocationId);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${baseUrl}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to release allocation: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  try {
    await regenerateSSHConfig(client);
  } catch {
    // Non-fatal: SSH config regeneration failure should not fail the command
  }

  console.log(`Released allocation ${idToSlug(allocationId)} debug hold`);
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Format milliseconds as a human-readable duration string.
 * Uses the largest clean unit (hours, minutes, seconds).
 */
function formatDuration(ms: number): string {
  if (ms >= 3_600_000 && ms % 3_600_000 === 0) {
    const h = ms / 3_600_000;
    return `${h}h`;
  }
  if (ms >= 60_000 && ms % 60_000 === 0) {
    const m = ms / 60_000;
    return `${m}m`;
  }
  if (ms >= 1_000 && ms % 1_000 === 0) {
    const s = ms / 1_000;
    return `${s}s`;
  }
  return `${ms}ms`;
}
