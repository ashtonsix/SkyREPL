import { getControlPlaneUrl, isConnectionRefused } from '../config';
import { ApiClient } from '../client';
import { parseInputId, idToSlug } from '@skyrepl/contracts';
import { isSSHAccessible } from '../ssh';

export async function sshCommand(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl ssh <allocation-id>');
    console.error('');
    console.error('SSH into a running allocation. The allocation must be ACTIVE,');
    console.error('or COMPLETE with an active debug hold.');
    console.error('');
    console.error('Examples:');
    console.error('  repl ssh serene-otter    # allocation by display name');
    console.error('  repl ssh abc123          # allocation by slug');
    console.error('  repl ssh repl-abc123     # also works (prefix stripped)');
    process.exit(2);
  }

  let input = args[0];
  // Strip 'repl-' prefix for muscle-memory compat (users accustomed to `ssh repl-<slug>`)
  if (input.startsWith('repl-')) {
    input = input.slice(5);
  }

  const client = new ApiClient(getControlPlaneUrl());

  // Fetch allocation — try display_name first (display names may look like valid base36),
  // fall back to base-36 slug parse
  let alloc: any;
  try {
    const { data } = await client.listAllocations();
    // Try display_name match first (e.g. "serene-otter", or base36-looking names like "bold", "cafe")
    alloc = data.find((a: any) => a.instance_display_name === input || a.display_name === input);
    // Fall back to base-36 slug parse
    if (!alloc) {
      let allocId: number | null = null;
      try { allocId = parseInputId(input); } catch { /* not a valid slug */ }
      if (allocId !== null) {
        alloc = data.find((a: any) => a.id === allocId);
      }
    }
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to look up allocation: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
    return;
  }

  if (!alloc) {
    console.error(`Allocation ${input} not found.`);
    process.exit(2);
    return;
  }

  // Check SSH accessibility using the canonical check function
  const slug = idToSlug(alloc.id);
  if (!isSSHAccessible(alloc)) {
    if (alloc.status === 'COMPLETE') {
      console.error(`Allocation ${slug} is COMPLETE and debug hold has expired.`);
      console.error(`Extend the hold with: repl extend ${slug}`);
    } else {
      console.error(`Allocation ${slug} is ${alloc.status} — SSH is only available for ACTIVE or held COMPLETE allocations.`);
    }
    process.exit(2);
    return;
  }
  // COMPLETE with active hold — accessible, but inform user of remaining hold time
  if (alloc.status === 'COMPLETE') {
    const remainMs = alloc.debug_hold_until - Date.now();
    const remainMin = Math.ceil(remainMs / 60_000);
    console.log(`Allocation ${slug} is COMPLETE (hold: ${remainMin}m remaining)`);
  }

  const ip = alloc.instance_ip;
  if (!ip) {
    console.error(`Allocation ${slug} has no IP address yet. The instance may still be booting.`);
    process.exit(2);
    return;
  }

  // Build ssh arguments
  const sshArgs = [
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'UserKnownHostsFile=/dev/null',
    '-o', 'LogLevel=ERROR',
    '-o', 'RequestTTY=yes',
    '-l', alloc.user || 'root',
    '-p', '22',
  ];

  // Add RemoteCommand to land in the right workdir
  if (alloc.workdir) {
    sshArgs.push('-t');  // Force TTY for RemoteCommand
    sshArgs.push(ip, `cd '${alloc.workdir.replace(/'/g, "'\\''")}' && exec $SHELL -l`);
  } else {
    sshArgs.push(ip);
  }

  // Exec into ssh with inherited stdio — direct terminal access, no pipes
  const proc = Bun.spawn(['ssh', ...sshArgs], {
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  });

  const exitCode = await proc.exited;
  process.exit(exitCode);
}
