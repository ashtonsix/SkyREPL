import { getControlPlaneUrl, isConnectionRefused } from '../config';
import { ApiClient } from '../client';
import { parseInputId, idToSlug } from '@skyrepl/contracts';

export async function sshCommand(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl ssh <allocation-id>');
    console.error('');
    console.error('SSH into a running allocation. The allocation must be ACTIVE,');
    console.error('or COMPLETE with an active debug hold.');
    console.error('');
    console.error('Examples:');
    console.error('  repl ssh abc123          # allocation by slug');
    console.error('  repl ssh repl-abc123     # also works (prefix stripped)');
    process.exit(2);
  }

  let input = args[0];
  // Strip 'repl-' prefix for muscle-memory compat (users accustomed to `ssh repl-<slug>`)
  if (input.startsWith('repl-')) {
    input = input.slice(5);
  }

  const allocId = parseInputId(input);
  const client = new ApiClient(getControlPlaneUrl());

  // Fetch allocation
  let alloc: any;
  try {
    const { data } = await client.listAllocations();
    alloc = data.find((a: any) => a.id === allocId);
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

  // Check SSH accessibility
  const slug = idToSlug(alloc.id);
  if (alloc.status === 'COMPLETE') {
    if (!alloc.debug_hold_until || alloc.debug_hold_until <= Date.now()) {
      console.error(`Allocation ${slug} is COMPLETE and debug hold has expired.`);
      console.error(`Extend the hold with: repl extend ${slug}`);
      process.exit(2);
      return;
    }
    // COMPLETE with active hold — OK, but inform user
    const remainMs = alloc.debug_hold_until - Date.now();
    const remainMin = Math.ceil(remainMs / 60_000);
    console.log(`Allocation ${slug} is COMPLETE (hold: ${remainMin}m remaining)`);
  } else if (alloc.status !== 'ACTIVE') {
    console.error(`Allocation ${slug} is ${alloc.status} — SSH is only available for ACTIVE or held COMPLETE allocations.`);
    process.exit(2);
    return;
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
