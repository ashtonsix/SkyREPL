#!/usr/bin/env bun

import { mkdirSync } from 'fs';
import { join } from 'path';
import { runCommand } from './commands/run';
import { controlCommand, ensureControlPlane } from './commands/control';
import { instanceCommand } from './commands/instance';
import { allocationCommand } from './commands/allocation';
import { artifactCommand } from './commands/artifact';
import { setupCommand } from './commands/setup';
import { authCommand } from './commands/auth';
import { teamCommand } from './commands/team';
import { extendCommand, releaseCommand } from './commands/hold';
import { orphanCommand } from './commands/orphan';
import { getControlPlaneUrl, isConnectionRefused, loadEnvFile, forceLoadEnvFile, REPL_DIR, printTable, displayState, setOutputMode, output } from './config';
import { ApiClient } from './client';
import { idToSlug, parseInputId } from '@skyrepl/contracts';
import { regenerateSSHConfig, ensureSSHConfigInclude } from './ssh';
import { ensureDaemon } from '../../daemon/lifecycle';
import { detectConsumer, setConsumer } from './consumer';

/**
 * Extract --json and --quiet from the raw argv array.
 * Returns the remaining args with those flags stripped.
 */
function parseGlobalFlags(args: string[]): { json: boolean; quiet: boolean; remainingArgs: string[] } {
  let json = false;
  let quiet = false;
  const remainingArgs: string[] = [];
  for (const arg of args) {
    if (arg === '--json') { json = true; }
    else if (arg === '--quiet') { quiet = true; }
    else { remainingArgs.push(arg); }
  }
  return { json, quiet, remainingArgs };
}

function printUsage(exitCode = 1) {
  const out = exitCode === 0 ? console.log : console.error;
  out('Usage: repl <command> [options]');
  out('');
  out('Commands:');
  out('  run [options]          Launch a run (alias: run launch)');
  out('  run attach <run-id>    Attach to a running run\'s log stream');
  out('  run list               List runs');
  out('  run cancel <id>        Cancel a workflow');
  out('  control <subcommand>   Manage services (start|stop|restart|status|reset) [service]');
  out('  instance list          List instances');
  out('  allocation list        List allocations');
  out('  orphan whitelist add   Add to orphan whitelist');
  out('  orphan whitelist remove Remove from orphan whitelist');
  out('  orphan whitelist list   List orphan whitelist entries');
  out('  artifact list <run-id> List artifacts for a run');
  out('  artifact download <id> Download an artifact');
  out('  extend <id> [duration] Extend debug hold on an allocation (default: 30m)');
  out('  release <id>           Release debug hold on an allocation');
  out('  ssh <allocation-id>    SSH into a running allocation');
  out('  team create <name>     Create a team');
  out('  team invite <email>    Invite a user to your team');
  out('  team remove <user-id>  Remove a user from your team');
  out('  team list              List team members');
  out('  team info              Show team info and usage');
  out('  team budget            Show/set budget caps');
  out('  auth create-key        Create an API key (admin only)');
  out('  auth revoke-key <id>   Revoke an API key (admin only)');
  out('  auth list-keys         List API keys (admin only)');
  out('  setup [subcommand]     First-run setup (URL, SSH, auth)');
  out('  ssh-config             Regenerate SSH config');
  out('  completion <shell>     Generate shell completion script');
  out('  help                   Show this help');
  out('');
  out('Global flags (before or after command):');
  out('  --json                 Output as JSON ({ "data": ... } envelope)');
  out('  --quiet                Minimal output (IDs only; errors only for warnings)');
  out('');
  process.exit(exitCode);
}

async function main() {
  mkdirSync(REPL_DIR, { recursive: true });
  // control.env wins over Bun-auto-loaded .env from the workspace root
  forceLoadEnvFile(join(REPL_DIR, 'control.env'));
  loadEnvFile(join(REPL_DIR, 'cli.env'));

  const rawArgs = process.argv.slice(2);
  const { json, quiet, remainingArgs: args } = parseGlobalFlags(rawArgs);

  // Detect consumer type early. --json flag overrides to program mode.
  if (json) {
    setConsumer('program');
    setOutputMode('json');
  } else if (quiet) {
    setOutputMode('quiet');
  } else {
    // Auto-detect: set the cached consumer type for the rest of this process
    const detected = detectConsumer();
    setConsumer(detected);
    if (detected === 'program') setOutputMode('json');
  }

  if (args.length === 0 || args[0] === '-h' || args[0] === '--help') {
    printUsage(0);
  }

  const command = args[0];
  const commandArgs = args.slice(1);

  // Hidden flag for shell completion scripts — must be before command dispatch
  if (command === '--list-completions') {
    const { listCompletions } = await import('./completions');
    listCompletions(commandArgs[0] ?? '');
    return;
  }

  // Ensure services are running for commands that need them.
  // Scaffold starts the daemon in-process; standalone daemon only needed when remote.
  const localOnly = ['help', 'control', 'setup', 'completion'];
  if (!localOnly.includes(command)) {
    await ensureControlPlane();
    // When control is remote, scaffold isn't local — start standalone daemon
    const url = getControlPlaneUrl();
    const isLocal = ['localhost', '127.0.0.1', '::1'].some(h => url.includes(h));
    if (!isLocal) ensureDaemon();
  }

  switch (command) {
    case 'help':
      printUsage(0);
      break;
    case 'run':
      if (commandArgs[0] === 'list') {
        await runList();
      } else if (commandArgs[0] === 'cancel') {
        await runCancel(commandArgs.slice(1));
      } else if (commandArgs[0] === 'launch') {
        await runCommand(commandArgs.slice(1));
      } else if (commandArgs[0] === 'attach') {
        await runAttach(commandArgs.slice(1));
      } else {
        await runCommand(commandArgs);
      }
      break;
    case 'extend':
      await extendCommand(commandArgs);
      break;
    case 'release':
      await releaseCommand(commandArgs);
      break;
    case 'ssh': {
      const { sshCommand } = await import('./commands/ssh');
      await sshCommand(commandArgs);
      break;
    }
    case 'control':
      await controlCommand(commandArgs);
      break;
    case 'instance':
      await instanceCommand(commandArgs);
      break;
    case 'allocation':
      await allocationCommand(commandArgs);
      break;
    case 'artifact':
      await artifactCommand(commandArgs);
      break;
    case 'orphan':
      await orphanCommand(commandArgs);
      break;
    case 'team':
      await teamCommand(commandArgs);
      break;
    case 'auth':
      await authCommand(commandArgs);
      break;
    case 'setup':
      await setupCommand(commandArgs);
      break;
    case 'ssh-config':
      await sshConfigCommand();
      break;
    case 'completion': {
      const { completionCommand } = await import('./commands/completion');
      await completionCommand(commandArgs);
      break;
    }
    default:
      console.error(`Unknown command: ${command}`);
      printUsage();
  }
}

async function sshConfigCommand(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    // Ensure ~/.ssh/config includes our generated config
    const modified = await ensureSSHConfigInclude();
    if (modified) {
      console.log('Added `Include ~/.repl/ssh_config` to ~/.ssh/config');
    }

    const count = await regenerateSSHConfig(client);
    console.log(`SSH config written: ${count} allocation entr${count === 1 ? 'y' : 'ies'} + catch-all fallback`);
    console.log(`  ~/.repl/ssh_config`);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to generate SSH config: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function runList(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const [runResult, wfResult] = await Promise.all([
      client.listRuns(),
      client.listWorkflows().catch(() => ({ data: [] as any[] })),
    ]);
    const { data } = runResult;

    if (data.length === 0) {
      output([], () => { console.log('No runs found.'); }, 'run');
      return;
    }

    // Build run→workflow status map (workflow status is authoritative for cancel/fail)
    const wfStatusByRunId = new Map<number, string>();
    for (const wf of wfResult.data) {
      try {
        const input = typeof wf.input_json === 'string' ? JSON.parse(wf.input_json) : wf.input_json;
        if (input?.runId) wfStatusByRunId.set(input.runId, wf.status);
      } catch { /* skip */ }
    }

    const rows = data.map((run: any) => {
      const cmd = run.command?.length > 35 ? run.command.slice(0, 32) + '...' : (run.command || '-');
      const created = run.created_at ? new Date(run.created_at).toISOString().slice(0, 16).replace('T', ' ') : '-';
      const exit = run.exit_code !== null && run.exit_code !== undefined ? String(run.exit_code) : '-';
      // Use workflow status if it's terminal and the run state hasn't caught up
      const wfStatus = wfStatusByRunId.get(run.id);
      let state = displayState(run.workflow_state);
      if (wfStatus === 'cancelled' && state !== 'completed') state = 'cancelled';
      if (wfStatus === 'failed' && state !== 'completed') state = 'failed';
      return [idToSlug(run.id), cmd, state, exit, created];
    });

    output(data, () => {
      printTable(['ID', 'COMMAND', 'STATE', 'EXIT', 'CREATED'], rows, [4, 37, 12, 5, 16]);
    }, 'run');
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list runs: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function runCancel(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl run cancel <workflow-id>');
    process.exit(2);
  }

  const workflowSlug = args[0];
  const workflowIntId = parseInputId(workflowSlug);
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const result = await client.cancelWorkflow(String(workflowIntId));
    if (result.cancelled) {
      console.log(`Workflow ${idToSlug(result.workflow_id)} cancelled.`);
    } else {
      console.log(`Workflow ${idToSlug(result.workflow_id)} is already ${result.status}.`);
    }
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to cancel workflow: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function runAttach(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl run attach <run-id>');
    process.exit(2);
  }

  const runSlug = args[0];
  const runIntId = parseInputId(runSlug);
  const baseUrl = getControlPlaneUrl();
  const client = new ApiClient(baseUrl);

  // Look up the run to show context
  let run: any = null;
  try {
    const result = await client.listRuns();
    run = result.data.find((r: any) => r.id === runIntId);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${baseUrl}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to look up run: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
    return;
  }

  if (!run) {
    console.error(`Run ${runSlug} not found.`);
    process.exit(2);
    return;
  }

  const state = displayState(run.workflow_state);
  if (state === 'completed' || state === 'failed' || state === 'cancelling') {
    console.log(`Run ${runSlug} already ${state} (exit ${run.exit_code ?? '-'}). No live output to stream.`);
    return;
  }

  const cmdPreview = run.command?.length > 50 ? run.command.slice(0, 47) + '...' : run.command;
  console.log(`Attaching to run ${runSlug} [${state}]: ${cmdPreview}`);
  console.log('Streaming logs... (Ctrl-C to detach)');

  process.on('SIGINT', () => {
    console.log(`\nDetached from run ${runSlug}.`);
    process.exit(0);
  });

  await client.streamLogs(
    String(runIntId),
    (stream, data) => {
      if (stream === 'stderr') {
        process.stderr.write(data);
      } else {
        process.stdout.write(data);
      }
    },
    (status, exitCode, _error) => {
      if (status === 'completed') {
        console.log(`\nRun ${runSlug} completed (exit ${exitCode ?? 0}).`);
      } else if (status === 'failed') {
        console.log(`\nRun ${runSlug} failed (exit ${exitCode ?? '-'}).`);
      } else if (status === 'timeout') {
        console.log(`\nRun ${runSlug} timed out.`);
      }
    },
  );
}

main().catch((error) => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
