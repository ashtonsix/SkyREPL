#!/usr/bin/env bun

import { mkdirSync } from 'fs';
import { join } from 'path';
import { runCommand } from './commands/run';
import { controlCommand } from './commands/control';
import { instanceCommand } from './commands/instance';
import { allocationCommand } from './commands/allocation';
import { getControlPlaneUrl, isConnectionRefused, loadEnvFile, REPL_DIR, printTable, displayState } from './config';
import { ApiClient } from './client';

function printUsage(exitCode = 1) {
  const out = exitCode === 0 ? console.log : console.error;
  out('Usage: repl <command> [options]');
  out('');
  out('Commands:');
  out('  run [options]          Launch a run (alias: run launch)');
  out('  run attach <run-id>    Attach to a running run\'s log stream');
  out('  run list               List runs');
  out('  run cancel <id>        Cancel a workflow');
  out('  control <subcommand>   Manage control plane (start|stop|restart|status|reset)');
  out('  instance list          List instances');
  out('  allocation list        List allocations');
  out('  help                   Show this help');
  out('');
  process.exit(exitCode);
}

async function main() {
  mkdirSync(REPL_DIR, { recursive: true });
  loadEnvFile(join(REPL_DIR, 'cli.env'));

  const args = process.argv.slice(2);

  if (args.length === 0) {
    printUsage();
  }

  const command = args[0];
  const commandArgs = args.slice(1);

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
    case 'control':
      await controlCommand(commandArgs);
      break;
    case 'instance':
      await instanceCommand(commandArgs);
      break;
    case 'allocation':
      await allocationCommand(commandArgs);
      break;
    default:
      console.error(`Unknown command: ${command}`);
      printUsage();
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
      console.log('No runs found.');
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
      return [String(run.id), cmd, state, exit, created];
    });

    printTable(['ID', 'COMMAND', 'STATE', 'EXIT', 'CREATED'], rows, [4, 37, 12, 5, 16]);
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

  const workflowId = args[0];
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const result = await client.cancelWorkflow(workflowId);
    if (result.cancelled) {
      console.log(`Workflow ${result.workflowId} cancelled.`);
    } else {
      console.log(`Workflow ${result.workflowId} is already ${result.status}.`);
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

  const runId = args[0];
  const baseUrl = getControlPlaneUrl();
  const client = new ApiClient(baseUrl);

  // Look up the run to show context
  let run: any = null;
  try {
    const result = await client.listRuns();
    run = result.data.find((r: any) => String(r.id) === runId);
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
    console.error(`Run ${runId} not found.`);
    process.exit(2);
    return;
  }

  const state = displayState(run.workflow_state);
  if (state === 'completed' || state === 'failed' || state === 'cancelling') {
    console.log(`Run ${runId} already ${state} (exit ${run.exit_code ?? '-'}). No live output to stream.`);
    return;
  }

  const cmdPreview = run.command?.length > 50 ? run.command.slice(0, 47) + '...' : run.command;
  console.log(`Attaching to run ${runId} [${state}]: ${cmdPreview}`);
  console.log('Streaming logs... (Ctrl-C to detach)');

  process.on('SIGINT', () => {
    console.log(`\nDetached from run ${runId}.`);
    process.exit(0);
  });

  await client.streamLogs(
    runId,
    (stream, data) => {
      if (stream === 'stderr') {
        process.stderr.write(data);
      } else {
        process.stdout.write(data);
      }
    },
    (status, exitCode, _error) => {
      if (status === 'completed') {
        console.log(`\nRun ${runId} completed (exit ${exitCode ?? 0}).`);
      } else if (status === 'failed') {
        console.log(`\nRun ${runId} failed (exit ${exitCode ?? '-'}).`);
      } else if (status === 'timeout') {
        console.log(`\nRun ${runId} timed out.`);
      }
    },
  );
}

main().catch((error) => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
