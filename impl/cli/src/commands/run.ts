import { getControlPlaneUrl } from '../config';
import { ApiClient } from '../client';
import type { WorkflowStatus } from '../client';

// Terminal workflow states — when the workflow reaches one of these, we're done
const TERMINAL_STATES = new Set(['completed', 'failed', 'cancelled']);

/**
 * Run command handler for Slice 1.
 *
 * Flow:
 * 1. POST /v1/workflows/launch-run with hardcoded defaults
 * 2. Connect WebSocket for log streaming
 * 3. Poll workflow status every 2s to detect completion
 * 4. Print summary and exit with appropriate code
 *
 * @param args Command line arguments after "run" (joined as the command string)
 */
export async function runCommand(args: string[]): Promise<void> {
  const command = args.join(' ');
  if (!command) {
    console.error('Usage: repl run <command>');
    console.error('');
    console.error('Example: repl run "echo hello"');
    process.exit(2);
  }

  const baseUrl = getControlPlaneUrl();
  const client = new ApiClient(baseUrl);

  // Step 1: Launch the run
  let workflowId: number;
  let runId: number;
  try {
    const result = await client.launchRun({
      command,
      spec: 'ubuntu',
      provider: 'orbstack',
      workdir: '/workspace',
      max_duration_ms: 3_600_000,   // 1 hour
      hold_duration_ms: 300_000,    // 5 minutes
      create_snapshot: false,
      files: [],
      artifact_patterns: [],
    });
    workflowId = result.workflow_id;
    runId = result.run_id;
  } catch (err) {
    console.error(`Failed to launch run: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }

  console.error(`Launched run ${runId}, workflow ${workflowId}`);

  // Step 2: Connect WebSocket for live log streaming
  // Run log streaming and status polling concurrently
  let finalStatus: WorkflowStatus | null = null;

  const logStreamPromise = client.streamLogs(
    String(runId),
    (stream, data) => {
      // Write log lines to the appropriate output stream
      if (stream === 'stderr') {
        process.stderr.write(data);
      } else {
        process.stdout.write(data);
      }
    },
    (_status, _exitCode, _error) => {
      // Status updates from the WebSocket are informational;
      // we rely on the polling loop for authoritative completion detection
    },
  );

  // Step 3: Poll workflow status every 2s to detect completion
  const pollPromise = new Promise<WorkflowStatus>((resolve, reject) => {
    const poll = async () => {
      try {
        const status = await client.getWorkflowStatus(String(workflowId));
        if (TERMINAL_STATES.has(status.status)) {
          resolve(status);
          return;
        }
      } catch (err) {
        // Transient poll failures are not fatal; keep trying
        console.error(`[poll] ${err instanceof Error ? err.message : err}`);
      }
      setTimeout(poll, 2000);
    };
    // Start first poll after a brief delay so the workflow has time to initialize
    setTimeout(poll, 1000);
  });

  // Wait for both: whichever resolves first between poll detecting terminal
  // state and the WebSocket closing. We need the poll result regardless.
  [finalStatus] = await Promise.all([
    pollPromise,
    // Log stream may close before or after poll completes; don't block on it
    logStreamPromise.catch(() => {}),
  ]);

  // Step 4: Print summary and exit
  console.error('');
  if (finalStatus.status === 'completed') {
    console.error(`Workflow ${workflowId} completed.`);
    process.exit(0);
  } else if (finalStatus.status === 'failed') {
    const errMsg = finalStatus.error?.message ?? 'unknown error';
    console.error(`Workflow ${workflowId} failed: ${errMsg}`);
    process.exit(1);
  } else if (finalStatus.status === 'cancelled') {
    console.error(`Workflow ${workflowId} was cancelled.`);
    process.exit(1);
  } else {
    console.error(`Workflow ${workflowId} ended with status: ${finalStatus.status}`);
    process.exit(1);
  }
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
