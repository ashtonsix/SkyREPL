import { getControlPlaneUrl, isConnectionRefused } from '../config';
import { ApiClient } from '../client';
import type { FileManifestEntry } from '../client';
import { findProjectConfig, parseDuration } from '../project';
import type { Profile } from '../project';
import type { WorkflowStatus, WorkflowStreamEvent } from '../client';
import { idToSlug } from '@skyrepl/contracts';
import { runPreflight, displayWarnings } from '../preflight';
import { createHash } from 'crypto';
import { readFileSync, statSync } from 'fs';
import { resolve, dirname } from 'path';
import { getConsumer } from '../consumer';
import { formatAgentRunOutput } from '../output/agent';
import { formatProgramRunOutput } from '../output/program';
import { createRunDir, initRunStatus } from '../../../daemon/run-artifacts';

// Terminal workflow states — when the workflow reaches one of these, we're done
const TERMINAL_STATES = new Set(['completed', 'failed', 'cancelled']);

// Hardcoded defaults (used when no profile and no flags)
const DEFAULTS: Required<Pick<Profile, 'provider' | 'spec' | 'workdir' | 'max_duration' | 'hold_duration'>> = {
  provider: 'orbstack',
  spec: 'ubuntu',
  workdir: '/workspace',
  max_duration: '1h',
  hold_duration: '5m',
};

/**
 * Parse flags from argv-style array.
 * All args must be flags; unexpected positional args cause exit.
 */
function parseRunArgs(args: string[]): { flags: Record<string, string | true> } {
  const flags: Record<string, string | true> = {};

  // Flags that take a value
  const valuedFlags = new Set(['-p', '--profile', '-c', '--command', '--timeout', '--hold', '--workdir', '--provider', '--spec', '--region']);
  // Boolean flags
  const boolFlags = new Set(['--dry-run', '--no-preflight']);

  let i = 0;
  while (i < args.length) {
    const arg = args[i];
    if (arg.startsWith('-')) {
      if (valuedFlags.has(arg) && i + 1 < args.length) {
        // Normalize short flags
        const key = arg === '-p' ? '--profile' : arg === '-c' ? '--command' : arg;
        flags[key] = args[i + 1];
        i += 2;
      } else if (boolFlags.has(arg)) {
        flags[arg] = true;
        i++;
      } else {
        console.error(`Unknown flag: ${arg}`);
        process.exit(2);
      }
    } else {
      // No positional args — command must come via -c/--command or profile
      console.error(`Unexpected argument: ${arg}`);
      process.exit(2);
    }
  }

  return { flags };
}

/**
 * Run command handler.
 *
 * Resolution order for each setting:
 *   CLI flag > profile value > hardcoded default
 *
 * If no command is given and the profile has a command, use that.
 * If no command at all, print usage and exit.
 */
export async function runCommand(args: string[]): Promise<void> {
  const { flags } = parseRunArgs(args);

  // Discover project config
  const project = findProjectConfig();
  const profileName = (flags['--profile'] as string) ?? 'default';

  let profile: Profile = {};
  if (project) {
    if (project.profiles[profileName]) {
      profile = project.profiles[profileName];
      console.log(`Using profile "${profileName}" from ${project.path}`);
    } else if (flags['--profile']) {
      // Explicit -p with a name that doesn't exist — error
      console.error(`Profile "${profileName}" not found in ${project.path}`);
      console.error(`Available profiles: ${Object.keys(project.profiles).join(', ')}`);
      process.exit(2);
    }
    // If no -p and no [default], just proceed with no profile (use defaults)
  }

  // Resolve command: -c flag > profile command (init prepended if present)
  let command = (flags['--command'] as string) ?? profile.command;
  if (!command) {
    console.error('Usage: repl run [options]');
    console.error('');
    console.error('Options:');
    console.error('  -c, --command <cmd>     Command to run (overrides profile command)');
    console.error('  -p, --profile <name>    Use named profile from repl.toml (default: "default")');
    console.error('  --provider <name>       Override provider');
    console.error('  --region <region>       Override region');
    console.error('  --spec <spec>           Override instance spec');
    console.error('  --workdir <path>        Override working directory');
    console.error('  --timeout <duration>    Override max duration (e.g. "1h", "30m")');
    console.error('  --hold <duration>       Override hold duration');
    console.error('  --dry-run               Show resolved config without launching');
    console.error('  --no-preflight          Skip preflight checks (for CI/scripting)');
    console.error('');
    console.error('Examples:');
    console.error('  repl run                              # uses [default] profile command');
    console.error('  repl run -p gpu-spot                   # uses gpu-spot profile');
    console.error('  repl run -c "echo hello"               # inline command');
    console.error('  repl run -p quick -c "echo override"   # profile settings + inline command');
    if (project) {
      console.error('');
      console.error(`Profiles in ${project.path}: ${Object.keys(project.profiles).join(', ')}`);
    }
    process.exit(2);
  }

  // Merge: flag > profile > default
  // Only apply DEFAULTS.provider if the user explicitly passed --provider or the profile sets it.
  // When neither is present, pass undefined so the API/orbital can resolve the best provider.
  const provider = (flags['--provider'] as string) ?? profile.provider ?? undefined;
  const region = (flags['--region'] as string) ?? profile.region;
  const spec = (flags['--spec'] as string) ?? profile.spec ?? DEFAULTS.spec;
  const workdir = (flags['--workdir'] as string) ?? profile.workdir ?? DEFAULTS.workdir;
  const maxDurationMs = parseDuration((flags['--timeout'] as string) ?? profile.max_duration ?? DEFAULTS.max_duration);
  const holdDurationMs = parseDuration((flags['--hold'] as string) ?? profile.hold_duration ?? DEFAULTS.hold_duration);

  // Dry-run: print resolved config and exit
  if (flags['--dry-run']) {
    console.log(JSON.stringify({
      profile: profileName,
      command,
      provider,
      ...(region ? { region } : {}),
      spec,
      workdir,
      max_duration_ms: maxDurationMs,
      hold_duration_ms: holdDurationMs,
      env: profile.env ?? {},
    }, null, 2));
    process.exit(0);
  }

  const baseUrl = getControlPlaneUrl();
  const client = new ApiClient(baseUrl);

  // Preflight checks
  if (!flags['--no-preflight']) {
    const warnings = await runPreflight(client, { operation: 'launch-run', spec, provider, region });
    const ok = displayWarnings(warnings);
    if (!ok) {
      process.exit(2);
    }
  }

  // Prepend init script to command if present
  const initScript = profile.init;
  if (initScript) {
    command = `${initScript} && ${command}`;
  }

  // Resolve files from profile: read, hash, and upload to control plane
  const fileEntries: FileManifestEntry[] = [];
  const profileFiles = profile.files ?? [];
  if (profileFiles.length > 0 && project) {
    const projectDir = dirname(project.path);
    for (const relPath of profileFiles) {
      const absPath = resolve(projectDir, relPath);
      try {
        const data = readFileSync(absPath);
        const checksum = createHash('sha256').update(data).digest('hex');
        const size_bytes = statSync(absPath).size;
        fileEntries.push({ path: relPath, checksum, size_bytes });
      } catch (err) {
        console.error(`Failed to read file "${relPath}": ${err instanceof Error ? err.message : err}`);
        process.exit(2);
      }
    }

    // Check which blobs already exist, upload missing ones
    const checksums = fileEntries.map(f => f.checksum);
    const { missing } = await client.checkBlobs(checksums);
    const missingSet = new Set(missing);

    for (const entry of fileEntries) {
      if (missingSet.has(entry.checksum)) {
        const absPath = resolve(projectDir, entry.path);
        const data = readFileSync(absPath);
        await client.uploadBlob(entry.checksum, data);
      }
    }
  }

  const artifactPatterns = profile.artifacts ?? [];

  // Launch the run
  let workflowId: number;
  let runId: number;
  try {
    const result = await client.launchRun({
      command,
      spec,
      provider,
      ...(region ? { region } : {}),
      workdir,
      max_duration_ms: maxDurationMs,
      hold_duration_ms: holdDurationMs,
      files: fileEntries,
      artifact_patterns: artifactPatterns,
      create_snapshot: false,
      env: profile.env,
      ...(profile.disk_size_gb ? { disk_size_gb: profile.disk_size_gb } : {}),
    });
    workflowId = result.workflow_id;
    runId = result.run_id;
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${baseUrl}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to launch run: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }

  const runSlug = idToSlug(runId);
  const workflowSlug = idToSlug(workflowId);

  // ── Consumer fork ────────────────────────────────────────────────────────
  // agent and program consumers exit immediately after printing launch info.
  // Only human consumers block and display the progress spinner.
  const consumer = getConsumer();
  const runContext = {
    runId,
    workflowId,
    provider,
    spec,
    region,
    command,
    runSlug,
    workflowSlug,
  };

  if (consumer === 'agent') {
    // Eagerly create the run artifact dir so the agent has files to watch
    const dirCreated = createRunDir(runSlug);
    if (dirCreated) {
      // Initialize status.json with submission metadata immediately (E4)
      initRunStatus(runSlug, { runId, workflowId, submittedAt: Date.now() });
    }
    formatAgentRunOutput({ ...runContext, runDirAvailable: dirCreated });
    process.exit(0);
  }

  if (consumer === 'program') {
    const dirCreated = createRunDir(runSlug);
    if (dirCreated) {
      initRunStatus(runSlug, { runId, workflowId, submittedAt: Date.now() });
    }
    formatProgramRunOutput(runContext);
    process.exit(0);
  }

  // human: log and fall through to progress display below
  console.log(`Launched run ${runSlug}, workflow ${workflowSlug}`);
  // Eagerly fetch total node count for progress display
  let initialTotalNodes = 0;
  try {
    const initialStatus = await client.getWorkflowStatus(String(workflowId));
    if (initialStatus.nodes_total > 0) initialTotalNodes = initialStatus.nodes_total;
  } catch {
    // Non-fatal — poll will pick it up
  }

  // Ctrl-C: detach from run, don't cancel
  process.on('SIGINT', () => {
    process.stdout.write('\r\x1b[K'); // Clear spinner line
    console.log(`Run ${runSlug} continuing in background. Use \`repl cancel ${workflowSlug}\` to stop it.`);
    process.exit(0);
  });

  // ─── Progress display state ───────────────────────────────────────────────

  /** Map node_type and node_id → human-readable label for progress display */
  const NODE_LABELS: Record<string, string> = {
    // By node type (from SSE node_started events)
    'check-budget':        'Checking budget',
    'claim-allocation':    'Allocating instance',
    'resolve-instance':    'Resolving instance',
    'spawn-instance':      'Creating instance',
    'validate-instance':   'Validating instance',
    'wait-for-boot':       'Waiting for boot',
    'create-allocation':   'Setting up allocation',
    'start-run':           'Syncing files & starting run',
    'wait-completion':     'Running command',
    'finalize':            'Finalizing',
    // By node ID (from SSE node_completed/node_failed events which lack node_type)
    'claim-warm-allocation': 'Allocating instance',
    'sync-files':          'Syncing files & starting run',
    'await-completion':    'Running command',
    'finalize-run':        'Finalizing',
    // Cleanup workflow nodes
    'sort-and-group':      'Preparing resources',
    'drain-ssh-sessions':  'Draining SSH sessions',
    'drain-allocations':   'Draining allocations',
    'cleanup-resources':   'Cleaning up resources',
    'cleanup-features':    'Cleaning up features',
    'cleanup-records':     'Cleaning up records',
    'delete-manifest':     'Deleting manifest',
    'terminate-provider':  'Terminating instance',
    'load-manifest-resources': 'Loading resources',
  };

  const SPINNER_FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
  let spinnerFrame = 0;
  let spinnerInterval: ReturnType<typeof setInterval> | null = null;
  let currentNodeLabel = '';
  const completedNodes: string[] = [];
  const isTTY = process.stdout.isTTY;

  // Step counter state
  const seenNodeIds = new Set<string>();
  let stepCompleted = 0;
  let totalNodes = initialTotalNodes; // Updated eagerly and by poll

  /** Format a step prefix like "[2/9] " */
  const stepPrefix = () => {
    const total = totalNodes || seenNodeIds.size;
    if (total > 0) {
      return `[${stepCompleted + 1}/${total}] `;
    }
    return '';
  };

  const startSpinner = (label: string) => {
    if (!isTTY) {
      console.log(`  ${stepPrefix()}${label}...`);
      return;
    }
    currentNodeLabel = label;
    if (!spinnerInterval) {
      spinnerInterval = setInterval(() => {
        const frame = SPINNER_FRAMES[spinnerFrame % SPINNER_FRAMES.length];
        process.stdout.write(`\r\x1b[K${frame} ${stepPrefix()}${currentNodeLabel}...`);
        spinnerFrame++;
      }, 80);
    }
  };

  const completeNode = (label: string) => {
    completedNodes.push(label);
    const total = totalNodes || seenNodeIds.size;
    const prefix = total > 0 ? `[${stepCompleted}/${total}] ` : '';
    if (!isTTY) {
      console.log(`  ✓ ${prefix}${label}`);
      return;
    }
    process.stdout.write(`\r\x1b[K\x1b[32m✓\x1b[0m ${prefix}${label}\n`);
  };

  const failNode = (label: string, error: string) => {
    if (spinnerInterval) {
      clearInterval(spinnerInterval);
      spinnerInterval = null;
    }
    if (!isTTY) {
      console.error(`  ✗ ${label}: ${error}`);
      return;
    }
    process.stdout.write(`\r\x1b[K\x1b[31m✗\x1b[0m ${label}: ${error}\n`);
  };

  const stopSpinner = () => {
    if (spinnerInterval) {
      clearInterval(spinnerInterval);
      spinnerInterval = null;
    }
    if (isTTY) {
      process.stdout.write('\r\x1b[K');
    }
  };

  const onWorkflowEvent = (event: WorkflowStreamEvent) => {
    const data = event.data as Record<string, unknown>;
    const nodeType = (data.node_type ?? data.node_id ?? '') as string;
    const nodeId = (data.node_id ?? '') as string;
    const label = NODE_LABELS[nodeType] ?? NODE_LABELS[nodeId] ?? nodeType ?? nodeId;

    switch (event.event) {
      case 'node_started':
        seenNodeIds.add(nodeId);
        startSpinner(label);
        break;
      case 'node_completed':
        seenNodeIds.add(nodeId); // Ensure catch-up completions are tracked
        stepCompleted++;
        stopSpinner();
        completeNode(label);
        break;
      case 'node_failed':
        failNode(label, (data.error as string) ?? 'unknown error');
        break;
      case 'workflow_completed':
        stopSpinner();
        break;
      case 'workflow_failed':
        stopSpinner();
        break;
    }
  };

  // ─── Polling fallback ────────────────────────────────────────────────────

  // Connect WebSocket for live log streaming + poll as fallback
  let finalStatus: WorkflowStatus | null = null;

  let pollTimer: ReturnType<typeof setTimeout>;
  let resolvePoll: (status: WorkflowStatus) => void;
  const pollPromise = new Promise<WorkflowStatus>((resolve) => {
    resolvePoll = (status) => {
      clearTimeout(pollTimer);
      resolve(status);
    };
    const poll = async () => {
      try {
        const status = await client.getWorkflowStatus(String(workflowId));
        // Update total node count for progress display
        if (status.nodes_total > 0) totalNodes = status.nodes_total;
        if (TERMINAL_STATES.has(status.status)) {
          resolvePoll(status);
          return;
        }
      } catch (err) {
        console.error(`[poll] ${err instanceof Error ? err.message : err}`);
      }
      pollTimer = setTimeout(poll, 5000);
    };
    pollTimer = setTimeout(poll, 200);
  });

  // ─── SSE progress stream ─────────────────────────────────────────────────

  const ssePromise = client.streamWorkflowProgress(
    String(workflowId),
    onWorkflowEvent,
  ).then(async (terminal) => {
    stopSpinner();
    // SSE says terminal — fetch final workflow status
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        const ws = await client.getWorkflowStatus(String(workflowId));
        if (TERMINAL_STATES.has(ws.status)) {
          resolvePoll!(ws);
          return;
        }
      } catch {
        // ignore, retry
      }
      await new Promise(r => setTimeout(r, 500));
    }
  }).catch(() => {
    // SSE failed — polling fallback is already running, that's fine
  });

  const logStreamPromise = client.streamLogs(
    String(runId),
    (stream, data) => {
      if (stream === 'stderr') {
        process.stderr.write(data);
      } else {
        process.stdout.write(data);
      }
    },
    async (status, _exitCode, _error) => {
      if (status && TERMINAL_STATES.has(status)) {
        // WS says terminal — fetch workflow status, but only resolve if the
        // HTTP response also shows terminal (avoids race with finalize node).
        for (let attempt = 0; attempt < 5; attempt++) {
          try {
            const ws = await client.getWorkflowStatus(String(workflowId));
            if (TERMINAL_STATES.has(ws.status)) {
              resolvePoll!(ws);
              return;
            }
          } catch {
            // ignore, retry
          }
          await new Promise(r => setTimeout(r, 500));
        }
        // After retries, fall through to poll
      }
    },
  ).catch((err) => {
    // Log stream is best-effort; polling fallback is already running.
    // Log the error for visibility but don't let an unhandled rejection crash the process.
    console.error(`[log stream] ${err instanceof Error ? err.message : err}`);
  });

  // pollPromise is authoritative — resolves when workflow reaches terminal state.
  // logStreamPromise and ssePromise are best-effort for live display; don't block on them.
  finalStatus = await pollPromise;
  stopSpinner();

  // Fetch allocation to get instance display name (best-effort, non-fatal)
  let instanceDisplayName: string | null = null;
  try {
    const allocResult = await client.listAllocations({ run_id: runId });
    const alloc = allocResult.data?.[0];
    if (alloc?.instance_display_name) {
      instanceDisplayName = alloc.instance_display_name as string;
    }
  } catch {
    // Non-fatal — display name is informational
  }

  // Print summary and exit
  console.log('');
  if (instanceDisplayName) {
    console.log(`Instance: ${instanceDisplayName}  (ssh repl-${instanceDisplayName})`);
  }
  if (finalStatus.status === 'completed') {
    // Propagate command exit code: workflow succeeded but command may have failed
    // Output is a map of node outputs keyed by node_id
    const outputMap = finalStatus.output as Record<string, { exitCode?: number }> | null;
    const exitCode = outputMap?.['await-completion']?.exitCode ?? 0;
    if (exitCode !== 0) {
      console.log(`Command exited with code ${exitCode}.`);
    } else {
      console.log(`Workflow ${workflowSlug} completed.`);
    }
    process.exit(exitCode);
  } else if (finalStatus.status === 'failed') {
    const errMsg = finalStatus.error?.message ?? 'unknown error';
    console.log(`Workflow ${workflowSlug} failed: ${errMsg}`);
    process.exit(1);
  } else if (finalStatus.status === 'cancelled') {
    console.log(`Workflow ${workflowSlug} was cancelled.`);
    process.exit(1);
  } else {
    console.log(`Workflow ${workflowSlug} ended with status: ${finalStatus.status}`);
    process.exit(1);
  }
}
