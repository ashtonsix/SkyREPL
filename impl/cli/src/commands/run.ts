import { getControlPlaneUrl, isConnectionRefused } from '../config';
import { ApiClient } from '../client';
import { findProjectConfig, parseDuration } from '../project';
import type { Profile } from '../project';
import type { WorkflowStatus } from '../client';

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
  const valuedFlags = new Set(['-p', '--profile', '-c', '--command', '--timeout', '--hold', '--workdir', '--provider', '--spec']);

  let i = 0;
  while (i < args.length) {
    const arg = args[i];
    if (arg.startsWith('-')) {
      if (valuedFlags.has(arg) && i + 1 < args.length) {
        // Normalize short flags
        const key = arg === '-p' ? '--profile' : arg === '-c' ? '--command' : arg;
        flags[key] = args[i + 1];
        i += 2;
      } else if (arg === '--dry-run') {
        flags['--dry-run'] = true;
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

  // Resolve command: -c flag > profile command
  const command = (flags['--command'] as string) ?? profile.command;
  if (!command) {
    console.error('Usage: repl run [options]');
    console.error('');
    console.error('Options:');
    console.error('  -c, --command <cmd>     Command to run (overrides profile command)');
    console.error('  -p, --profile <name>    Use named profile from repl.toml (default: "default")');
    console.error('  --provider <name>       Override provider');
    console.error('  --spec <spec>           Override instance spec');
    console.error('  --workdir <path>        Override working directory');
    console.error('  --timeout <duration>    Override max duration (e.g. "1h", "30m")');
    console.error('  --hold <duration>       Override hold duration');
    console.error('  --dry-run               Show resolved config without launching');
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
  const provider = (flags['--provider'] as string) ?? profile.provider ?? DEFAULTS.provider;
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

  // Launch the run
  let workflowId: number;
  let runId: number;
  try {
    const result = await client.launchRun({
      command,
      spec,
      provider,
      workdir,
      max_duration_ms: maxDurationMs,
      hold_duration_ms: holdDurationMs,
      files: [],
      artifact_patterns: [],
      env: profile.env,
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

  console.log(`Launched run ${runId}, workflow ${workflowId}`);

  // Ctrl-C: detach from run, don't cancel
  process.on('SIGINT', () => {
    console.log(`\nRun ${runId} continuing in background. Use \`repl run cancel ${workflowId}\` to stop it.`);
    process.exit(0);
  });

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
        if (TERMINAL_STATES.has(status.status)) {
          resolvePoll(status);
          return;
        }
      } catch (err) {
        console.error(`[poll] ${err instanceof Error ? err.message : err}`);
      }
      pollTimer = setTimeout(poll, 30000);
    };
    pollTimer = setTimeout(poll, 1000);
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
  );

  [finalStatus] = await Promise.all([
    pollPromise,
    logStreamPromise.catch(() => {}),
  ]);

  // Print summary and exit
  console.log('');
  if (finalStatus.status === 'completed') {
    console.log(`Workflow ${workflowId} completed.`);
    process.exit(0);
  } else if (finalStatus.status === 'failed') {
    const errMsg = finalStatus.error?.message ?? 'unknown error';
    console.log(`Workflow ${workflowId} failed: ${errMsg}`);
    process.exit(1);
  } else if (finalStatus.status === 'cancelled') {
    console.log(`Workflow ${workflowId} was cancelled.`);
    process.exit(1);
  } else {
    console.log(`Workflow ${workflowId} ended with status: ${finalStatus.status}`);
    process.exit(1);
  }
}
