// cli/src/consumer.ts — Consumer type detection
//
// Determines whether the CLI is being invoked by:
//   - "human"  — interactive terminal user
//   - "agent"  — AI coding assistant (Claude Code, Cursor, Aider, etc.)
//   - "program" — CI pipeline, script, or non-interactive process
//
// Detection hierarchy (first match wins):
//   1. SKYREPL_USER_TYPE env var (explicit override)
//   2. Confirmed agent env vars (CLAUDECODE, GEMINI_CLI)
//   3. Probable agent env vars (IDE-scoped, may leak)
//   4. CI/program signals
//   5. TTY checks (isTTY, TERM, TERM_PROGRAM, SSH without PTY)
//   6. Process tree (parent process name)
//   7. Soft signals (tiebreakers: NO_COLOR, geometry, SHLVL, WARP_*, etc.)
//   8. Default: human
//
// NOTE: Tier 7 from BL-67 (escape sequence probing via DSR/DA) is deferred.
// Active escape-sequence probing requires async I/O and is invasive for simple
// CLI invocations. The current 8-tier hierarchy covers the practical cases.
// See BL-67 for the full signal list including deferred probing.

import { readFileSync } from 'fs';

export type ConsumerType = 'human' | 'agent' | 'program';

/**
 * Detect the type of consumer invoking the CLI.
 * Returns the first match in the detection hierarchy.
 *
 * Accepts an optional env override for testing.
 */
export function detectConsumer(env: Record<string, string | undefined> = process.env): ConsumerType {
  // ── 1. Explicit override ─────────────────────────────────────────────────
  const explicit = env['SKYREPL_USER_TYPE'];
  if (explicit === 'human' || explicit === 'agent' || explicit === 'program') {
    return explicit;
  }

  // ── 2. Confirmed agent env vars ──────────────────────────────────────────
  if (env['CLAUDECODE'] === '1') return 'agent';
  if (env['GEMINI_CLI'] === '1') return 'agent';

  // ── 3. Probable agent env vars (IDE-scoped, may leak) ────────────────────
  if (env['CURSOR_AGENT'] || env['CURSOR_SESSION_ID']) return 'agent';
  if (env['AIDER_MODEL']) return 'agent';
  if (hasAiderVar(env)) return 'agent';
  if (env['CONTINUE_GLOBAL_DIR']) return 'agent';
  if (hasCodyVar(env)) return 'agent';
  if (env['CODEX']) return 'agent';
  if (hasWindsurfVar(env)) return 'agent';
  if (env['GITHUB_COPILOT']) return 'agent';

  // ── 4. CI/program signals ────────────────────────────────────────────────
  if (env['CI'] === 'true' || env['CI'] === '1') return 'program';
  if (env['CONTINUOUS_INTEGRATION'] === 'true') return 'program';
  if (env['GITHUB_ACTIONS']) return 'program';
  if (env['GITLAB_CI']) return 'program';
  if (env['CIRCLECI']) return 'program';
  if (env['TRAVIS']) return 'program';
  if (env['JENKINS_URL']) return 'program';
  if (env['BUILD_NUMBER']) return 'program';
  if (env['NONINTERACTIVE'] === '1') return 'program';
  if (env['DEBIAN_FRONTEND'] === 'noninteractive') return 'program';

  // ── 5. TTY checks ────────────────────────────────────────────────────────
  if (!process.stdin.isTTY || !process.stdout.isTTY) return 'program';
  if (env['TERM'] === 'dumb') return 'program';
  if (!env['TERM']) return 'program';
  // TERM_PROGRAM is absent on many Linux terminals (xterm, rxvt, konsole) —
  // only treat it as a hard signal when combined with no TTY (already caught
  // above). Reaching here means we have a real TTY, so don't gate on TERM_PROGRAM.
  // SSH without a PTY: connected but no terminal allocated
  if (env['SSH_CONNECTION'] && !env['SSH_TTY']) return 'program';

  // ── 6. Process tree (catch-all for unmarked agents) ──────────────────────
  const parentName = getParentProcessName();
  if (parentName) {
    const agentParents = ['claude', 'cursor', 'aider', 'gemini'];
    for (const name of agentParents) {
      if (parentName.toLowerCase().includes(name)) return 'agent';
    }
  }

  // ── 7. Soft signals (tiebreakers) ────────────────────────────────────────
  // These alone don't determine consumer type, but if multiple are present
  // they suggest a non-human context.
  let softSignals = 0;
  if (env['NO_COLOR'] === '1') softSignals++;
  if (!env['COLUMNS'] && !env['LINES']) softSignals++;
  if (env['INSIDE_EMACS']) softSignals++;
  if (env['VIM_TERMINAL']) softSignals++;
  // Unusually deep shell nesting suggests automated context
  const shlvl = parseInt(env['SHLVL'] ?? '0', 10);
  if (!isNaN(shlvl) && shlvl >= 5) softSignals++;
  // Warp terminal is AI-adjacent; may indicate agent context
  if (hasWarpVar(env)) softSignals++;

  if (softSignals >= 2) return 'program';

  // ── 8. Default ───────────────────────────────────────────────────────────
  return 'human';
}

/** Check if any AIDER_* env var is set. */
function hasAiderVar(env: Record<string, string | undefined>): boolean {
  return Object.keys(env).some(k => k.startsWith('AIDER_'));
}

/** Check if any CODY_* env var is set. */
function hasCodyVar(env: Record<string, string | undefined>): boolean {
  return Object.keys(env).some(k => k.startsWith('CODY_'));
}

/** Check if any WINDSURF_* or CODEIUM_* env var is set. */
function hasWindsurfVar(env: Record<string, string | undefined>): boolean {
  return Object.keys(env).some(k => k.startsWith('WINDSURF_') || k.startsWith('CODEIUM_'));
}

/** Check if any WARP_* env var is set (Warp terminal is AI-adjacent). */
function hasWarpVar(env: Record<string, string | undefined>): boolean {
  return Object.keys(env).some(k => k.startsWith('WARP_'));
}

/**
 * Read the parent process name from /proc/$PPID/cmdline or `ps`.
 * Returns null if unavailable.
 */
function getParentProcessName(): string | null {
  try {
    const ppid = process.ppid;
    if (!ppid) return null;

    // Try /proc first (Linux)
    try {
      // cmdline is NUL-separated; read synchronously via Bun
      const text = readFileSync(`/proc/${ppid}/cmdline`, 'utf-8');
      // Extract the first token (executable name)
      const parts = text.split('\0').filter(Boolean);
      if (parts.length > 0) {
        // Get the basename of the executable
        const exe = parts[0];
        return exe.split('/').pop() ?? exe;
      }
    } catch {
      // /proc not available (macOS, etc.) — fall through to ps
    }

    // Try ps as fallback (macOS and other POSIX)
    const result = Bun.spawnSync(['ps', '-o', 'comm=', '-p', String(ppid)], {
      stdout: 'pipe',
      stderr: 'ignore',
      timeout: 5000,
    });
    if (result.exitCode === 0) {
      return result.stdout.toString().trim() || null;
    }

    return null;
  } catch {
    return null;
  }
}

/** Cached consumer type — detected once per process. */
let _cachedConsumer: ConsumerType | null = null;

/**
 * Get the cached consumer type for this process.
 * Detects once and caches the result.
 */
export function getConsumer(): ConsumerType {
  if (_cachedConsumer === null) {
    _cachedConsumer = detectConsumer();
  }
  return _cachedConsumer;
}

/** Override the cached consumer type (for testing and --json flag). */
export function setConsumer(type: ConsumerType): void {
  _cachedConsumer = type;
}

/** Reset the cached consumer (for testing). */
export function resetConsumer(): void {
  _cachedConsumer = null;
}
