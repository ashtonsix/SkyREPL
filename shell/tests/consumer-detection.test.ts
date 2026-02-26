// shell/tests/consumer-detection.test.ts
//
// Unit tests for detectConsumer() — covers all branches in the detection
// hierarchy with mock env/TTY combinations.

import { describe, test, expect, beforeEach, afterEach, mock } from 'bun:test';

// We need to control process.stdin.isTTY and process.stdout.isTTY.
// Bun doesn't let us override process directly so we test detectConsumer
// with the env parameter (everything except TTY checks) and test TTY
// behaviour by mocking isTTY accessors.

// Import after mocking
import { detectConsumer, resetConsumer } from '../cli/src/consumer';

// =============================================================================
// Helpers
// =============================================================================

/** Build a minimal env that looks like a real human terminal session */
function humanEnv(overrides: Record<string, string | undefined> = {}): Record<string, string | undefined> {
  return {
    TERM: 'xterm-256color',
    TERM_PROGRAM: 'iTerm.app',
    COLUMNS: '220',
    LINES: '50',
    ...overrides,
  };
}

/** Save and restore TTY flags around a test */
function withTTY(stdin: boolean, stdout: boolean, fn: () => void): void {
  const origStdin = Object.getOwnPropertyDescriptor(process.stdin, 'isTTY');
  const origStdout = Object.getOwnPropertyDescriptor(process.stdout, 'isTTY');

  Object.defineProperty(process.stdin, 'isTTY', { value: stdin, configurable: true });
  Object.defineProperty(process.stdout, 'isTTY', { value: stdout, configurable: true });

  try {
    fn();
  } finally {
    if (origStdin) Object.defineProperty(process.stdin, 'isTTY', origStdin);
    else delete (process.stdin as any).isTTY;
    if (origStdout) Object.defineProperty(process.stdout, 'isTTY', origStdout);
    else delete (process.stdout as any).isTTY;
  }
}

afterEach(() => {
  resetConsumer();
});

// =============================================================================
// 1. Explicit override (SKYREPL_USER_TYPE)
// =============================================================================

describe('detectConsumer — explicit override', () => {
  test('SKYREPL_USER_TYPE=human returns human', () => {
    expect(detectConsumer({ SKYREPL_USER_TYPE: 'human' })).toBe('human');
  });

  test('SKYREPL_USER_TYPE=agent returns agent', () => {
    expect(detectConsumer({ SKYREPL_USER_TYPE: 'agent' })).toBe('agent');
  });

  test('SKYREPL_USER_TYPE=program returns program', () => {
    expect(detectConsumer({ SKYREPL_USER_TYPE: 'program' })).toBe('program');
  });

  test('SKYREPL_USER_TYPE=unknown falls through to default', () => {
    // With a full human-like env and TTY, should default to human
    withTTY(true, true, () => {
      expect(detectConsumer({ SKYREPL_USER_TYPE: 'unknown', ...humanEnv() })).toBe('human');
    });
  });
});

// =============================================================================
// 2. Confirmed agent env vars
// =============================================================================

describe('detectConsumer — confirmed agent env vars', () => {
  test('CLAUDECODE=1 returns agent', () => {
    expect(detectConsumer({ CLAUDECODE: '1' })).toBe('agent');
  });

  test('CLAUDECODE=0 does not trigger agent', () => {
    // Falls through to program — no TTY is present in the test environment
    const result = detectConsumer({ CLAUDECODE: '0', ...humanEnv() });
    expect(result).toBe('program');
  });

  test('GEMINI_CLI=1 returns agent', () => {
    expect(detectConsumer({ GEMINI_CLI: '1' })).toBe('agent');
  });
});

// =============================================================================
// 3. Probable agent env vars
// =============================================================================

describe('detectConsumer — probable agent env vars', () => {
  test('CURSOR_AGENT returns agent', () => {
    expect(detectConsumer({ CURSOR_AGENT: '1' })).toBe('agent');
  });

  test('CURSOR_SESSION_ID returns agent', () => {
    expect(detectConsumer({ CURSOR_SESSION_ID: 'sess-123' })).toBe('agent');
  });

  test('AIDER_MODEL returns agent', () => {
    expect(detectConsumer({ AIDER_MODEL: 'gpt-4' })).toBe('agent');
  });

  test('any AIDER_* var returns agent', () => {
    expect(detectConsumer({ AIDER_SOMETHING: 'yes' })).toBe('agent');
  });

  test('CONTINUE_GLOBAL_DIR returns agent', () => {
    expect(detectConsumer({ CONTINUE_GLOBAL_DIR: '/home/user/.continue' })).toBe('agent');
  });

  test('any CODY_* var returns agent', () => {
    expect(detectConsumer({ CODY_AUTH_TOKEN: 'abc' })).toBe('agent');
  });

  test('CODEX returns agent', () => {
    expect(detectConsumer({ CODEX: '1' })).toBe('agent');
  });

  test('any WINDSURF_* var returns agent', () => {
    expect(detectConsumer({ WINDSURF_SESSION: 'ws-1' })).toBe('agent');
  });

  test('any CODEIUM_* var returns agent', () => {
    expect(detectConsumer({ CODEIUM_API_KEY: 'key' })).toBe('agent');
  });

  test('GITHUB_COPILOT returns agent', () => {
    expect(detectConsumer({ GITHUB_COPILOT: '1' })).toBe('agent');
  });
});

// =============================================================================
// 4. CI/program signals
// =============================================================================

describe('detectConsumer — CI/program signals', () => {
  test('CI=true returns program', () => {
    expect(detectConsumer({ CI: 'true' })).toBe('program');
  });

  test('CI=1 returns program', () => {
    expect(detectConsumer({ CI: '1' })).toBe('program');
  });

  test('CONTINUOUS_INTEGRATION=true returns program', () => {
    expect(detectConsumer({ CONTINUOUS_INTEGRATION: 'true' })).toBe('program');
  });

  test('GITHUB_ACTIONS returns program', () => {
    expect(detectConsumer({ GITHUB_ACTIONS: 'true' })).toBe('program');
  });

  test('GITLAB_CI returns program', () => {
    expect(detectConsumer({ GITLAB_CI: 'true' })).toBe('program');
  });

  test('CIRCLECI returns program', () => {
    expect(detectConsumer({ CIRCLECI: 'true' })).toBe('program');
  });

  test('TRAVIS returns program', () => {
    expect(detectConsumer({ TRAVIS: 'true' })).toBe('program');
  });

  test('JENKINS_URL returns program', () => {
    expect(detectConsumer({ JENKINS_URL: 'http://jenkins/' })).toBe('program');
  });

  test('BUILD_NUMBER returns program', () => {
    expect(detectConsumer({ BUILD_NUMBER: '42' })).toBe('program');
  });

  test('NONINTERACTIVE=1 returns program', () => {
    expect(detectConsumer({ NONINTERACTIVE: '1' })).toBe('program');
  });

  test('DEBIAN_FRONTEND=noninteractive returns program', () => {
    expect(detectConsumer({ DEBIAN_FRONTEND: 'noninteractive' })).toBe('program');
  });
});

// =============================================================================
// 5. TTY checks
// =============================================================================

describe('detectConsumer — TTY checks', () => {
  test('stdin not a TTY returns program', () => {
    withTTY(false, true, () => {
      expect(detectConsumer(humanEnv())).toBe('program');
    });
  });

  test('stdout not a TTY returns program', () => {
    withTTY(true, false, () => {
      expect(detectConsumer(humanEnv())).toBe('program');
    });
  });

  test('TERM=dumb returns program (when TTY is true)', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({ ...humanEnv(), TERM: 'dumb' })).toBe('program');
    });
  });

  test('TERM unset returns program (when TTY is true)', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({ TERM_PROGRAM: 'iTerm.app' })).toBe('program');
    });
  });

  test('TERM_PROGRAM unset + real TTY still falls through to human (Linux terminals lack TERM_PROGRAM)', () => {
    withTTY(true, true, () => {
      // Many Linux terminals (xterm, rxvt, konsole) do not set TERM_PROGRAM.
      // With a real TTY present, absence of TERM_PROGRAM is not sufficient to
      // classify as program — the TTY check already handled non-TTY cases above.
      expect(detectConsumer({ TERM: 'xterm-256color' })).toBe('human');
    });
  });

  test('TERM_PROGRAM unset + no TTY returns program (pipe/non-interactive)', () => {
    withTTY(false, false, () => {
      expect(detectConsumer({ TERM: 'xterm-256color' })).toBe('program');
    });
  });

  test('SSH_CONNECTION without SSH_TTY returns program (SSH without PTY)', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({ ...humanEnv(), SSH_CONNECTION: '1.2.3.4 12345 5.6.7.8 22' })).toBe('program');
    });
  });

  test('SSH_CONNECTION with SSH_TTY does not trigger program (PTY allocated)', () => {
    withTTY(true, true, () => {
      // Has both SSH_CONNECTION and SSH_TTY → falls through to human
      expect(detectConsumer({ ...humanEnv(), SSH_CONNECTION: '1.2.3.4 12345 5.6.7.8 22', SSH_TTY: '/dev/pts/0' })).toBe('human');
    });
  });

  test('full human TTY env returns human', () => {
    withTTY(true, true, () => {
      expect(detectConsumer(humanEnv())).toBe('human');
    });
  });
});

// =============================================================================
// 6. Agent signals take precedence over CI
// =============================================================================

describe('detectConsumer — precedence ordering', () => {
  test('CLAUDECODE=1 beats CI=true (agent wins)', () => {
    expect(detectConsumer({ CLAUDECODE: '1', CI: 'true' })).toBe('agent');
  });

  test('SKYREPL_USER_TYPE=human beats CLAUDECODE=1 (explicit wins)', () => {
    expect(detectConsumer({ SKYREPL_USER_TYPE: 'human', CLAUDECODE: '1' })).toBe('human');
  });

  test('SKYREPL_USER_TYPE=program beats CLAUDECODE=1 (explicit wins)', () => {
    expect(detectConsumer({ SKYREPL_USER_TYPE: 'program', CLAUDECODE: '1' })).toBe('program');
  });
});

// =============================================================================
// 7. Soft signals
// =============================================================================

describe('detectConsumer — soft signals', () => {
  test('2+ soft signals return program when TTY is true', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({
        ...humanEnv(),
        NO_COLOR: '1',
        INSIDE_EMACS: '1',
      })).toBe('program');
    });
  });

  test('1 soft signal alone does not change the result to program', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({ ...humanEnv(), NO_COLOR: '1' })).toBe('human');
    });
  });

  test('SHLVL >= 5 counts as a soft signal', () => {
    withTTY(true, true, () => {
      // One soft signal alone: SHLVL=5 + one other needed
      expect(detectConsumer({ ...humanEnv(), SHLVL: '5', NO_COLOR: '1' })).toBe('program');
    });
  });

  test('SHLVL < 5 does not count as a soft signal', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({ ...humanEnv(), SHLVL: '2', NO_COLOR: '1' })).toBe('human');
    });
  });

  test('any WARP_* var counts as a soft signal', () => {
    withTTY(true, true, () => {
      expect(detectConsumer({ ...humanEnv(), WARP_SESSION_ID: 'abc', NO_COLOR: '1' })).toBe('program');
    });
  });
});

// =============================================================================
// 8. Default
// =============================================================================

describe('detectConsumer — default', () => {
  test('minimal human env + TTY defaults to human', () => {
    withTTY(true, true, () => {
      expect(detectConsumer(humanEnv())).toBe('human');
    });
  });
});
