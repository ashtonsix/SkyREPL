// shell/tests/daemon-events.test.ts
//
// Unit test: verify that handleSSEEvent triggers regenerateSSHConfig when a
// workflow_completed event is received.
//
// The test mocks the SSE event arrival by calling handleSSEEvent() directly,
// and mocks regenerateSSHConfig via module aliasing with a spy.

import { describe, test, expect, mock, afterEach } from 'bun:test';

// =============================================================================
// Minimal mock for regenerateSSHConfig
// =============================================================================

// We intercept the dynamic import path used inside index.ts by mocking the
// modules. Since Bun's module system resolves at import time, we stub the
// relevant exports on the mocked modules before importing the system under
// test (SUT).

let regenCallCount = 0;
let regenLastCalledAt: number | null = null;

// Mock ApiClient constructor (needed so import('../cli/src/client') doesn't fail)
mock.module('/home/ashtonsix/SkyREPL/shell/cli/src/client', () => ({
  ApiClient: class MockApiClient {
    constructor() {}
  },
}));

// Mock regenerateSSHConfig to count calls
mock.module('/home/ashtonsix/SkyREPL/shell/cli/src/ssh', () => ({
  regenerateSSHConfig: async (_client: unknown) => {
    regenCallCount++;
    regenLastCalledAt = Date.now();
    return 0;
  },
}));

// Import the handler under test AFTER mocking
const { handleSSEEvent } = await import('../daemon/index');

// =============================================================================
// Helpers
// =============================================================================

function makeEventData(extra: Record<string, unknown> = {}): string {
  return JSON.stringify({ timestamp: Date.now(), ...extra });
}

afterEach(() => {
  regenCallCount = 0;
  regenLastCalledAt = null;
});

// =============================================================================
// Tests
// =============================================================================

describe('handleSSEEvent — workflow_completed', () => {
  test('calls regenerateSSHConfig when workflow_completed is received', async () => {
    handleSSEEvent('workflow_completed', makeEventData({ status: 'completed', output: null }));

    // triggerSSHRegen is async fire-and-forget; wait one microtask tick
    await new Promise(r => setTimeout(r, 10));

    expect(regenCallCount).toBe(1);
  });

  test('calls regenerateSSHConfig for workflow_failed as well', async () => {
    handleSSEEvent('workflow_failed', makeEventData({ error: 'node timed out' }));

    await new Promise(r => setTimeout(r, 10));

    expect(regenCallCount).toBe(1);
  });

  test('does NOT call regenerateSSHConfig for heartbeat events', async () => {
    handleSSEEvent('heartbeat', makeEventData());

    await new Promise(r => setTimeout(r, 10));

    expect(regenCallCount).toBe(0);
  });

  test('calls regenerateSSHConfig for node_completed events with spawn in node_id', async () => {
    handleSSEEvent(
      'node_completed',
      makeEventData({ node_id: 'spawn-instance', output: {} }),
    );

    await new Promise(r => setTimeout(r, 10));

    expect(regenCallCount).toBe(1);
  });

  test('does NOT call regenerateSSHConfig for node_completed without spawn/allocation in node_id', async () => {
    handleSSEEvent(
      'node_completed',
      makeEventData({ node_id: 'execute-run', output: {} }),
    );

    await new Promise(r => setTimeout(r, 10));

    expect(regenCallCount).toBe(0);
  });

  test('ignores malformed JSON data gracefully', async () => {
    // Should not throw
    expect(() => handleSSEEvent('workflow_completed', '{ not valid json')).not.toThrow();

    await new Promise(r => setTimeout(r, 10));

    // No regen because data was unparseable
    expect(regenCallCount).toBe(0);
  });
});
