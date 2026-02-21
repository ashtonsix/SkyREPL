// tests/unit/command-ack.test.ts - Command Acknowledgment Protocol Tests

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import { SSEManager } from "../../control/src/api/sse-protocol";
import type { StartRunMessage } from "@skyrepl/contracts";

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

describe("Command Acknowledgment Protocol", () => {
  let manager: SSEManager;

  beforeEach(() => {
    manager = new SSEManager();
  });

  test("tracks commands with command_id after send", async () => {
    const command: StartRunMessage = {
      type: "start_run",
      command_id: 1,
      run_id: 123,
      allocation_id: 456,
      command: "echo test",
      workdir: "/workspace",
      files: [],
      artifacts: [],
    };

    // Command should be queued (agent not connected)
    await manager.sendCommand("instance-1", command);

    // No unacked commands yet because agent isn't connected
    const retries1 = manager.retryUnackedCommands("instance-1", 0);
    expect(retries1).toBe(0);

    // Simulate agent connection with a mock controller
    const encoder = new TextEncoder();
    const mockController = {
      enqueue: () => {},
      close: () => {},
    } as unknown as ReadableStreamDefaultController<Uint8Array>;

    // Create a fake SSE stream to register the agent
    const stream = manager.createCommandStream("instance-1", undefined, new Request("http://localhost"));
    // Wait a tick for the stream to be registered
    await new Promise(resolve => setTimeout(resolve, 10));

    // Now send a command with the agent connected
    await manager.sendCommand("instance-1", command);

    // Command should be tracked
    const retries2 = manager.retryUnackedCommands("instance-1", 0, 3);
    expect(retries2).toBeGreaterThan(0);
  });

  test("processes acknowledgments from agent", async () => {
    const command: StartRunMessage = {
      type: "start_run",
      command_id: 42,
      run_id: 123,
      allocation_id: 456,
      command: "echo test",
      workdir: "/workspace",
      files: [],
      artifacts: [],
    };

    // Create a fake SSE stream
    const stream = manager.createCommandStream("instance-2", undefined, new Request("http://localhost"));
    await new Promise(resolve => setTimeout(resolve, 10));

    // Send command
    await manager.sendCommand("instance-2", command);

    // Process ack
    manager.processCommandAcks("instance-2", [42]);

    // Command should no longer be tracked (no retries needed)
    const retries = manager.retryUnackedCommands("instance-2", 0, 3);
    expect(retries).toBe(0);
  });

  test("retries unacked commands after timeout", async () => {
    const command: StartRunMessage = {
      type: "start_run",
      command_id: 99,
      run_id: 123,
      allocation_id: 456,
      command: "echo test",
      workdir: "/workspace",
      files: [],
      artifacts: [],
    };

    // Create a fake SSE stream
    const stream = manager.createCommandStream("instance-3", undefined, new Request("http://localhost"));
    await new Promise(resolve => setTimeout(resolve, 10));

    // Send command
    await manager.sendCommand("instance-3", command);

    // Immediately retry (timeout = 0)
    const retries1 = manager.retryUnackedCommands("instance-3", 0, 3);
    expect(retries1).toBe(1); // Should retry once

    // Second retry
    const retries2 = manager.retryUnackedCommands("instance-3", 0, 3);
    expect(retries2).toBe(1); // Should retry again

    // Third retry
    const retries3 = manager.retryUnackedCommands("instance-3", 0, 3);
    expect(retries3).toBe(1); // Should retry again

    // Fourth retry should be rejected (max retries = 3)
    const retries4 = manager.retryUnackedCommands("instance-3", 0, 3);
    expect(retries4).toBe(0); // No more retries, command dropped
  });

  test("moves unacked commands to pending queue when agent disconnects", async () => {
    const command: StartRunMessage = {
      type: "start_run",
      command_id: 77,
      run_id: 123,
      allocation_id: 456,
      command: "echo test",
      workdir: "/workspace",
      files: [],
      artifacts: [],
    };

    // Create a fake SSE stream
    const stream = manager.createCommandStream("instance-4", undefined, new Request("http://localhost"));
    await new Promise(resolve => setTimeout(resolve, 10));

    // Send command
    await manager.sendCommand("instance-4", command);

    // Verify it has pending commands before cleanup
    const hasPending1 = manager.hasPendingCommands("instance-4");

    // Clean up instance (simulates disconnect)
    manager.cleanupInstance("instance-4");

    // After cleanup, retry should move to pending queue
    // But since the agent is disconnected, retry will fail to send
    const retries = manager.retryUnackedCommands("instance-4", 0, 3);
    expect(retries).toBe(0);
  });

  test("does not track commands without command_id", async () => {
    const command = {
      type: "heartbeat_ack",
      control_plane_id: "test",
    };

    // Create a fake SSE stream
    const stream = manager.createCommandStream("instance-5", undefined, new Request("http://localhost"));
    await new Promise(resolve => setTimeout(resolve, 10));

    // Send command without command_id
    await manager.sendCommand("instance-5", command as any);

    // Should not be tracked for acks
    const retries = manager.retryUnackedCommands("instance-5", 0, 3);
    expect(retries).toBe(0);
  });
});
