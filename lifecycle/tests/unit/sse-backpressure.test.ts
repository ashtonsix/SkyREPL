// tests/unit/sse-backpressure.test.ts - SSE Backpressure Tests (#AGENT-03)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import { SSEManager } from "../../control/src/api/sse-protocol";
import type { StartRunMessage } from "@skyrepl/contracts";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest(); });
afterEach(() => cleanup());

/**
 * Create a mock ReadableStreamDefaultController with configurable desiredSize.
 * desiredSize > 0 = healthy, desiredSize <= 0 = backpressured.
 */
function mockController(desiredSize: number | null = 1024) {
  const enqueued: Uint8Array[] = [];
  let closed = false;
  return {
    controller: {
      get desiredSize() { return desiredSize; },
      enqueue(chunk: Uint8Array) {
        if (closed) throw new Error("Controller closed");
        enqueued.push(chunk);
      },
      close() { closed = true; },
      error(_e: any) {},
    } as unknown as ReadableStreamDefaultController<Uint8Array>,
    enqueued,
    get closed() { return closed; },
  };
}

function makeStartRunCommand(commandId: number): StartRunMessage {
  return {
    type: "start_run",
    command_id: commandId,
    run_id: 100,
    allocation_id: 200,
    command: "echo test",
    workdir: "/workspace",
    files: [],
    artifacts: [],
  };
}

describe("SSE Backpressure (#AGENT-03)", () => {
  let manager: SSEManager;

  beforeEach(() => {
    manager = new SSEManager();
  });

  test("enqueueToAgent sends data when not backpressured", () => {
    const mock = mockController(1024);
    const data = new TextEncoder().encode("test data");

    const result = manager.enqueueToAgent("inst-1", mock.controller, data, false);

    expect(result).toBe(true);
    expect(mock.enqueued.length).toBe(1);
  });

  test("enqueueToAgent drops non-commands when backpressured", () => {
    const mock = mockController(0); // desiredSize=0 → backpressured
    const data = new TextEncoder().encode("heartbeat_ack data");

    const result = manager.enqueueToAgent("inst-2", mock.controller, data, false);

    expect(result).toBe(false);
    expect(mock.enqueued.length).toBe(0);
  });

  test("enqueueToAgent sends commands even when backpressured", () => {
    const mock = mockController(-100); // heavily backpressured
    const data = new TextEncoder().encode("start_run command");

    const result = manager.enqueueToAgent("inst-3", mock.controller, data, true);

    expect(result).toBe(true);
    expect(mock.enqueued.length).toBe(1);
  });

  test("sendCommand drops heartbeat_ack under backpressure", async () => {
    // Register agent via createCommandStream, then replace with backpressured mock
    const stream = manager.createCommandStream("inst-bp", undefined, new Request("http://localhost"));
    await new Promise(resolve => setTimeout(resolve, 10));

    // Inject a backpressured mock controller
    const mock = mockController(0);
    (manager as any).agentStreams.set("inst-bp", mock.controller);

    const heartbeatAck = { type: "heartbeat_ack", control_plane_id: "test" } as any;
    const result = await manager.sendCommand("inst-bp", heartbeatAck);

    // Returns true (not an error, just silently dropped)
    expect(result).toBe(true);
    expect(mock.enqueued.length).toBe(0);
  });

  test("sendCommand delivers commands under backpressure", async () => {
    const stream = manager.createCommandStream("inst-cmd", undefined, new Request("http://localhost"));
    await new Promise(resolve => setTimeout(resolve, 10));

    // Inject a backpressured mock controller
    const mock = mockController(-50);
    (manager as any).agentStreams.set("inst-cmd", mock.controller);

    const command = makeStartRunCommand(1);
    const result = await manager.sendCommand("inst-cmd", command);

    expect(result).toBe(true);
    expect(mock.enqueued.length).toBe(1);
  });

  test("backpressure clears when desiredSize recovers", () => {
    let currentDesiredSize = 0;
    const enqueued: Uint8Array[] = [];
    const controller = {
      get desiredSize() { return currentDesiredSize; },
      enqueue(chunk: Uint8Array) { enqueued.push(chunk); },
      close() {},
      error(_e: any) {},
    } as unknown as ReadableStreamDefaultController<Uint8Array>;

    const data = new TextEncoder().encode("test");

    // Initially backpressured — drop non-command
    const r1 = manager.enqueueToAgent("inst-r", controller, data, false);
    expect(r1).toBe(false);

    // Recover
    currentDesiredSize = 1024;
    const r2 = manager.enqueueToAgent("inst-r", controller, data, false);
    expect(r2).toBe(true);
    expect(enqueued.length).toBe(1);
  });
});
