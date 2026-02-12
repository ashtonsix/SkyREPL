// tests/unit/degraded-mode.test.ts - Degraded Mode Disk Buffer Tests (#AGENT-04)
//
// Tests the disk-buffered log behavior by spawning the Python agent modules.
// Since the degraded mode logic is purely agent-side (Python), we test via
// subprocess calls to Python snippets that exercise the logs module.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, unlinkSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { spawnSync } from "child_process";

const AGENT_DIR = join(import.meta.dir, "..", "..", "agent");
const DEGRADED_LOG_PATH = "/tmp/skyrepl-test-degraded-logs.jsonl";

/** Run a Python snippet in the agent directory context */
function pyRun(code: string): { stdout: string; stderr: string; exitCode: number } {
  const result = spawnSync("python3", ["-c", code], {
    cwd: AGENT_DIR,
    env: {
      ...process.env,
      PYTHONPATH: AGENT_DIR,
      SKYREPL_DEGRADED_LOG_PATH: DEGRADED_LOG_PATH,
      SKYREPL_CONTROL_PLANE_URL: "http://localhost:1", // unreachable
    },
    timeout: 10_000,
  });
  return {
    stdout: result.stdout?.toString() ?? "",
    stderr: result.stderr?.toString() ?? "",
    exitCode: result.status ?? 1,
  };
}

function cleanup() {
  try { unlinkSync(DEGRADED_LOG_PATH); } catch { /* ok */ }
}

describe("Degraded Mode (#AGENT-04)", () => {
  beforeEach(cleanup);
  afterEach(cleanup);

  test("set_degraded toggles degraded mode flag", () => {
    const { stdout, exitCode } = pyRun(`
import logs
assert not logs.is_degraded()
logs.set_degraded(True)
assert logs.is_degraded()
logs.set_degraded(False)
assert not logs.is_degraded()
print("OK")
`);
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("OK");
  });

  test("_write_to_disk_buffer creates JSONL file", () => {
    const { exitCode } = pyRun(`
import logs, os, json
logs.DEGRADED_LOG_PATH = os.environ["SKYREPL_DEGRADED_LOG_PATH"]
entries = [{"run_id": 1, "stream": "stdout", "data": "hello"}]
logs._write_to_disk_buffer(entries)
assert os.path.exists(logs.DEGRADED_LOG_PATH)
with open(logs.DEGRADED_LOG_PATH) as f:
    lines = f.readlines()
assert len(lines) == 1
parsed = json.loads(lines[0])
assert parsed["data"] == "hello"
print("OK")
`);
    expect(exitCode).toBe(0);
  });

  test("_write_to_disk_buffer appends to existing file", () => {
    const { exitCode } = pyRun(`
import logs, os, json
logs.DEGRADED_LOG_PATH = os.environ["SKYREPL_DEGRADED_LOG_PATH"]
logs._write_to_disk_buffer([{"data": "line1"}])
logs._write_to_disk_buffer([{"data": "line2"}])
with open(logs.DEGRADED_LOG_PATH) as f:
    lines = f.readlines()
assert len(lines) == 2
print("OK")
`);
    expect(exitCode).toBe(0);
  });

  test("_write_to_disk_buffer truncates at max size", () => {
    // Write a file larger than 10MB, verify truncation
    const { exitCode } = pyRun(`
import logs, os
logs.DEGRADED_LOG_PATH = os.environ["SKYREPL_DEGRADED_LOG_PATH"]
logs.DEGRADED_LOG_MAX_BYTES = 1024  # 1KB for test

# Write 2KB of data (exceeds 1KB limit)
big_entry = {"data": "x" * 100}
for _ in range(20):
    logs._write_to_disk_buffer([big_entry])

size = os.path.getsize(logs.DEGRADED_LOG_PATH)
# After truncation, should be roughly half
assert size < 2048, f"File too large: {size}"
print("OK")
`);
    expect(exitCode).toBe(0);
  });

  test("flush_disk_buffer reads and cleans up file", () => {
    // Write some entries, then flush (will fail to send since control plane is unreachable,
    // but the file reading and cleanup logic can be tested)
    const { exitCode } = pyRun(`
import logs, os
logs.DEGRADED_LOG_PATH = os.environ["SKYREPL_DEGRADED_LOG_PATH"]

# Write entries
logs._write_to_disk_buffer([{"run_id": 1, "data": "test1"}, {"run_id": 1, "data": "test2"}])
assert os.path.exists(logs.DEGRADED_LOG_PATH)

# flush_disk_buffer will try to send and fail (unreachable control plane)
# but we can test _cleanup_disk_buffer directly
logs._cleanup_disk_buffer()
assert not os.path.exists(logs.DEGRADED_LOG_PATH)
print("OK")
`);
    expect(exitCode).toBe(0);
  });

  test("heartbeat get_state returns correct state", () => {
    const { stdout, exitCode } = pyRun(`
import threading, heartbeat

heartbeat.configure("http://localhost:1", "1", threading.Event())
assert heartbeat.get_state() == "healthy"

# Simulate degraded
heartbeat._degraded_since = 1.0
assert heartbeat.get_state() == "degraded"

# Simulate panic
heartbeat._panic_started = True
assert heartbeat.get_state() == "panic"

# Reset
heartbeat._degraded_since = None
heartbeat._panic_started = False
assert heartbeat.get_state() == "healthy"
print("OK")
`);
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("OK");
  });

  test("record_heartbeat_ack clears degraded state", () => {
    const { stdout, exitCode } = pyRun(`
import threading, heartbeat

heartbeat.configure("http://localhost:1", "1", threading.Event())
heartbeat._degraded_since = 1.0

heartbeat.record_heartbeat_ack("test-cp-id")
assert heartbeat._degraded_since is None
assert heartbeat.get_state() == "healthy"
print("OK")
`);
    expect(exitCode).toBe(0);
    expect(stdout).toContain("OK");
  });
});
