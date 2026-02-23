#!/usr/bin/env python3
"""
Agent Main Entry Point

Startup: validate config -> start threads -> connect SSE
Running: SSE dispatch loop + background threads (heartbeat, logs)
Shutdown: terminate run -> flush logs -> exit

All connections agent-initiated (instances may be behind NAT/VPCs).
Complexity lives in the control plane, not the agent.
"""

from __future__ import annotations

import collections
import fcntl
import json
import os
import signal
import subprocess
import sys
import threading
import time
from typing import Optional

from sse import run_command_loop, set_shutting_down as sse_set_shutting_down, configure as sse_configure, get_reconnect_count
from heartbeat import (
    heartbeat_thread,
    set_shutting_down as hb_set_shutting_down,
    set_executor as hb_set_executor,
    configure as hb_configure,
    get_last_ack_time,
)
from logs import (
    log_flusher_thread,
    flush_all_logs,
    set_shutting_down as logs_set_shutting_down,
    configure as logs_configure,
    get_log_buffer_size,
)
from executor import RunExecutor, configure as executor_configure
from http_client import http_post
from tailscale import detect_tailscale_ip as tailscale_detect, tailscale_shutdown
from spot import SpotMonitor

# =============================================================================
# Env File (lowest priority — won't overwrite existing vars)
# =============================================================================

def _load_env_file(path: str) -> None:
    """Load KEY=VALUE env file, skipping blanks and comments."""
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip()
                if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0]:
                    value = value[1:-1]
                if key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        pass

_load_env_file(os.path.expanduser("~/.repl/agent.env"))

# =============================================================================
# Configuration (from environment)
# =============================================================================

CONTROL_PLANE_URL = os.getenv("SKYREPL_CONTROL_PLANE_URL", "")
INSTANCE_ID = os.getenv("SKYREPL_INSTANCE_ID")  # Required
WORKDIR = os.getenv("SKYREPL_WORKDIR", "/workspace")
SHUTDOWN_GRACE_PERIOD_S = int(os.getenv("SKYREPL_SHUTDOWN_GRACE_PERIOD_S", "5"))

# Registration token for auth (read from env or config file)
REGISTRATION_TOKEN = os.getenv("SKYREPL_REGISTRATION_TOKEN", "")

# Fallback: read missing config from /etc/skyrepl/config.json (written by cloud-init)
if not CONTROL_PLANE_URL or not INSTANCE_ID or not REGISTRATION_TOKEN:
    try:
        with open("/etc/skyrepl/config.json") as f:
            _config = json.load(f)
            if not CONTROL_PLANE_URL:
                CONTROL_PLANE_URL = _config.get("controlPlaneUrl", "")
            if not INSTANCE_ID:
                INSTANCE_ID = _config.get("SKYREPL_INSTANCE_ID", "")
            if not REGISTRATION_TOKEN:
                REGISTRATION_TOKEN = _config.get("registrationToken", "")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

if not CONTROL_PLANE_URL:
    CONTROL_PLANE_URL = "http://localhost:3000"

# =============================================================================
# Global State
# =============================================================================

_shutting_down: bool = False
_agent_start_time: float = 0.0
_executor: Optional[RunExecutor] = None
_shutdown_event = threading.Event()
_error_log_buffer: collections.deque = collections.deque(maxlen=200)


# =============================================================================
# Signal Handling
# =============================================================================


def _setup_signal_handlers() -> None:
    """Register SIGTERM and SIGINT handlers for graceful shutdown."""

    def signal_handler(signum: int, frame: object) -> None:
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        _log("INFO", f"Received {sig_name}, initiating shutdown")
        _initiate_shutdown(reason=sig_name.lower())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


# =============================================================================
# Panic Diagnostics
# =============================================================================


def _get_recent_error_logs(max_lines: int = 50) -> list:
    """Return recent error/warning log entries for panic diagnostics."""
    with threading.Lock():
        recent_logs = list(_error_log_buffer)
        return recent_logs[-max_lines:]


def _get_system_diagnostics() -> dict:
    """
    Collect system diagnostics (best-effort, Linux-only).
    Returns dict with cpu_percent, memory_percent, disk_percent.
    All percentages are returned as floats.
    """
    diagnostics = {
        "cpu_percent": 0.0,
        "memory_percent": 0.0,
        "disk_percent": 0.0,
        "active_threads": threading.active_count(),
        "log_buffer_size": 0,
    }

    try:
        diagnostics["log_buffer_size"] = get_log_buffer_size()
    except Exception:
        pass

    # Memory percent from /proc/meminfo
    try:
        with open("/proc/meminfo") as f:
            lines = f.readlines()
            mem_total = 0
            mem_available = 0
            for line in lines:
                if line.startswith("MemTotal:"):
                    mem_total = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    mem_available = int(line.split()[1])
            if mem_total > 0:
                diagnostics["memory_percent"] = float(((mem_total - mem_available) / mem_total) * 100)
    except Exception:
        pass

    # Disk percent from os.statvfs
    try:
        stat = os.statvfs("/")
        total = stat.f_blocks * stat.f_frsize
        free = stat.f_bfree * stat.f_frsize
        if total > 0:
            diagnostics["disk_percent"] = float(((total - free) / total) * 100)
    except Exception:
        pass

    return diagnostics


def _send_panic_diagnostics(reason: str) -> None:
    """
    Send panic diagnostics to control plane (best-effort, 2s timeout).
    Called at shutdown start to capture agent state before termination.

    MUST NOT block shutdown or raise exceptions.
    """
    if not INSTANCE_ID:
        return

    try:
        # Determine workflow state
        workflow_state = "idle"
        run_id = None
        allocation_id = None
        if _executor:
            if hasattr(_executor, "current_run_id") and _executor.current_run_id:
                workflow_state = "running"
                run_id = _executor.current_run_id
            if hasattr(_executor, "current_allocation_id"):
                allocation_id = _executor.current_allocation_id

        # Collect diagnostics
        diagnostics = _get_system_diagnostics()

        # Build payload
        payload = {
            "instance_id": int(INSTANCE_ID),
            "reason": reason,
            "last_state": {
                "workflow_state": workflow_state,
                "run_id": run_id,
                "allocation_id": allocation_id,
                "uptime_seconds": float(time.time() - _agent_start_time),
                "last_heartbeat_ack": float(get_last_ack_time()),
                "sse_reconnect_count": get_reconnect_count(),
            },
            "diagnostics": diagnostics,
            "error_logs": _get_recent_error_logs(max_lines=50),
            "timestamp": int(time.time() * 1000),
        }

        # POST with 2s timeout (MUST NOT block shutdown)
        resp = http_post(f"/v1/instances/{INSTANCE_ID}/panic", payload, timeout=2)
        resp.read()
        _log("INFO", f"Panic diagnostics sent: {reason}")

    except Exception as e:
        # Best-effort: log but never raise
        _log("WARN", f"Failed to send panic diagnostics (non-blocking): {e}")


# =============================================================================
# Shutdown Sequence
# =============================================================================


def _initiate_shutdown(reason: str = "unknown") -> None:
    """
    Ordered shutdown sequence:
    1. Set shutting_down flag (stops all threads)
    2. Signal all modules to stop
    3. Send panic diagnostics (2s timeout, best-effort)
    4. Terminate running run (if any)
    5. Flush remaining logs
    6. Wait grace period
    7. Exit
    """
    global _shutting_down

    if _shutting_down:
        return
    _shutting_down = True

    _log("INFO", f"Shutdown initiated: {reason}")

    # Signal all modules to stop
    sse_set_shutting_down(True)
    hb_set_shutting_down(True)
    logs_set_shutting_down(True)
    _shutdown_event.set()

    # Step 1: Send panic diagnostics (2s timeout, best-effort)
    _send_panic_diagnostics(reason)

    # Step 2: Terminate running run
    if _executor:
        try:
            _executor.terminate_current_run()
        except Exception as e:
            _log("WARN", f"Run termination failed: {e}")

    # Step 2b: Tailscale shutdown (logout + kill daemon + cleanup)
    try:
        tailscale_shutdown()
    except Exception as e:
        _log("WARN", f"Tailscale shutdown failed (best-effort): {e}")

    # Step 3: Flush remaining logs
    try:
        flush_all_logs()
    except Exception as e:
        _log("WARN", f"Log flush failed: {e}")

    # Step 4: Grace period
    _log("INFO", f"Waiting {SHUTDOWN_GRACE_PERIOD_S}s grace period")
    time.sleep(SHUTDOWN_GRACE_PERIOD_S)

    _log("INFO", "Shutdown complete")

    # Step 5: Halt the VM (production only) so billing stops
    if os.environ.get("SKYREPL_PRODUCTION"):
        subprocess.run(["shutdown", "-h", "now"])


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> int:
    """
    Agent startup and main loop.

    Startup sequence:
    1. Validate required env vars
    2. Configure all modules
    3. Start background threads (log flusher, heartbeat)
    4. Connect SSE command stream and enter dispatch loop

    Returns:
        Exit code (0 = success, 1 = error)
    """
    global _agent_start_time, _executor

    _agent_start_time = time.time()

    # Validate required configuration
    if not INSTANCE_ID:
        _log("FATAL", "SKYREPL_INSTANCE_ID not set")
        return 1

    _log("INFO", f"Agent starting (instance={INSTANCE_ID}, url={CONTROL_PLANE_URL})")

    # Acquire lock file to prevent duplicate agents
    lock_path = "/tmp/repl-agent.lock"
    try:
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
    except (IOError, OSError):
        _log("FATAL", "Another agent is already running (lock file held)")
        return 1

    # Setup signal handlers
    _setup_signal_handlers()

    # Configure all modules with control plane URL and auth token
    logs_configure(CONTROL_PLANE_URL, REGISTRATION_TOKEN)
    sse_configure(CONTROL_PLANE_URL, INSTANCE_ID, REGISTRATION_TOKEN)
    hb_configure(CONTROL_PLANE_URL, INSTANCE_ID, _shutdown_event, REGISTRATION_TOKEN)
    executor_configure(CONTROL_PLANE_URL, WORKDIR, REGISTRATION_TOKEN)

    # Detect existing Tailscale installation (e.g., from snapshot)
    existing_ts_ip = tailscale_detect()
    if existing_ts_ip:
        _log("INFO", f"Tailscale already available at startup: {existing_ts_ip}")

    # Create executor
    _executor = RunExecutor()
    hb_set_executor(_executor)

    # Start spot interruption monitor (daemon thread, no-op on non-spot instances)
    _spot_monitor = SpotMonitor(
        instance_id=int(INSTANCE_ID),
        control_url=CONTROL_PLANE_URL,
        executor=_executor,
        shutdown_event=_shutdown_event,
    )
    _spot_monitor.start()

    # Start background threads (all daemon=True)
    log_thread = threading.Thread(target=log_flusher_thread, daemon=True, name="log-flusher")
    log_thread.start()

    hb_thread = threading.Thread(target=heartbeat_thread, daemon=True, name="heartbeat")
    hb_thread.start()

    _log("INFO", "Background threads started (log-flusher, heartbeat)")

    # Enter SSE command loop (blocks until exit)
    exit_reason = run_command_loop(_executor)
    _log("INFO", f"Command loop exited: {exit_reason}")

    # Initiate shutdown based on exit reason
    if not _shutting_down:
        _initiate_shutdown(reason=exit_reason)

    return 0


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{level}] [agent] {message}"
    print(log_line)

    # Append ERROR/WARN to ring buffer for panic diagnostics
    if level in ("ERROR", "WARN"):
        _error_log_buffer.append(log_line)


if __name__ == "__main__":
    sys.exit(main())
