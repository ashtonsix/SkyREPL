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

import json
import os
import signal
import sys
import threading
import time
from typing import Optional

from sse import run_command_loop, set_shutting_down as sse_set_shutting_down, configure as sse_configure
from heartbeat import (
    heartbeat_thread,
    set_shutting_down as hb_set_shutting_down,
    set_executor as hb_set_executor,
    configure as hb_configure,
)
from logs import (
    log_flusher_thread,
    flush_all_logs,
    set_shutting_down as logs_set_shutting_down,
    configure as logs_configure,
)
from executor import RunExecutor, configure as executor_configure

# =============================================================================
# Configuration (from environment)
# =============================================================================

CONTROL_PLANE_URL = os.getenv("SKYREPL_CONTROL_PLANE_URL", "http://localhost:3000")
INSTANCE_ID = os.getenv("SKYREPL_INSTANCE_ID")  # Required
WORKDIR = os.getenv("SKYREPL_WORKDIR", "/workspace")
SHUTDOWN_GRACE_PERIOD_S = int(os.getenv("SKYREPL_SHUTDOWN_GRACE_PERIOD_S", "5"))

# Registration token for auth (read from env or config file)
REGISTRATION_TOKEN = os.getenv("SKYREPL_REGISTRATION_TOKEN", "")
if not REGISTRATION_TOKEN:
    # Fallback: read from config.json
    try:
        with open("/etc/skyrepl/config.json") as f:
            config = json.load(f)
            REGISTRATION_TOKEN = config.get("registrationToken", "")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

# =============================================================================
# Global State
# =============================================================================

_shutting_down: bool = False
_agent_start_time: float = 0.0
_executor: Optional[RunExecutor] = None
_shutdown_event = threading.Event()


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
# Shutdown Sequence
# =============================================================================


def _initiate_shutdown(reason: str = "unknown") -> None:
    """
    Ordered shutdown sequence:
    1. Set shutting_down flag (stops all threads)
    2. Terminate running run (if any)
    3. Flush remaining logs
    4. Wait grace period
    5. Exit
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

    # Step 1: Terminate running run
    if _executor:
        try:
            _executor.terminate_current_run()
        except Exception as e:
            _log("WARN", f"Run termination failed: {e}")

    # Step 2: Flush remaining logs
    try:
        flush_all_logs()
    except Exception as e:
        _log("WARN", f"Log flush failed: {e}")

    # Step 3: Grace period
    _log("INFO", f"Waiting {SHUTDOWN_GRACE_PERIOD_S}s grace period")
    time.sleep(SHUTDOWN_GRACE_PERIOD_S)

    _log("INFO", "Shutdown complete")


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

    # Setup signal handlers
    _setup_signal_handlers()

    # Configure all modules with control plane URL and auth token
    logs_configure(CONTROL_PLANE_URL, REGISTRATION_TOKEN)
    sse_configure(CONTROL_PLANE_URL, INSTANCE_ID, REGISTRATION_TOKEN)
    hb_configure(CONTROL_PLANE_URL, INSTANCE_ID, _shutdown_event, REGISTRATION_TOKEN)
    executor_configure(CONTROL_PLANE_URL, WORKDIR, REGISTRATION_TOKEN)

    # Create executor
    _executor = RunExecutor()
    hb_set_executor(_executor)

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
    print(f"[{timestamp}] [{level}] [agent] {message}")


if __name__ == "__main__":
    sys.exit(main())
