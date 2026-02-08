#!/usr/bin/env python3
"""
Agent Main Entry Point

Startup: lock -> detect tailscale -> start threads -> connect SSE
Running: SSE dispatch loop + background threads (heartbeat, logs, spot)
Shutdown: panic diagnostics -> tailscale logout -> terminate run -> flush logs -> grace -> self-term

All connections agent-initiated (instances may be behind NAT/VPCs).
Complexity lives in the control plane, not the agent.
"""

from __future__ import annotations

import os
import sys
import time
import threading
from typing import Optional

# Note: These imports will fail until the modules are created
# from sse import SSEReader
# from executor import Executor
# from heartbeat import HeartbeatSender
# from logs import LogFlusher


class Agent:
    """
    Main agent orchestrator.

    Responsibilities:
    - Lock acquisition (prevent duplicate agents on warm pool)
    - Environment validation
    - Thread lifecycle management
    - SSE connection and dispatch
    - Graceful shutdown coordination
    """

    def __init__(self):
        self.instance_id: Optional[str] = None
        self.control_plane_url: str = ""
        self.provider: str = ""
        self.shutting_down: bool = False
        self.executor = None
        self.heartbeat_sender = None
        self.log_flusher = None
        self.sse_reader = None

    def load_config(self) -> None:
        """
        Load configuration from environment variables.

        Required:
        - REPL_INSTANCE_ID: Instance identifier

        Optional:
        - REPL_CONTROL_PLANE_URL: Control plane URL (default: https://api.skyrepl.io)
        - REPL_PROVIDER: Provider name (default: aws)
        """
        raise NotImplementedError("load_config not implemented")

    def acquire_lock(self) -> bool:
        """
        Acquire exclusive agent lock to prevent duplicates on warm pool reuse.

        Lock file: /tmp/repl-agent.lock
        - Persists across restarts within same boot
        - Cleared on instance termination
        - Uses LOCK_NB (non-blocking) to fail immediately if held

        Returns:
            True if lock acquired, False if lock held by another process
        """
        raise NotImplementedError("acquire_lock not implemented")

    def setup_signal_handlers(self) -> None:
        """Register SIGTERM and SIGINT handlers for graceful shutdown."""
        raise NotImplementedError("setup_signal_handlers not implemented")

    def start_threads(self) -> None:
        """
        Start background threads.

        Threads:
        - log-flusher: 100ms interval (log batching)
        - heartbeat: 10s interval (liveness + state reporting)
        - spot-monitor: 5s interval (AWS IMDS polling, AWS only)

        All threads are daemon threads.
        """
        raise NotImplementedError("start_threads not implemented")

    def run_sse_loop(self) -> str:
        """
        Connect to SSE command stream and enter dispatch loop.

        Blocks until connection fails or shutdown requested.

        Returns:
            Exit reason string (e.g., 'shutdown_requested', 'sse_fatal')
        """
        raise NotImplementedError("run_sse_loop not implemented")

    def initiate_shutdown(self, reason: str) -> None:
        """
        Ordered shutdown sequence:
        1. Set shutting_down flag (stops all threads)
        2. Send panic diagnostics (2s timeout, best-effort)
        3. Tailscale logout (cleanup device list)
        4. Terminate running run (if any)
        5. Flush remaining logs
        6. Wait grace period (SHUTDOWN_GRACE_PERIOD_S)
        7. Self-terminate: shutdown -h now (BACKUP SAFETY)

        Agent self-termination is LAST RESORT. Normal path: control plane
        terminates via provider API. Backup: agent self-terminates if control
        plane unreachable (crash, network partition, bug).

        Args:
            reason: Shutdown reason (e.g., 'sigterm', 'sse_fatal', 'heartbeat_timeout')
        """
        raise NotImplementedError("initiate_shutdown not implemented")

    def run(self) -> int:
        """
        Main agent entry point.

        Returns:
            Exit code (0 = success, 1 = error)
        """
        raise NotImplementedError("run not implemented")


def main() -> int:
    """Entry point."""
    agent = Agent()
    return agent.run()


if __name__ == "__main__":
    sys.exit(main())
