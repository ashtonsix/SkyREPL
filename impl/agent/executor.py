"""
Run Executor

Handles start_run, cancel_run, prepare_snapshot commands.
Manages subprocess lifecycle, file sync, and command acknowledgment.

Command acknowledgment protocol:
- Track last 100 processed command_ids for deduplication
- Queue acks in pending_command_acks for next heartbeat
- Prevents duplicate execution on SSE retries
"""

from __future__ import annotations

import collections
import subprocess
import threading
from typing import Optional, List, Set
from dataclasses import dataclass


@dataclass
class RunState:
    """State for currently executing run."""

    run_id: str
    allocation_id: int
    process: Optional[subprocess.Popen]
    interrupted: bool = False


class Executor:
    """
    Manages run lifecycle: file sync, command execution, artifact collection.

    State tracking:
    - current_run: Active run state (None when idle)
    - pending_command_acks: Command IDs awaiting heartbeat transmission
    - processed_command_ids: Last 100 command IDs (deduplication buffer)
    """

    def __init__(self):
        """Initialize executor."""
        self.current_run: Optional[RunState] = None
        self.pending_command_acks: Set[int] = set()
        self.processed_command_ids: collections.deque = collections.deque(maxlen=100)
        self._lock = threading.Lock()

    def start_run(self, msg: dict) -> None:
        """
        Handle start_run command. Full sequence:
        1. Dedup check + ack
        2. Clean workdir (critical for warm pool security)
        3. Download files (verify checksums)
        4. Execute init script (if provided)
        5. Send sync_complete
        6. Execute command (subprocess)
        7. Stream stdout/stderr to log buffer
        8. Wait for completion
        9. Send run_complete
        10. Collect and upload artifacts

        Args:
            msg: start_run message with run_id, allocation_id, command, etc.
        """
        raise NotImplementedError("start_run not implemented")

    def cancel_run(self, msg: dict) -> None:
        """
        Cancel a running run. Sequence:
        1. Dedup check + ack
        2. Find process by run_id
        3. Send SIGTERM, wait 10s
        4. If still running: SIGKILL
        5. Send run_complete with exit_code=-15 (SIGTERM)

        Args:
            msg: cancel_run message with run_id
        """
        raise NotImplementedError("cancel_run not implemented")

    def prepare_snapshot(self, msg: dict) -> None:
        """
        Prepare instance for snapshot creation. Sequence:
        1. Dedup check + ack
        2. Sync filesystems
        3. Clear temp files
        4. Clear workdir (no run state in snapshots)
        5. Flush buffers
        6. Send prepare_snapshot_ack

        Args:
            msg: prepare_snapshot message with instance_id
        """
        raise NotImplementedError("prepare_snapshot not implemented")

    def capture_artifacts(self, msg: dict) -> None:
        """
        Mid-run or emergency artifact capture (fire-and-forget).
        Runs in background thread to not block SSE reader.

        Args:
            msg: capture_artifacts message with run_id
        """
        raise NotImplementedError("capture_artifacts not implemented")

    def terminate_current_run(self) -> None:
        """
        Terminate current run during shutdown.

        Called by agent.py during graceful shutdown.
        Sends SIGTERM, waits 10s, then SIGKILL if still running.
        """
        raise NotImplementedError("terminate_current_run not implemented")

    def get_and_clear_pending_acks(self) -> List[int]:
        """
        Get and clear pending command acknowledgments.

        Called by heartbeat thread after successful heartbeat POST.

        Returns:
            List of command IDs to acknowledge
        """
        raise NotImplementedError("get_and_clear_pending_acks not implemented")

    def _check_and_ack_command(self, command_id: Optional[int]) -> bool:
        """
        Check deduplication and queue acknowledgment.

        Args:
            command_id: Command ID to check (None = fire-and-forget)

        Returns:
            True if command is a duplicate (skip execution)
            False if command is new (proceed with execution)
        """
        raise NotImplementedError("_check_and_ack_command not implemented")
