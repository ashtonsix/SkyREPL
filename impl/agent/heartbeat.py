"""
Heartbeat Thread

Responsibilities:
- Send periodic heartbeat to control plane (10s interval)
- Track last acknowledgment time
- Detect degraded state (2m no ack)
- Initiate panic checkpoint on timeout (15m no ack)
- Report allocation SSH state
- Report dropped log count
"""

from __future__ import annotations

import threading
import time
from typing import Optional


class HeartbeatSender:
    """
    Manages heartbeat lifecycle and state detection.

    State tracking:
    - last_ack_time: Timestamp of last successful heartbeat ack
    - degraded_since: Timestamp when degraded state began (None = healthy)
    - expected_control_plane_id: Control plane ID from first ack (for validation)
    """

    def __init__(self, control_plane_url: str, instance_id: str):
        """
        Initialize heartbeat sender.

        Args:
            control_plane_url: Control plane base URL
            instance_id: Instance identifier
        """
        self.control_plane_url = control_plane_url
        self.instance_id = instance_id
        self.last_ack_time: float = time.time()
        self.degraded_since: Optional[float] = None
        self.expected_control_plane_id: Optional[str] = None
        self.shutting_down: bool = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """
        Start heartbeat thread.

        Sends immediate heartbeat, then enters 10s loop.
        Checks degraded threshold (2m) and panic threshold (15m).
        """
        raise NotImplementedError("start not implemented")

    def stop(self) -> None:
        """Signal heartbeat thread to stop."""
        self.shutting_down = True

    def send_heartbeat(self, workflow_state: Optional[str] = None) -> bool:
        """
        Send heartbeat to control plane.

        POST /v1/agent/heartbeat

        Payload:
        - tailscale_ip: Tailscale IP (if available)
        - active_allocations: List of {allocation_id, has_ssh_sessions}
        - tailscale_status: Tailscale connection state
        - workflow_state: Current workflow state or 'degraded:...'
        - pending_command_acks: Command IDs to acknowledge
        - dropped_logs_count: Total dropped logs

        Args:
            workflow_state: Override workflow state (for degraded reporting)

        Returns:
            True if ack received, False on failure
        """
        raise NotImplementedError("send_heartbeat not implemented")

    def initiate_panic_checkpoint(self, time_since_ack_ms: int) -> None:
        """
        Execute panic checkpoint and shutdown.

        Sequence:
        1. Send panic_start message
        2. Execute checkpoint script (5m budget)
        3. Upload artifacts
        4. Send panic_complete message
        5. Initiate shutdown

        Args:
            time_since_ack_ms: Milliseconds since last heartbeat ack
        """
        raise NotImplementedError("initiate_panic_checkpoint not implemented")

    def record_heartbeat_ack(self) -> None:
        """
        Record successful heartbeat acknowledgment.

        Called by SSE handler when heartbeat_ack message received.
        Updates last_ack_time and clears degraded state.
        """
        raise NotImplementedError("record_heartbeat_ack not implemented")

    def get_last_ack_time(self) -> float:
        """Return timestamp of last heartbeat ack."""
        return self.last_ack_time
