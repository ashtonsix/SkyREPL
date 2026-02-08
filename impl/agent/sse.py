"""
SSE Implementation for Agent Command Stream

Design rationale:
- Byte-by-byte reading avoids Python's 8KB buffer deadlock
- SSE messages are ~150 bytes, infrequent (<1/min typical)
- Syscall overhead negligible for low-frequency control signaling
- Agent spends 99.9% of time blocked on read(1)

Message flow:
  Control Plane ──SSE──> Agent
  Types: start_run, cancel_run, prepare_snapshot, capture_artifacts,
         trigger_tailscale, heartbeat_ack
"""

from __future__ import annotations

import json
import time
from typing import Optional, Generator, Any
from dataclasses import dataclass


@dataclass
class SSEMessage:
    """Parsed SSE message with event type and data."""

    event: str
    data: dict


class SSEReader:
    """
    SSE event stream reader with reconnection logic.

    Responsibilities:
    - Low-level byte-by-byte reading (avoids buffer deadlock)
    - SSE protocol parsing (event/data lines, blank line separation)
    - Exponential backoff reconnection
    - Message dispatch to executor
    """

    def __init__(self, control_plane_url: str, instance_id: str):
        """
        Initialize SSE reader.

        Args:
            control_plane_url: Control plane base URL
            instance_id: Instance identifier for authentication
        """
        self.control_plane_url = control_plane_url
        self.instance_id = instance_id
        self.shutting_down = False
        self.reconnect_count = 0
        self.last_message_time: Optional[float] = None

    def connect(self) -> None:
        """
        Connect to SSE command stream.

        GET /v1/agent/commands?instance_id={instance_id}
        Accept: text/event-stream

        Raises:
            Exception: On connection or HTTP errors
        """
        raise NotImplementedError("connect not implemented")

    def read_events(self) -> Generator[SSEMessage, None, None]:
        """
        Read SSE events from stream with automatic reconnection.

        Implements exponential backoff:
        - Base delay: 2 seconds
        - Max delay: 64 seconds
        - Max retries: 10

        Yields:
            SSEMessage: Parsed event with type and data
            None: Signals fatal connection failure (max retries exhausted)
        """
        raise NotImplementedError("read_events not implemented")

    def parse_sse_line(self, line: str) -> Optional[SSEMessage]:
        """
        Parse a single SSE line.

        SSE format:
            event: <type>
            data: <json>

            (blank line separates events)

        Args:
            line: Single line from SSE stream

        Returns:
            SSEMessage if complete event parsed, None otherwise
        """
        raise NotImplementedError("parse_sse_line not implemented")

    def set_shutting_down(self, value: bool) -> None:
        """Signal SSE reader to exit gracefully."""
        self.shutting_down = value

    def get_reconnect_count(self) -> int:
        """Return total reconnection count for diagnostics."""
        return self.reconnect_count

    def get_last_message_time(self) -> Optional[float]:
        """Return timestamp of last received message for diagnostics."""
        return self.last_message_time
