"""
SSE Implementation for Agent Command Stream

Design rationale:
- Byte-by-byte reading avoids Python's 8KB buffer deadlock
- SSE messages are ~150 bytes, infrequent (<1/min typical)
- Syscall overhead negligible for low-frequency control signaling
- Agent spends 99.9% of time blocked on read(1)

Message flow:
  Control Plane --SSE--> Agent
  Types: start_run, cancel_run, heartbeat_ack
"""

from __future__ import annotations

import http.client
import json
import os
import time
from dataclasses import dataclass
from typing import Generator, Optional
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Configuration (from environment, milliseconds converted to seconds)
# ---------------------------------------------------------------------------

SSE_RECONNECT_BASE_S = int(os.getenv("SKYREPL_SSE_RECONNECT_BASE", "2000")) / 1000  # 2s
SSE_RECONNECT_MAX_S = int(os.getenv("SKYREPL_SSE_RECONNECT_MAX", "64000")) / 1000  # 64s
SSE_MAX_RETRIES = int(os.getenv("SKYREPL_SSE_MAX_RETRIES", "10"))

# Module-level state (set by agent.py at startup)
_control_plane_url: str = ""
_instance_id: str = ""
_auth_token: str = ""
_shutting_down: bool = False
_reconnect_count: int = 0
_last_message_time: Optional[float] = None


def configure(control_plane_url: str, instance_id: str, auth_token: str = "") -> None:
    """Set connection parameters. Called once at startup."""
    global _control_plane_url, _instance_id, _auth_token
    _control_plane_url = control_plane_url
    _instance_id = instance_id
    _auth_token = auth_token


def set_shutting_down(value: bool) -> None:
    """Signal SSE reader to exit gracefully."""
    global _shutting_down
    _shutting_down = value


def get_reconnect_count() -> int:
    """Return total reconnection count for diagnostics."""
    return _reconnect_count


def get_last_message_time() -> Optional[float]:
    """Return timestamp of last received message for diagnostics."""
    return _last_message_time


# =============================================================================
# SSE Message
# =============================================================================


@dataclass
class SSEMessage:
    """Parsed SSE message with event type and data."""

    event: str  # Event type (e.g., 'start_run', 'cancel_run')
    data: dict  # Parsed JSON payload


# =============================================================================
# Low-Level Byte Reading
# =============================================================================


def read_line(response: http.client.HTTPResponse) -> Optional[bytes]:
    """
    Read a single line from HTTP response, byte-by-byte via response.read(1).

    IMPORTANT: We must use response.read() (not response.fp.readline()) because
    response.fp is the raw socket buffer which does NOT decode chunked transfer
    encoding. response.read() goes through the HTTPResponse's chunk decoder,
    so chunk size headers (e.g. "aa\\r\\n") are stripped transparently.

    Byte-by-byte is fine here: SSE messages are ~150 bytes, infrequent (<1/min),
    and the agent spends 99.9% of time blocked waiting for the next byte.
    """
    if _shutting_down:
        return None
    try:
        buf = bytearray()
        while not _shutting_down:
            b = response.read(1)
            if not b:
                if buf:
                    return bytes(buf).rstrip(b"\r\n")
                return None
            if b == b"\n":
                return bytes(buf).rstrip(b"\r\n")
            buf.extend(b)
        return None
    except Exception:
        return None


# =============================================================================
# SSE Event Parsing
# =============================================================================


def parse_sse_events(response: http.client.HTTPResponse) -> Generator[SSEMessage, None, None]:
    """
    Parse SSE event stream into structured messages.

    SSE format:
        event: <type>
        data: <json>

        (blank line separates events)
    """
    global _last_message_time

    current_event: Optional[str] = None
    current_data: list[str] = []

    while not _shutting_down:
        line_bytes = read_line(response)
        if line_bytes is None:
            return

        line = line_bytes.decode("utf-8", errors="replace").strip()

        # Blank line = end of event, emit if we have data
        if not line:
            if current_data:
                try:
                    data_str = "\n".join(current_data)
                    data = json.loads(data_str)
                    event_type = current_event or "message"
                    _last_message_time = time.time()
                    yield SSEMessage(event=event_type, data=data)
                except json.JSONDecodeError as e:
                    _log("WARN", f"Invalid JSON in SSE data: {e}")

            current_event = None
            current_data = []
            continue

        # Comment line (SSE keepalive)
        if line.startswith(":"):
            continue

        # Event type line
        if line.startswith("event:"):
            current_event = line[6:].strip()
            continue

        # Data line
        if line.startswith("data:"):
            current_data.append(line[5:].strip())
            continue

        # Unknown line format
        _log("DEBUG", f"Unknown SSE line format: {line[:50]}")


# =============================================================================
# Connection Management
# =============================================================================


def _get_connection(url: str) -> http.client.HTTPConnection:
    """Create HTTP(S) connection from URL."""
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return http.client.HTTPSConnection(parsed.hostname, parsed.port or 443)
    else:
        return http.client.HTTPConnection(parsed.hostname, parsed.port or 80)


def _command_stream_single() -> Generator[SSEMessage, None, None]:
    """
    Single connection attempt for SSE command stream.

    Connects to control plane, sets up SSE stream, yields parsed messages.
    Exits on connection error or stream close.
    """
    conn = None
    try:
        conn = _get_connection(_control_plane_url)
        path = f"/v1/agent/commands?instance_id={_instance_id}"
        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        }
        if _auth_token:
            headers["Authorization"] = f"Bearer {_auth_token}"

        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()

        if resp.status != 200:
            _log("ERROR", f"Command stream failed: HTTP {resp.status}")
            raise Exception(f"HTTP {resp.status}: {resp.reason}")

        _log("INFO", "SSE command stream connected")

        for msg in parse_sse_events(resp):
            yield msg

        _log("INFO", "SSE stream closed by server")

    except Exception as e:
        _log("ERROR", f"Command stream error: {e}")
        raise

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# =============================================================================
# Reconnection with Exponential Backoff
# =============================================================================


def command_stream() -> Generator[Optional[SSEMessage], None, None]:
    """
    SSE command stream with automatic reconnection.

    Backoff: 2s, 4s, 8s, 16s, 32s, 64s, 64s, 64s, 64s, 64s
    On successful message, retry count resets to 0.
    Yields None on fatal failure (max retries exhausted).
    """
    global _reconnect_count

    retry_count = 0
    delay = SSE_RECONNECT_BASE_S

    while not _shutting_down:
        try:
            for msg in _command_stream_single():
                if retry_count > 0:
                    _log("INFO", f"SSE reconnected after {retry_count} retries")
                    retry_count = 0
                    delay = SSE_RECONNECT_BASE_S
                yield msg

            # Stream closed normally, reconnect
            if not _shutting_down:
                _log("INFO", "SSE stream ended, reconnecting...")
                _reconnect_count += 1
                continue

        except Exception as e:
            retry_count += 1
            _reconnect_count += 1

            if retry_count > SSE_MAX_RETRIES:
                _log("ERROR", f"SSE max retries ({SSE_MAX_RETRIES}) exhausted")
                yield None
                return

            _log("WARN", f"SSE connection failed ({retry_count}/{SSE_MAX_RETRIES}), retrying in {delay}s: {e}")
            _sleep_with_shutdown_check(delay)
            delay = min(delay * 2, SSE_RECONNECT_MAX_S)


def _sleep_with_shutdown_check(seconds: float) -> None:
    """Sleep, checking shutdown flag every 0.5s."""
    end_time = time.time() + seconds
    while time.time() < end_time and not _shutting_down:
        remaining = end_time - time.time()
        time.sleep(min(remaining, 0.5))


# =============================================================================
# Message Dispatch
# =============================================================================


def dispatch_message(msg: SSEMessage, executor: object) -> None:
    """
    Dispatch SSE message to appropriate executor handler.

    Routes: start_run, cancel_run, heartbeat_ack, trigger_tailscale.
    Non-MVP types logged and ignored for Slice 1.
    """
    msg_type = msg.data.get("type", msg.event)

    _log("DEBUG", f"Dispatching SSE message: {msg_type}")

    if msg_type == "start_run":
        executor.start_run(msg.data)  # type: ignore[attr-defined]

    elif msg_type == "cancel_run":
        executor.cancel_run(msg.data)  # type: ignore[attr-defined]

    elif msg_type == "heartbeat_ack":
        _handle_heartbeat_ack(msg.data)

    elif msg_type == "prepare_snapshot":
        _log("INFO", "prepare_snapshot not implemented in Slice 1, ignoring")

    elif msg_type == "capture_artifacts":
        _log("INFO", "capture_artifacts not implemented in Slice 1, ignoring")

    elif msg_type == "trigger_tailscale":
        _handle_trigger_tailscale(msg.data)

    else:
        _log("WARN", f"Unknown SSE message type: {msg_type}")


def _handle_heartbeat_ack(data: dict) -> None:
    """Handle heartbeat acknowledgment from control plane."""
    from heartbeat import record_heartbeat_ack

    control_plane_id = data.get("control_plane_id")
    record_heartbeat_ack(control_plane_id)


def _handle_trigger_tailscale(data: dict) -> None:
    """Dispatch trigger_tailscale to the tailscale module (non-blocking)."""
    from tailscale import handle_trigger_tailscale
    handle_trigger_tailscale(data)


# =============================================================================
# Main Command Loop
# =============================================================================


def run_command_loop(executor: object) -> str:
    """
    Main SSE command loop. Reads commands and dispatches to executor.

    Returns exit reason: 'shutdown_requested', 'sse_fatal', or error string.
    """
    try:
        for msg in command_stream():
            if _shutting_down:
                return "shutdown_requested"

            if msg is None:
                return "sse_fatal"

            dispatch_message(msg, executor)

    except Exception as e:
        _log("ERROR", f"Command loop error: {e}")
        return f"error: {e}"

    return "unknown"


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [sse] {message}")


__all__ = [
    "SSEMessage",
    "command_stream",
    "dispatch_message",
    "run_command_loop",
    "configure",
    "set_shutting_down",
    "get_reconnect_count",
    "get_last_message_time",
]
