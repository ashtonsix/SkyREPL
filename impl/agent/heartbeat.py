"""
Heartbeat Thread

Responsibilities:
- Send periodic heartbeat to control plane (10s interval)
- Track last acknowledgment time
- Detect degraded state (2m no ack) -- logged only for Slice 1
- Report allocation state and dropped log count
"""

from __future__ import annotations

import http.client
import json
import os
import threading
import time
from typing import Optional
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Configuration (from environment, milliseconds)
# ---------------------------------------------------------------------------

HEARTBEAT_INTERVAL_MS = int(os.getenv("SKYREPL_HEARTBEAT_INTERVAL_MS", "10000"))
HEARTBEAT_DEGRADED_MS = 2 * 60 * 1000  # 2 minutes

# Module-level state
_control_plane_url: str = ""
_instance_id: str = ""
_shutting_down: bool = False
_shutdown_event: Optional[threading.Event] = None

_last_ack_time: float = 0.0
_degraded_since: Optional[float] = None
_expected_control_plane_id: Optional[str] = None
_lock = threading.Lock()

# Reference to executor for pending acks (set by agent.py)
_executor: object = None


def configure(control_plane_url: str, instance_id: str, shutdown_event: threading.Event) -> None:
    """Set connection parameters. Called once at startup."""
    global _control_plane_url, _instance_id, _shutdown_event, _last_ack_time
    _control_plane_url = control_plane_url
    _instance_id = instance_id
    _shutdown_event = shutdown_event
    _last_ack_time = time.time()


def set_executor(executor: object) -> None:
    """Set executor reference for pending acks."""
    global _executor
    _executor = executor


def set_shutting_down(value: bool) -> None:
    """Signal heartbeat thread to exit."""
    global _shutting_down
    _shutting_down = value
    if _shutdown_event and value:
        _shutdown_event.set()


def get_last_ack_time() -> float:
    """Return timestamp of last heartbeat ack."""
    return _last_ack_time


# =============================================================================
# HTTP Helper
# =============================================================================


def _http_post(path: str, payload: object, timeout: int = 10) -> http.client.HTTPResponse:
    """POST JSON to control plane."""
    parsed = urlparse(_control_plane_url)
    if parsed.scheme == "https":
        conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=timeout)
    else:
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout)

    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}

    try:
        conn.request("POST", path, body=body, headers=headers)
        return conn.getresponse()
    except Exception:
        conn.close()
        raise


# =============================================================================
# Heartbeat Thread
# =============================================================================


def heartbeat_thread() -> None:
    """
    Background thread: send heartbeat every 10s.

    Immediate heartbeat on startup, then periodic.
    Tracks degraded state (2m no ack) with log warnings.
    """
    global _last_ack_time, _degraded_since

    # Immediate first heartbeat
    if _send_heartbeat():
        _last_ack_time = time.time()
        _log("INFO", "Initial heartbeat acknowledged")
    else:
        _log("WARN", "Initial heartbeat failed")

    while not _shutting_down:
        # Sleep with shutdown check (use event for clean wakeup)
        if _shutdown_event:
            _shutdown_event.wait(timeout=HEARTBEAT_INTERVAL_MS / 1000.0)
        else:
            time.sleep(HEARTBEAT_INTERVAL_MS / 1000.0)

        if _shutting_down:
            break

        time_since_ack_ms = (time.time() - _last_ack_time) * 1000

        # Check degraded threshold (2m)
        if time_since_ack_ms >= HEARTBEAT_DEGRADED_MS and _degraded_since is None:
            _degraded_since = time.time()
            _log("WARN", f"Control plane unreachable for {HEARTBEAT_DEGRADED_MS / 1000}s, entering DEGRADED state")

        # Build workflow state
        workflow_state = "idle"
        if _degraded_since is not None:
            workflow_state = "degraded:control_plane_unreachable"

        # Send heartbeat
        if _send_heartbeat(workflow_state):
            _last_ack_time = time.time()
            if _degraded_since is not None:
                _log("INFO", f"Control plane recovered after {time.time() - _degraded_since:.1f}s degraded")
                _degraded_since = None


def _send_heartbeat(workflow_state: str = "idle") -> bool:
    """POST /v1/agent/heartbeat. Returns True if ack received."""
    global _expected_control_plane_id

    # Get pending acks from executor
    pending_acks: list = []
    if _executor and hasattr(_executor, "get_and_clear_pending_acks"):
        pending_acks = _executor.get_and_clear_pending_acks()  # type: ignore[attr-defined]

    # Get active allocations from executor
    active_allocations: list = []
    if _executor and hasattr(_executor, "current_allocation_id"):
        alloc_id = getattr(_executor, "current_allocation_id", None)
        if alloc_id is not None:
            active_allocations.append({"allocation_id": alloc_id, "has_ssh_sessions": False})

    # Get dropped logs count
    dropped_logs = 0
    try:
        from logs import get_dropped_logs_count

        dropped_logs = get_dropped_logs_count()
    except ImportError:
        pass

    payload = {
        "instance_id": int(_instance_id) if _instance_id else 0,
        "status": "running" if active_allocations else "idle",
        "active_allocations": active_allocations,
        "pending_command_acks": pending_acks,
        "dropped_logs_count": dropped_logs,
    }

    try:
        resp = _http_post("/v1/agent/heartbeat", payload, timeout=10)
        body = resp.read()

        if resp.status == 200:
            try:
                data = json.loads(body.decode("utf-8"))
                received_id = data.get("control_plane_id")
                if received_id:
                    if _expected_control_plane_id is None:
                        _expected_control_plane_id = received_id
                        _log("INFO", f"Control plane ID established: {received_id}")
                    elif received_id != _expected_control_plane_id:
                        _log("WARN", f"Control plane ID mismatch: expected={_expected_control_plane_id}, got={received_id}")
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            return True

        _log("WARN", f"Heartbeat returned {resp.status}")
        return False

    except Exception as e:
        _log("WARN", f"Heartbeat failed: {e}")
        return False


# =============================================================================
# Heartbeat Ack (called from SSE dispatch)
# =============================================================================


def record_heartbeat_ack(control_plane_id: Optional[str] = None) -> None:
    """
    Record successful heartbeat acknowledgment from SSE stream.

    Called by sse.py when heartbeat_ack message received.
    Updates last_ack_time and validates control_plane_id.
    """
    global _last_ack_time, _degraded_since, _expected_control_plane_id

    with _lock:
        _last_ack_time = time.time()

        if _degraded_since is not None:
            _log("INFO", f"Control plane recovered via SSE ack after {time.time() - _degraded_since:.1f}s")
            _degraded_since = None

        if control_plane_id:
            if _expected_control_plane_id is None:
                _expected_control_plane_id = control_plane_id
            elif control_plane_id != _expected_control_plane_id:
                _log("WARN", f"Control plane ID mismatch in SSE ack: expected={_expected_control_plane_id}, got={control_plane_id}")


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [heartbeat] {message}")
