"""
Heartbeat Thread

Responsibilities:
- Send periodic heartbeat to control plane (10s interval)
- Track last acknowledgment time
- Detect degraded state (2m no ack) -- logged only for Slice 1
- Detect PANIC state (15m no ack) -- begin checkpoint, send HeartbeatPanicStart
- Self-terminate at 20m no ack -- send HeartbeatPanicComplete, shutdown
- Report allocation state and dropped log count
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Optional

from http_client import http_post

# ---------------------------------------------------------------------------
# Configuration (from environment, milliseconds)
# ---------------------------------------------------------------------------

HEARTBEAT_INTERVAL_MS = int(os.getenv("SKYREPL_HEARTBEAT_INTERVAL_MS", "10000"))
HEARTBEAT_DEGRADED_MS = 2 * 60 * 1000  # 2 minutes
HEARTBEAT_PANIC_MS = 15 * 60 * 1000  # 15 minutes — enter PANIC (§11.5)
PANIC_CHECKPOINT_BUDGET_MS = 5 * 60 * 1000  # 5 minutes — checkpoint budget
HEARTBEAT_SELF_TERM_MS = 20 * 60 * 1000  # 20 minutes — self-terminate

# Degraded mode backoff: double interval each failure, cap at 60s
DEGRADED_BACKOFF_MAX_S = 60

# Module-level state
_instance_id: str = ""
_shutting_down: bool = False
_shutdown_event: Optional[threading.Event] = None

_last_ack_time: float = 0.0
_degraded_since: Optional[float] = None
_panic_started: bool = False
_expected_control_plane_id: Optional[str] = None
_lock = threading.Lock()

# Reference to executor for pending acks (set by agent.py)
_executor: object = None


def configure(control_plane_url: str, instance_id: str, shutdown_event: threading.Event, auth_token: str = "") -> None:
    """Set connection parameters. Called once at startup."""
    global _instance_id, _shutdown_event, _last_ack_time
    import http_client
    http_client.configure(control_plane_url, auth_token)
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
# Heartbeat Thread
# =============================================================================


def heartbeat_thread() -> None:
    """
    Background thread: send heartbeat every 10s (healthy) or with exponential
    backoff (degraded).

    Immediate heartbeat on startup, then periodic.
    Tracks degraded state (2m no ack), PANIC state (15m),
    and self-terminates at 20m.

    Degraded mode (#AGENT-04):
      - Exponential backoff: interval doubles each cycle, capped at 60s
      - Notifies logs module to buffer to disk
      - On recovery: resets backoff, flushes disk-buffered logs
    """
    global _last_ack_time, _degraded_since, _panic_started

    # Current sleep interval (increases during degraded mode)
    current_interval_s = HEARTBEAT_INTERVAL_MS / 1000.0

    # Immediate first heartbeat
    if _send_heartbeat():
        _last_ack_time = time.time()
        _log("INFO", "Initial heartbeat acknowledged")
    else:
        _log("WARN", "Initial heartbeat failed")

    while not _shutting_down:
        # Sleep with shutdown check (use event for clean wakeup)
        if _shutdown_event:
            _shutdown_event.wait(timeout=current_interval_s)
        else:
            time.sleep(current_interval_s)

        if _shutting_down:
            break

        time_since_ack_ms = (time.time() - _last_ack_time) * 1000

        # Check degraded threshold (2m)
        if time_since_ack_ms >= HEARTBEAT_DEGRADED_MS and _degraded_since is None:
            _degraded_since = time.time()
            _log("WARN", f"Control plane unreachable for {HEARTBEAT_DEGRADED_MS / 1000}s, entering DEGRADED state")
            _notify_logs_degraded(True)

        # Check self-termination threshold (20m) — must be checked before panic
        # start to handle case where both thresholds crossed in same iteration
        if time_since_ack_ms >= HEARTBEAT_SELF_TERM_MS and _panic_started:
            _log("WARN", f"No heartbeat ack for {HEARTBEAT_SELF_TERM_MS / 1000 / 60:.0f}m, initiating self-termination")
            _send_panic_complete()
            _self_terminate()
            break

        # Check panic threshold (15m) — begin self-termination sequence
        if time_since_ack_ms >= HEARTBEAT_PANIC_MS and not _panic_started:
            _panic_started = True
            _log("WARN", f"No heartbeat ack for {HEARTBEAT_PANIC_MS / 1000 / 60:.0f}m, entering PANIC state")
            _send_panic_start(time_since_ack_ms)
            _run_checkpoint_stub()

        # Build workflow state
        workflow_state = "idle"
        if _panic_started:
            workflow_state = "panic:self_termination_pending"
        elif _degraded_since is not None:
            workflow_state = "degraded:control_plane_unreachable"

        # Send heartbeat
        if _send_heartbeat(workflow_state):
            with _lock:
                _last_ack_time = time.time()
                was_degraded = _degraded_since is not None
                was_panic = _panic_started
                if _panic_started:
                    _log("INFO", "Control plane recovered during PANIC state, cancelling self-termination")
                    _panic_started = False
                if _degraded_since is not None:
                    _log("INFO", f"Control plane recovered after {time.time() - _degraded_since:.1f}s degraded")
                    _degraded_since = None

            # Recovery: reset backoff interval and flush disk-buffered logs
            if was_degraded or was_panic:
                current_interval_s = HEARTBEAT_INTERVAL_MS / 1000.0
                _notify_logs_degraded(False)
                _flush_degraded_logs()
        else:
            # Heartbeat failed — apply exponential backoff when degraded
            if _degraded_since is not None:
                current_interval_s = min(current_interval_s * 2, DEGRADED_BACKOFF_MAX_S)


def _send_heartbeat(workflow_state: str = "idle") -> bool:
    """POST /v1/agent/heartbeat. Returns True if ack received."""
    global _expected_control_plane_id

    # Get pending acks from executor (don't clear yet — clear after confirmed delivery)
    pending_acks: list = []
    if _executor and hasattr(_executor, "get_pending_acks"):
        pending_acks = _executor.get_pending_acks()  # type: ignore[attr-defined]

    # Get active allocations from executor
    active_allocations: list = []
    if _executor and hasattr(_executor, "current_allocation_id"):
        alloc_id = getattr(_executor, "current_allocation_id", None)
        if alloc_id is not None:
            active_allocations.append({
                "allocation_id": alloc_id,
                "has_ssh_sessions": _detect_ssh_sessions(),
            })

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
        # Stub fields — not yet instrumented
        "cpu_percent": None,
        "memory_percent": None,
        "disk_percent": None,
        "gpu_utilization": None,
        "gpu_memory_percent": None,
        "tailscale_ip": _get_tailscale_ip(),
        "tailscale_status": _get_tailscale_status(),
    }

    try:
        resp = http_post("/v1/agent/heartbeat", payload, timeout=10)
        body = resp.read()

        if resp.status == 200:
            # Clear acks only after confirmed delivery (DB-4 fix)
            if pending_acks and _executor and hasattr(_executor, "clear_pending_acks"):
                _executor.clear_pending_acks(pending_acks)  # type: ignore[attr-defined]
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
# Degraded Mode Helpers (#AGENT-04)
# =============================================================================


def _notify_logs_degraded(degraded: bool) -> None:
    """Notify logs module about degraded/healthy state change."""
    try:
        from logs import set_degraded
        set_degraded(degraded)
    except ImportError:
        pass


def _flush_degraded_logs() -> None:
    """Flush disk-buffered logs after recovery from degraded mode."""
    try:
        from logs import flush_disk_buffer
        flush_disk_buffer()
    except ImportError:
        pass


def get_state() -> str:
    """Return current heartbeat state: 'healthy', 'degraded', or 'panic'."""
    if _panic_started:
        return "panic"
    if _degraded_since is not None:
        return "degraded"
    return "healthy"


# =============================================================================
# Heartbeat Ack (called from SSE dispatch)
# =============================================================================


def record_heartbeat_ack(control_plane_id: Optional[str] = None) -> None:
    """
    Record successful heartbeat acknowledgment from SSE stream.

    Called by sse.py when heartbeat_ack message received.
    Updates last_ack_time and validates control_plane_id.
    Resets panic state if active (recovery from panic).
    On recovery from degraded: notifies logs module and flushes disk buffer.
    """
    global _last_ack_time, _degraded_since, _panic_started, _expected_control_plane_id

    was_degraded = False
    with _lock:
        _last_ack_time = time.time()

        if _panic_started:
            _log("INFO", "Control plane recovered via SSE ack during PANIC state, cancelling self-termination")
            _panic_started = False
            was_degraded = True

        if _degraded_since is not None:
            _log("INFO", f"Control plane recovered via SSE ack after {time.time() - _degraded_since:.1f}s")
            _degraded_since = None
            was_degraded = True

        if control_plane_id:
            if _expected_control_plane_id is None:
                _expected_control_plane_id = control_plane_id
            elif control_plane_id != _expected_control_plane_id:
                _log("WARN", f"Control plane ID mismatch in SSE ack: expected={_expected_control_plane_id}, got={control_plane_id}")

    # Recovery actions outside the lock to avoid deadlock
    if was_degraded:
        _notify_logs_degraded(False)
        _flush_degraded_logs()


# =============================================================================
# Panic / Self-Termination Helpers (§11.5)
# =============================================================================


def _get_current_run_id() -> Optional[int]:
    """Get current run ID from executor, if available."""
    if _executor and hasattr(_executor, "current_run_id"):
        return getattr(_executor, "current_run_id", None)
    return None


def _send_panic_start(time_since_ack_ms: float) -> None:
    """Best-effort POST to /v1/agent/heartbeat-panic-start."""
    run_id = _get_current_run_id()
    payload = {
        "instance_id": int(_instance_id) if _instance_id else 0,
        "run_id": run_id or 0,
        "time_since_ack_ms": int(time_since_ack_ms),
    }
    try:
        resp = http_post("/v1/agent/heartbeat-panic-start", payload, timeout=5)
        resp.read()
    except Exception as e:
        _log("WARN", f"Failed to send panic start (best-effort): {e}")


def _run_checkpoint_stub() -> None:
    """
    Checkpoint stub. Actual artifact upload via presigned URLs
    (primary + failover) lands in #AGENT-04.
    """
    _log("INFO", "Checkpoint stub — actual implementation in #AGENT-04")


def _send_panic_complete() -> None:
    """Best-effort POST to /v1/agent/heartbeat-panic-complete."""
    run_id = _get_current_run_id()
    payload = {
        "instance_id": int(_instance_id) if _instance_id else 0,
        "run_id": run_id or 0,
        "checkpoint_exit_code": None,
        "artifacts_uploaded": 0,
    }
    try:
        resp = http_post("/v1/agent/heartbeat-panic-complete", payload, timeout=5)
        resp.read()
    except Exception as e:
        _log("WARN", f"Failed to send panic complete (best-effort): {e}")


def _self_terminate() -> None:
    """
    Terminate the agent process.

    Signals the main thread to shut down via _shutdown_event.
    In dev/test: agent.py handles actual exit.
    In production: the shutdown handler would issue 'shutdown -h now'.
    """
    global _shutting_down
    _log("WARN", "Self-terminating agent due to prolonged control plane unreachability")
    _shutting_down = True
    if _shutdown_event:
        _shutdown_event.set()


# =============================================================================
# Tailscale State Accessors
# =============================================================================


def _get_tailscale_ip() -> Optional[str]:
    """Return current Tailscale IP from the tailscale module (best-effort)."""
    try:
        from tailscale import get_tailscale_ip
        return get_tailscale_ip()
    except ImportError:
        return None


def _get_tailscale_status() -> Optional[str]:
    """Return current Tailscale status from the tailscale module (best-effort)."""
    try:
        from tailscale import get_tailscale_status
        return get_tailscale_status()
    except ImportError:
        return None


# =============================================================================
# SSH Session Detection
# =============================================================================


def _detect_ssh_sessions() -> bool:
    """
    Detect whether there are active SSH sessions on this instance.

    Uses `who` to list logged-in users and checks for the instance's SSH user
    (from SKYREPL_SSH_USER env var, default 'ubuntu'). Falls back to counting
    any pts/* entries as SSH sessions.

    Best-effort: returns False on any error.
    """
    import subprocess as _subprocess
    try:
        result = _subprocess.run(
            ["who"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode != 0:
            return False

        ssh_user = os.getenv("SKYREPL_SSH_USER", "ubuntu")
        lines = result.stdout.strip().splitlines()
        for line in lines:
            # `who` output: username  pts/0  date  time  (host)
            # A pts entry means a terminal session (typically SSH)
            parts = line.split()
            if len(parts) >= 2:
                user = parts[0]
                tty = parts[1]
                if tty.startswith("pts/") and (user == ssh_user or ssh_user == "*"):
                    return True
        return False
    except Exception:
        return False


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [heartbeat] {message}")
