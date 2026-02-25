"""
Run Executor

Handles start_run, cancel_run commands.
Manages subprocess lifecycle, file sync, and command acknowledgment.

Command acknowledgment protocol:
- Track last 100 processed command_ids for deduplication
- Queue acks in pending_command_acks for next heartbeat
- Prevents duplicate execution on SSE retries
"""

from __future__ import annotations

import collections
import hashlib
import http.client
import json
import os
import shutil
import signal
import subprocess
import threading
import time
from typing import Optional, List, Set
from urllib.parse import urlparse

from http_client import http_post, get_control_plane_url
from logs import log_buffer, flush_and_send, flush_all

# =============================================================================
# Constants
# =============================================================================

PROCESSED_COMMAND_HISTORY_SIZE = 100
CANCEL_GRACE_PERIOD_S = 10

# Module-level state (set by agent.py)
_workdir_base: str = "/workspace"


def configure(control_plane_url: str, workdir_base: str = "/workspace", auth_token: str = "") -> None:
    """Set connection parameters. Called once at startup."""
    global _workdir_base
    import http_client
    http_client.configure(control_plane_url, auth_token)
    _workdir_base = workdir_base


# =============================================================================
# HTTP Helpers
# =============================================================================


def _http_get(url: str, timeout: int = 300) -> bytes:
    """GET raw bytes from a URL (for file downloads)."""
    from http_client import _auth_token  # type: ignore[attr-defined]

    parsed = urlparse(url)

    # If relative URL, prepend control plane
    if not parsed.scheme:
        parsed = urlparse(get_control_plane_url() + url)

    if parsed.scheme == "https":
        conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=timeout)
    else:
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout)

    path = parsed.path
    if parsed.query:
        path = f"{path}?{parsed.query}"

    headers = {}
    if _auth_token:
        headers["Authorization"] = f"Bearer {_auth_token}"

    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}: {resp.reason}")
        return resp.read()
    finally:
        conn.close()


# =============================================================================
# Run Executor
# =============================================================================


class RunExecutor:
    """
    Manages run lifecycle: file sync, command execution, completion reporting.

    State tracking:
    - current_run_id: Active run identifier (None when idle)
    - current_process: subprocess.Popen for running command
    - current_allocation_id: Allocation executing current run
    - pending_command_acks: Command IDs awaiting heartbeat transmission
    - processed_command_ids: Last 100 command IDs (deduplication buffer)
    """

    def __init__(self) -> None:
        self.current_run_id: Optional[int] = None
        self.current_process: Optional[subprocess.Popen] = None
        self.current_allocation_id: Optional[int] = None

        # Command acknowledgment protocol
        self.pending_command_acks: Set[int] = set()
        self.processed_command_ids: collections.deque = collections.deque(
            maxlen=PROCESSED_COMMAND_HISTORY_SIZE
        )

        self._lock = threading.Lock()

    # -------------------------------------------------------------------------
    # Command Acknowledgment
    # -------------------------------------------------------------------------

    def _check_and_ack_command(self, command_id: Optional[int]) -> bool:
        """
        Check deduplication and queue acknowledgment.

        Returns True if command is a duplicate (skip execution).
        Returns False if command is new (proceed).
        """
        if command_id is None:
            return False

        with self._lock:
            self.pending_command_acks.add(command_id)

            if command_id in self.processed_command_ids:
                _log("INFO", f"Duplicate command_id={command_id}, skipping")
                return True

            self.processed_command_ids.append(command_id)
            return False

    def get_pending_acks(self) -> List[int]:
        """Return pending acks without clearing. Caller must clear after confirmed delivery."""
        with self._lock:
            return list(self.pending_command_acks)

    def clear_pending_acks(self, ack_ids: List[int]) -> None:
        """Clear specific ack IDs after confirmed delivery via heartbeat POST."""
        with self._lock:
            self.pending_command_acks -= set(ack_ids)

    # -------------------------------------------------------------------------
    # start_run Handler
    # -------------------------------------------------------------------------

    def start_run(self, msg: dict) -> None:
        """
        Handle start_run command. Dispatches to a background thread so the SSE
        reader stays available for cancel_run messages during execution.
        """
        command_id = msg.get("command_id")
        if self._check_and_ack_command(command_id):
            return

        thread = threading.Thread(
            target=self._run_in_background,
            args=(msg,),
            daemon=True,
            name="run-executor",
        )
        thread.start()

    def _run_in_background(self, msg: dict) -> None:
        """
        Execute run in background thread. Full sequence:
        1. Clean workdir
        2. Download files (verify checksums)
        3. Send sync_complete
        4. Execute command (subprocess)
        5. Stream stdout/stderr to log buffer
        6. Wait for completion
        7. Report completion via POST /v1/agent/status
        """
        run_id = msg["run_id"]
        allocation_id = msg["allocation_id"]
        workdir = msg.get("workdir", _workdir_base)
        command = msg["command"]
        files = msg.get("files", [])

        self.current_run_id = run_id
        self.current_allocation_id = allocation_id

        _log("INFO", f"Starting run {run_id} (allocation={allocation_id})")

        try:
            # Step 1: Send "started" status
            _send_status(run_id, "started")

            # Step 2: Clean workdir
            _cleanup_workdir(workdir)
            os.makedirs(workdir, exist_ok=True)

            # Step 3: Download files
            sync_success = _download_files(run_id, workdir, files)
            if not sync_success:
                _send_sync_complete(run_id, allocation_id, success=False)
                _send_status(run_id, "failed", exit_code=1)
                return

            # Step 4: Send sync_complete (bypasses batching)
            _send_sync_complete(run_id, allocation_id, success=True)

            # Step 5: Execute command
            exit_code = self._execute_command(run_id, allocation_id, workdir, command)

            # Step 6: Collect artifacts (best-effort, before log flush)
            artifact_patterns = msg.get("artifacts", [])
            _log("INFO", f"Artifact patterns for run {run_id}: {artifact_patterns}")
            if artifact_patterns:
                try:
                    import glob as _glob
                    debug_parts = [f"patterns={artifact_patterns}"]
                    for p in artifact_patterns:
                        full_p = os.path.join(workdir, p)
                        matches = _glob.glob(full_p, recursive=True)
                        debug_parts.append(f"glob('{full_p}') -> {matches}")
                        parent = os.path.dirname(full_p)
                        if os.path.isdir(parent):
                            contents = os.listdir(parent)
                            debug_parts.append(f"ls({parent}) -> {contents}")
                        else:
                            debug_parts.append(f"parent dir {parent} DOES NOT EXIST")
                    from artifacts import collect_and_upload, _debug_trace
                    _debug_trace.clear()
                    n = collect_and_upload(run_id, workdir, artifact_patterns)
                    debug_parts.append(f"collect_and_upload returned {n}")
                    if _debug_trace:
                        debug_parts.append("trace=[" + "; ".join(_debug_trace) + "]")
                    debug_msg = "[artifact-debug] " + " | ".join(debug_parts) + "\n"
                    log_buffer.append({
                        "type": "log",
                        "run_id": run_id,
                        "allocation_id": allocation_id,
                        "stream": "stderr",
                        "data": debug_msg,
                        "phase": "execution",
                        "timestamp": time.time() * 1000,
                    })
                except Exception as e:
                    _log("WARN", f"Artifact collection failed (best-effort): {e}")
                    log_buffer.append({
                        "type": "log",
                        "run_id": run_id,
                        "allocation_id": allocation_id,
                        "stream": "stderr",
                        "data": f"[artifact-debug] EXCEPTION: {e}\n",
                        "phase": "execution",
                        "timestamp": time.time() * 1000,
                    })

            # Step 7: Flush logs then report completion
            # Flush all buffered logs before sending terminal status, otherwise
            # the control plane closes CLI WebSockets before logs are delivered.
            flush_all()
            status = "completed" if exit_code == 0 else "failed"
            _send_status(run_id, status, exit_code=exit_code)

        except Exception as e:
            _log("ERROR", f"Run {run_id} failed: {e}")
            flush_all()
            _send_status(run_id, "failed", exit_code=1)

        finally:
            self.current_run_id = None
            self.current_process = None
            self.current_allocation_id = None

    def _execute_command(self, run_id: int, allocation_id: int, workdir: str, command: str) -> int:
        """Execute run command as subprocess, streaming output to log buffer."""
        try:
            proc = subprocess.Popen(
                command,
                shell=True,
                cwd=workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,  # New session/process group for clean cancel
            )
            self.current_process = proc

            # Stream stdout/stderr in background threads
            stdout_thread = threading.Thread(
                target=_stream_output,
                args=(proc.stdout, run_id, allocation_id, "stdout"),
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=_stream_output,
                args=(proc.stderr, run_id, allocation_id, "stderr"),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()

            # Wait for completion
            proc.wait()
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

            return proc.returncode

        except Exception as e:
            _log("ERROR", f"Command execution failed: {e}")
            return 1

    # -------------------------------------------------------------------------
    # cancel_run Handler
    # -------------------------------------------------------------------------

    def cancel_run(self, msg: dict) -> None:
        """
        Cancel a running run.
        SIGTERM -> wait 10s -> SIGKILL if still running.
        Uses process group kill (os.killpg) to reach all children when shell=True.
        """
        command_id = msg.get("command_id")
        if self._check_and_ack_command(command_id):
            return

        run_id = msg["run_id"]
        _log("INFO", f"Cancelling run {run_id}")

        if self.current_run_id != run_id:
            _log("WARN", f"Cancel for unknown run {run_id}, ignoring")
            return

        proc = self.current_process
        if proc is None or proc.poll() is not None:
            _log("WARN", f"Run {run_id} not running, ignoring cancel")
            return

        try:
            # Kill the entire process group (shell + children)
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGTERM)
            _log("INFO", f"Sent SIGTERM to process group {pgid} for run {run_id}")

            try:
                proc.wait(timeout=CANCEL_GRACE_PERIOD_S)
                _log("INFO", f"Run {run_id} exited gracefully after SIGTERM")
            except subprocess.TimeoutExpired:
                os.killpg(pgid, signal.SIGKILL)
                proc.wait(timeout=5)
                _log("WARN", f"Run {run_id} killed after {CANCEL_GRACE_PERIOD_S}s grace period")

        except Exception as e:
            _log("ERROR", f"Cancel failed for run {run_id}: {e}")

    # -------------------------------------------------------------------------
    # Terminate (for shutdown)
    # -------------------------------------------------------------------------

    def terminate_current_run(self) -> None:
        """Terminate current run during shutdown. Called by agent.py."""
        proc = self.current_process
        if proc and proc.poll() is None:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
                proc.wait(timeout=CANCEL_GRACE_PERIOD_S)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    proc.kill()
            except Exception as e:
                _log("WARN", f"Failed to terminate run: {e}")


# =============================================================================
# Workdir Cleanup
# =============================================================================


def _cleanup_workdir(workdir: str) -> None:
    """Clean workdir before start_run. Critical for warm pool security."""
    # Change cwd to home before deletion — process cwd may be inside workdir (§11.10)
    try:
        os.chdir(os.path.expanduser("~"))
    except Exception:
        pass
    if os.path.exists(workdir):
        try:
            shutil.rmtree(workdir)
        except Exception as e:
            _log("WARN", f"Error removing workdir: {e}")


# =============================================================================
# File Download
# =============================================================================


def _download_files(run_id: int, workdir: str, files: list) -> bool:
    """
    Download files, verify SHA256 checksums.

    Returns True if all files downloaded and verified, False on any failure.
    """
    for file_entry in files:
        path = file_entry["path"]
        checksum = file_entry["checksum"]
        url = file_entry["url"]

        full_path = os.path.join(workdir, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        try:
            data = _http_get(url)

            # Verify checksum
            actual_checksum = hashlib.sha256(data).hexdigest()
            if actual_checksum != checksum:
                _log("ERROR", f"Checksum mismatch for {path}: expected={checksum}, actual={actual_checksum}")
                log_buffer.append(
                    {
                        "type": "log",
                        "run_id": run_id,
                        "stream": "stderr",
                        "data": f"Checksum mismatch: {path}\n",
                        "phase": "sync",
                        "timestamp": time.time() * 1000,
                    }
                )
                return False

            with open(full_path, "wb") as f:
                f.write(data)

            _log("DEBUG", f"Downloaded: {path} ({len(data)} bytes)")

        except Exception as e:
            _log("ERROR", f"Download failed for {path}: {e}")
            log_buffer.append(
                {
                    "type": "log",
                    "run_id": run_id,
                    "stream": "stderr",
                    "data": f"Download failed: {path}: {e}\n",
                    "phase": "sync",
                    "timestamp": time.time() * 1000,
                }
            )
            return False

    _log("INFO", f"Downloaded {len(files)} files for run {run_id}")
    return True


# =============================================================================
# Output Streaming
# =============================================================================


def _stream_output(pipe: object, run_id: int, allocation_id: int, stream: str) -> None:
    """Read subprocess pipe and send to log buffer."""
    try:
        for line in iter(pipe.readline, b""):  # type: ignore[attr-defined]
            text = line.decode("utf-8", errors="replace")  # type: ignore[union-attr]
            log_buffer.append(
                {
                    "type": "log",
                    "run_id": run_id,
                    "allocation_id": allocation_id,
                    "stream": stream,
                    "data": text,
                    "phase": "execution",
                    "timestamp": time.time() * 1000,
                }
            )
    except Exception as e:
        _log("WARN", f"Output stream error ({stream}): {e}")
    finally:
        try:
            pipe.close()  # type: ignore[union-attr]
        except Exception:
            pass


# =============================================================================
# Message Senders
# =============================================================================


def _send_sync_complete(run_id: int, allocation_id: int, success: bool) -> None:
    """
    Send sync_complete log message. BYPASSES batching via flush_and_send().
    Critical: sync_complete latency counts against allocation timeout.
    """
    payload = {
        "type": "log",
        "run_id": run_id,
        "allocation_id": allocation_id,
        "stream": "sync_complete",
        "data": "",
        "phase": "sync",
        "sync_success": success,
        "timestamp": time.time() * 1000,
    }
    flush_and_send([payload])


def _send_status(run_id: int, status: str, exit_code: Optional[int] = None) -> None:
    """POST /v1/agent/status to report run status."""
    payload: dict = {
        "run_id": run_id,
        "status": status,
    }
    if exit_code is not None:
        payload["exit_code"] = exit_code

    try:
        resp = http_post("/v1/agent/status", payload, timeout=10)
        resp.read()
    except Exception as e:
        _log("ERROR", f"Failed to send status ({status}): {e}")


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [executor] {message}")
