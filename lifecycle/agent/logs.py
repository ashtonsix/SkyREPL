"""
Log Collection, Batching, Upload

Two-Level Log Batching

Level 1 - Local Buffer (100ms):
  Collects stdout/stderr into memory, flushes to send queue every 100ms.
  Ensures low-latency local processing without per-line overhead.

Level 2 - Network Batch (5s or 64KB):
  Sends queued logs to control plane every 5s OR when queue exceeds 64KB.
  Reduces HTTP request overhead. Backpressure: if network slow, logs buffer locally.

Special: sync_complete bypasses both levels (sent immediately via flush_and_send).
"""

from __future__ import annotations

from collections import deque
import os
import threading
import time
from typing import Deque, List, Optional

from http_client import http_post

# ---------------------------------------------------------------------------
# Configuration (from environment, milliseconds converted to seconds)
# ---------------------------------------------------------------------------

LOG_LOCAL_BUFFER_MS = int(os.getenv("SKYREPL_LOG_LOCAL_BUFFER", "100"))
LOG_NETWORK_BATCH_MS = int(os.getenv("SKYREPL_LOG_NETWORK_BATCH_MS", "5000"))
LOG_NETWORK_BATCH_SIZE = 64 * 1024  # 64KB threshold
LOG_BUFFER_MAX_SIZE = 1024 * 1024  # 1MB max buffer before overflow

LOG_RETRY_BASE_S = 1
LOG_RETRY_MAX_S = 32

# Degraded mode disk buffer
DEGRADED_LOG_PATH = os.getenv("SKYREPL_DEGRADED_LOG_PATH", "/tmp/skyrepl-degraded-logs.jsonl")
DEGRADED_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB cap

# Module-level state
_shutting_down: bool = False
_degraded_mode: bool = False


def configure(control_plane_url: str, auth_token: str = "") -> None:
    """Set the control plane URL and auth token. Called once at startup."""
    import http_client
    http_client.configure(control_plane_url, auth_token)


def set_shutting_down(value: bool) -> None:
    """Signal log flusher to exit gracefully."""
    global _shutting_down
    _shutting_down = value


def set_degraded(value: bool) -> None:
    """Enter/exit degraded mode. When degraded, failed sends go to disk buffer."""
    global _degraded_mode
    _degraded_mode = value


def is_degraded() -> bool:
    """Return whether agent is in degraded mode."""
    return _degraded_mode


# =============================================================================
# Degraded Mode Disk Buffer
# =============================================================================


def _write_to_disk_buffer(batch: List[dict]) -> None:
    """Append log batch to disk buffer file. Truncates if over 10MB."""
    import json as _json

    try:
        # Check current size and truncate if needed
        try:
            size = os.path.getsize(DEGRADED_LOG_PATH)
        except FileNotFoundError:
            size = 0

        if size >= DEGRADED_LOG_MAX_BYTES:
            # Truncate: keep the last half of the file
            _truncate_disk_buffer()

        with open(DEGRADED_LOG_PATH, "a") as f:
            for entry in batch:
                f.write(_json.dumps(entry) + "\n")
    except Exception as e:
        _log("WARN", f"Failed to write degraded log buffer: {e}")


def _truncate_disk_buffer() -> None:
    """Truncate disk buffer to keep only the newer half."""
    try:
        with open(DEGRADED_LOG_PATH, "r") as f:
            lines = f.readlines()
        # Keep last half
        keep = lines[len(lines) // 2 :]
        with open(DEGRADED_LOG_PATH, "w") as f:
            f.writelines(keep)
        _log("INFO", f"Truncated degraded log buffer: {len(lines)} → {len(keep)} entries")
    except Exception as e:
        _log("WARN", f"Failed to truncate degraded log buffer: {e}")


def flush_disk_buffer() -> bool:
    """
    Flush disk-buffered logs to control plane after recovery.
    Returns True if all flushed successfully.
    """
    import json as _json

    if not os.path.exists(DEGRADED_LOG_PATH):
        return True

    try:
        with open(DEGRADED_LOG_PATH, "r") as f:
            lines = f.readlines()
    except Exception as e:
        _log("WARN", f"Failed to read degraded log buffer: {e}")
        return False

    if not lines:
        _cleanup_disk_buffer()
        return True

    _log("INFO", f"Flushing {len(lines)} degraded-mode log entries to control plane")

    batch: List[dict] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            batch.append(_json.loads(line))
        except _json.JSONDecodeError:
            continue

    if batch:
        success = _send_log_batch(batch)
        if not success:
            _log("WARN", "Failed to flush degraded log buffer, will retry next recovery")
            return False

    _cleanup_disk_buffer()
    _log("INFO", "Degraded log buffer flushed successfully")
    return True


def _cleanup_disk_buffer() -> None:
    """Remove disk buffer file."""
    try:
        os.remove(DEGRADED_LOG_PATH)
    except FileNotFoundError:
        pass
    except Exception as e:
        _log("WARN", f"Failed to clean up degraded log buffer: {e}")


# =============================================================================
# Log Buffer
# =============================================================================


class LogBuffer:
    """
    Thread-safe log buffer with two-level batching.

    Producers (executor threads) append log entries.
    Consumer (log_flusher_thread) drains and sends in batches.
    """

    def __init__(self) -> None:
        self._buffer: List[dict] = []  # Level 1: local buffer
        self._send_queue: Deque[dict] = deque()  # Level 2: network send queue
        self._lock = threading.Lock()
        self._batch_sequence_id: int = 0
        self._dropped_logs_count: int = 0
        self._total_queued_bytes: int = 0

    def append(self, entry: dict) -> None:
        """Add log entry to local buffer. Thread-safe."""
        with self._lock:
            entry_size = len(entry.get("data", ""))
            if self._total_queued_bytes + entry_size > LOG_BUFFER_MAX_SIZE:
                self._handle_overflow(entry_size)
                return
            self._buffer.append(entry)

    def _handle_overflow(self, entry_size: int) -> None:
        """Handle buffer overflow (>1MB). Drop oldest entries to make room."""
        dropped = 0
        while self._total_queued_bytes + entry_size > LOG_BUFFER_MAX_SIZE and self._send_queue:
            removed = self._send_queue.popleft()
            self._total_queued_bytes -= len(removed.get("data", ""))
            dropped += 1

        self._dropped_logs_count += dropped

        if dropped > 0:
            self._buffer.append(
                {
                    "type": "log",
                    "stream": "stderr",
                    "data": f"[WARN] Log buffer overflow, {dropped} oldest entries dropped",
                    "phase": "execution",
                }
            )

        _log("WARN", f"Log buffer overflow: dropped {dropped} entries (total: {self._dropped_logs_count})")

    def flush_local_to_queue(self) -> None:
        """Level 1 flush: move local buffer entries to network send queue."""
        with self._lock:
            if not self._buffer:
                return
            for entry in self._buffer:
                self._total_queued_bytes += len(entry.get("data", ""))
            self._send_queue.extend(self._buffer)
            self._buffer.clear()

    def drain_send_queue(self) -> Optional[List[dict]]:
        """Level 2 drain: get all entries from send queue for network transmission."""
        with self._lock:
            if not self._send_queue:
                return None

            batch = list(self._send_queue)
            self._send_queue.clear()
            self._total_queued_bytes = 0

            seq_id = self._batch_sequence_id
            self._batch_sequence_id += 1

        for entry in batch:
            entry["batch_sequence_id"] = seq_id

        return batch

    def should_send(self) -> bool:
        """Check if send queue exceeds size threshold (64KB)."""
        with self._lock:
            return self._total_queued_bytes >= LOG_NETWORK_BATCH_SIZE

    def get_dropped_count(self) -> int:
        """Return total dropped logs count (included in heartbeat)."""
        return self._dropped_logs_count

    def size(self) -> int:
        """Return approximate buffer size (for diagnostics)."""
        with self._lock:
            return len(self._buffer) + len(self._send_queue)


# Singleton log buffer (imported by executor.py and agent.py)
log_buffer = LogBuffer()


# =============================================================================
# Log Flusher Thread
# =============================================================================


def log_flusher_thread() -> None:
    """
    Background thread: two-level log batching.

    Inner loop (100ms): flush local buffer to send queue.
    Outer check: send to network every 5s OR when queue > 64KB.
    """
    last_network_send = time.time()

    while not _shutting_down:
        time.sleep(LOG_LOCAL_BUFFER_MS / 1000.0)
        if _shutting_down:
            break

        # Level 1: flush local buffer to send queue
        log_buffer.flush_local_to_queue()

        # Level 2: check if network send needed
        time_since_send = (time.time() - last_network_send) * 1000
        should_send_time = time_since_send >= LOG_NETWORK_BATCH_MS
        should_send_size = log_buffer.should_send()

        if should_send_time or should_send_size:
            batch = log_buffer.drain_send_queue()
            if batch:
                _send_log_batch(batch)
                last_network_send = time.time()

    # Final flush on shutdown
    flush_all()


# =============================================================================
# Network Sending
# =============================================================================


def _send_log_batch(batch: List[dict]) -> bool:
    """
    POST /v1/agent/logs with retry and backoff.

    For Slice 1, sends each entry individually (control plane expects single log).
    Returns True if sent successfully, False on persistent failure.
    When degraded, failed entries are buffered to disk instead of being dropped.
    """
    delay = LOG_RETRY_BASE_S
    max_attempts = 2 if _degraded_mode else 5
    failed_entries: List[dict] = []

    for entry in batch:
        # Ensure required fields
        if "timestamp" not in entry:
            entry["timestamp"] = time.time() * 1000

        payload = {
            "run_id": entry.get("run_id"),
            "stream": entry.get("stream", "stdout"),
            "data": entry.get("data", ""),
            "timestamp": entry.get("timestamp"),
            "sequence": entry.get("batch_sequence_id", 0),
            "phase": entry.get("phase", "execution"),
        }
        if entry.get("sync_success") is not None:
            payload["sync_success"] = entry["sync_success"]

        sent = False
        attempt_delay = delay
        for attempt in range(max_attempts):
            try:
                resp = http_post("/v1/agent/logs", payload, timeout=10)
                # Read response body to release connection
                resp.read()
                if resp.status == 200:
                    sent = True
                    break
                elif resp.status >= 500:
                    _log("WARN", f"Log batch failed (5xx): {resp.status}, retrying in {attempt_delay}s")
                else:
                    _log("WARN", f"Log batch failed: {resp.status}, not retrying")
                    break
            except Exception as e:
                _log("WARN", f"Log batch send error: {e}, retrying in {attempt_delay}s")

            if _shutting_down:
                if _degraded_mode and not sent:
                    failed_entries.append(entry)
                return False

            time.sleep(attempt_delay)
            attempt_delay = min(attempt_delay * 2, LOG_RETRY_MAX_S)

        if not sent and _degraded_mode:
            failed_entries.append(entry)

    # Buffer failed entries to disk during degraded mode
    if failed_entries:
        _write_to_disk_buffer(failed_entries)

    return len(failed_entries) == 0


# =============================================================================
# Immediate Send (for critical events)
# =============================================================================


def flush_and_send(entries: List[dict]) -> bool:
    """
    Immediate send bypassing both buffer levels.

    Used for critical events: sync_complete, run_complete, critical errors.
    Also flushes current local buffer to ensure ordering.
    """
    # First flush any pending local buffer
    log_buffer.flush_local_to_queue()

    # Drain anything in the send queue (send it first for ordering)
    pending_batch = log_buffer.drain_send_queue()
    if pending_batch:
        _send_log_batch(pending_batch)

    # Now send the critical entries immediately
    if entries:
        seq_id = log_buffer._batch_sequence_id
        log_buffer._batch_sequence_id += 1
        for entry in entries:
            entry["batch_sequence_id"] = seq_id
        return _send_log_batch(entries)

    return True


def flush_all() -> None:
    """Flush all buffered logs. Called during shutdown."""
    log_buffer.flush_local_to_queue()
    batch = log_buffer.drain_send_queue()
    if batch:
        _send_log_batch(batch)


def flush_all_logs() -> None:
    """Alias for flush_all, called by agent.py during shutdown."""
    flush_all()


# =============================================================================
# State Accessors (for heartbeat and diagnostics)
# =============================================================================


def get_dropped_logs_count() -> int:
    """Return total dropped logs count (for heartbeat payload)."""
    return log_buffer.get_dropped_count()


def get_log_buffer_size() -> int:
    """Return buffer size (for panic diagnostics)."""
    return log_buffer.size()


# =============================================================================
# Convenience (module-level functions matching L2 interface)
# =============================================================================


def buffer_log(entry: dict) -> None:
    """Add log entry to buffer. Global function for ease of use from executor."""
    log_buffer.append(entry)


def flush() -> None:
    """Flush all buffered logs (called during shutdown)."""
    flush_all()


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [logs] {message}")
