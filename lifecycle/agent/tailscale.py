"""
Tailscale Handler — JIT Installation and Lifecycle Management

Installs Tailscale on demand when a trigger_tailscale SSE command arrives.
Uses static binary download (no apt, cross-distro, no lock contention).

Status lifecycle:
  not_installed -> installing -> ready | failed

Thread safety: module-level state is protected by _lock.
Auth key and hostname come from the SSE command payload (with env var fallback).

Shutdown: call tailscale_shutdown() to logout and kill the daemon.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.request
from typing import Optional

# =============================================================================
# Module-Level State
# =============================================================================

_tailscale_ip: Optional[str] = None
_tailscale_status: str = "not_installed"  # not_installed | installing | ready | failed
_tailscaled_proc: Optional[subprocess.Popen] = None
_install_dir: str = "/tmp/skyrepl-tailscale"
_lock = threading.Lock()

# =============================================================================
# Architecture Detection
# =============================================================================

_ARCH_MAP = {
    "x86_64": "amd64",
    "aarch64": "arm64",
    "armv7l": "arm",
}

def _detect_arch() -> str:
    """Return Tailscale arch string for the current machine."""
    machine = platform.machine()
    return _ARCH_MAP.get(machine, "amd64")


def _tailscale_url() -> str:
    """Return Tailscale download URL for the current arch."""
    arch = _detect_arch()
    env_url = os.getenv("REPL_TAILSCALE_URL", "")
    if env_url:
        return env_url
    return f"https://pkgs.tailscale.com/stable/tailscale_latest_{arch}.tgz"


# =============================================================================
# State Accessors (for heartbeat)
# =============================================================================


def get_tailscale_ip() -> Optional[str]:
    """Return current Tailscale IP (for HeartbeatMessage.tailscale_ip)."""
    with _lock:
        return _tailscale_ip


def get_tailscale_status() -> str:
    """Return current Tailscale status (for HeartbeatMessage.tailscale_status)."""
    with _lock:
        return _tailscale_status


# =============================================================================
# Startup: Detect Existing Installation
# =============================================================================


def detect_tailscale_ip() -> Optional[str]:
    """
    Detect existing Tailscale IP at agent startup (before any SSE commands).

    Checks:
    1. tailscale0 network interface via `ip addr`
    2. Tailscale CLI in PATH (`tailscale ip -4`)
    3. Bundled binary in _install_dir

    Updates module state if found.
    """
    global _tailscale_ip, _tailscale_status

    # 1. Check tailscale0 interface
    if os.path.exists("/sys/class/net/tailscale0"):
        try:
            result = subprocess.run(
                ["ip", "-4", "addr", "show", "tailscale0"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and "inet " in result.stdout:
                for line in result.stdout.split("\n"):
                    line = line.strip()
                    if line.startswith("inet "):
                        ip = line.split()[1].split("/")[0]
                        with _lock:
                            _tailscale_ip = ip
                            _tailscale_status = "ready"
                        _log("INFO", f"Existing Tailscale detected via interface: {ip}")
                        return ip
        except Exception:
            pass

    # 2. System tailscale CLI
    for binary in ["tailscale", os.path.join(_install_dir, "tailscale")]:
        try:
            result = subprocess.run(
                [binary, "ip", "-4"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                ip = result.stdout.strip()
                with _lock:
                    _tailscale_ip = ip
                    _tailscale_status = "ready"
                _log("INFO", f"Existing Tailscale detected via CLI: {ip}")
                return ip
        except (FileNotFoundError, subprocess.TimeoutExpired, PermissionError):
            pass

    return None


# =============================================================================
# JIT Installation
# =============================================================================


def handle_trigger_tailscale(msg: dict) -> None:
    """
    Handle the trigger_tailscale SSE command (fire-and-forget, runs in a thread).

    Payload fields:
      auth_key  (str) — Tailscale auth key. Falls back to REPL_TAILSCALE_AUTH_KEY env var.
      hostname  (str) — Desired Tailscale hostname. Falls back to REPL_INSTANCE_ID env var.
      command_id (int, optional) — Acknowledged via heartbeat automatically.

    Idempotent: no-op if status is already 'ready' or 'installing'.
    """
    with _lock:
        current = _tailscale_status

    if current in ("ready", "installing"):
        _log("INFO", f"trigger_tailscale: already {current}, no-op")
        return

    auth_key = msg.get("auth_key") or os.getenv("REPL_TAILSCALE_AUTH_KEY", "")
    hostname = msg.get("hostname") or _default_hostname()

    # Run installation in a background thread so SSE dispatch isn't blocked.
    t = threading.Thread(
        target=_install_tailscale,
        args=(auth_key, hostname),
        daemon=True,
        name="tailscale-install",
    )
    t.start()


def _default_hostname() -> str:
    """Build a default hostname from the instance ID env var."""
    instance_id = os.getenv("SKYREPL_INSTANCE_ID") or os.getenv("REPL_INSTANCE_ID", "unknown")
    return f"skyrepl-{instance_id}"


def _install_tailscale(auth_key: str, hostname: str) -> None:
    """
    Full Tailscale installation sequence. Runs in a background thread.

    Steps:
    1. Check for existing installation (idempotent)
    2. Download static binary to _install_dir
    3. Start tailscaled daemon
    4. Wait for daemon socket to be ready
    5. Authenticate with `tailscale up`
    6. Wait for IP assignment
    7. Update module state

    On any failure: set status='failed'.
    """
    global _tailscale_ip, _tailscale_status

    with _lock:
        if _tailscale_status in ("ready", "installing"):
            return
        _tailscale_status = "installing"

    _log("INFO", "Starting JIT Tailscale installation")

    # Step 1: Check for existing installation from a previous run / snapshot
    existing_ip = detect_tailscale_ip()
    if existing_ip:
        _log("INFO", f"Tailscale already installed (IP: {existing_ip}), skipping download")
        return

    try:
        # Step 2: Download and extract static binary
        _download_binary()

        # Step 3: Start tailscaled daemon
        _start_daemon()

        # Step 4: Wait for daemon socket (~10s)
        socket_path = os.path.join(_install_dir, "tailscaled.sock")
        if not _wait_for_socket(socket_path, timeout_s=10):
            raise RuntimeError("tailscaled socket not ready after 10s")

        # Step 5: Authenticate
        if not auth_key:
            raise RuntimeError("No Tailscale auth key provided (set auth_key in command or REPL_TAILSCALE_AUTH_KEY)")

        _authenticate(auth_key, hostname)

        # Step 6: Get assigned IP (poll up to 15s)
        ip = _wait_for_ip(timeout_s=15)
        if not ip:
            raise RuntimeError("Tailscale authenticated but no IP assigned after 15s")

        with _lock:
            _tailscale_ip = ip
            _tailscale_status = "ready"

        _log("INFO", f"Tailscale ready, IP: {ip}")

    except Exception as e:
        _log("ERROR", f"Tailscale installation failed: {e}")
        with _lock:
            _tailscale_status = "failed"


# =============================================================================
# Download
# =============================================================================


def _download_binary() -> None:
    """
    Download and extract Tailscale static binary to _install_dir.

    Skips download if both 'tailscale' and 'tailscaled' already exist in _install_dir.
    """
    ts_bin = os.path.join(_install_dir, "tailscale")
    tsd_bin = os.path.join(_install_dir, "tailscaled")

    if os.path.isfile(ts_bin) and os.path.isfile(tsd_bin):
        _log("INFO", "Tailscale binaries already present, skipping download")
        return

    os.makedirs(_install_dir, exist_ok=True)
    url = _tailscale_url()
    _log("INFO", f"Downloading Tailscale from {url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tgz_path = os.path.join(tmpdir, "tailscale.tgz")

        # Download with 120s timeout (binary is ~25MB)
        urllib.request.urlretrieve(url, tgz_path)

        _log("INFO", "Extracting Tailscale archive")
        with tarfile.open(tgz_path, "r:gz") as tar:
            tar.extractall(tmpdir)

        # Find tailscale and tailscaled in the extracted tree
        found = {"tailscale": None, "tailscaled": None}
        for root, _dirs, files in os.walk(tmpdir):
            for fname in files:
                if fname in found:
                    found[fname] = os.path.join(root, fname)

        for name, src in found.items():
            if src is None:
                raise RuntimeError(f"Binary '{name}' not found in Tailscale archive")
            dst = os.path.join(_install_dir, name)
            shutil.copy2(src, dst)
            os.chmod(dst, 0o755)
            _log("DEBUG", f"Installed {name} -> {dst}")


# =============================================================================
# Daemon
# =============================================================================


def _start_daemon() -> None:
    """
    Start tailscaled in userspace-networking mode.

    Uses state file and socket under _install_dir to avoid needing root or
    writing to system paths.
    """
    global _tailscaled_proc

    tsd_bin = os.path.join(_install_dir, "tailscaled")
    state_path = os.path.join(_install_dir, "state")
    socket_path = os.path.join(_install_dir, "tailscaled.sock")

    with _lock:
        if _tailscaled_proc is not None and _tailscaled_proc.poll() is None:
            _log("INFO", "tailscaled already running")
            return

    _log("INFO", "Starting tailscaled daemon (userspace-networking)")
    proc = subprocess.Popen(
        [
            tsd_bin,
            "--state=" + state_path,
            "--socket=" + socket_path,
            "--tun=userspace-networking",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )

    with _lock:
        _tailscaled_proc = proc

    _log("INFO", f"tailscaled started (pid={proc.pid})")


def _wait_for_socket(socket_path: str, timeout_s: float = 10.0) -> bool:
    """
    Poll for the tailscaled Unix socket to appear, up to timeout_s seconds.
    Returns True when ready, False on timeout.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if os.path.exists(socket_path):
            return True
        time.sleep(0.3)
    return False


# =============================================================================
# Authentication
# =============================================================================


def _authenticate(auth_key: str, hostname: str) -> None:
    """
    Run `tailscale up` to authenticate with the tailnet.

    Raises RuntimeError on non-zero exit code.
    """
    ts_bin = os.path.join(_install_dir, "tailscale")
    socket_path = os.path.join(_install_dir, "tailscaled.sock")

    _log("INFO", f"Authenticating Tailscale as hostname={hostname!r}")
    result = subprocess.run(
        [
            ts_bin,
            "--socket=" + socket_path,
            "up",
            "--authkey=" + auth_key,
            "--hostname=" + hostname,
            "--accept-routes",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    if result.returncode != 0:
        raise RuntimeError(f"tailscale up failed (exit {result.returncode}): {result.stderr.strip()}")

    _log("INFO", "Tailscale authenticated")


def _get_ip() -> Optional[str]:
    """
    Run `tailscale ip -4` and return the 100.x.x.x address, or None.
    """
    ts_bin = os.path.join(_install_dir, "tailscale")
    socket_path = os.path.join(_install_dir, "tailscaled.sock")

    try:
        result = subprocess.run(
            [ts_bin, "--socket=" + socket_path, "ip", "-4"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            ip = result.stdout.strip()
            if ip:
                return ip
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def _wait_for_ip(timeout_s: float = 15.0) -> Optional[str]:
    """
    Poll `tailscale ip -4` until a 100.x.x.x address appears or timeout.
    Returns the IP string or None.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ip = _get_ip()
        if ip and ip.startswith("100."):
            return ip
        time.sleep(1.0)
    return None


# =============================================================================
# Shutdown
# =============================================================================


def tailscale_shutdown() -> None:
    """
    Gracefully shut down Tailscale.

    Sequence:
    1. `tailscale logout` — removes device from tailnet
    2. Kill tailscaled daemon
    3. Clean up _install_dir

    Best-effort: logs errors but never raises.
    Called from agent.py shutdown sequence.
    """
    global _tailscaled_proc

    with _lock:
        status = _tailscale_status

    if status not in ("ready", "installing"):
        return

    # 1. Logout
    ts_bin = os.path.join(_install_dir, "tailscale")
    socket_path = os.path.join(_install_dir, "tailscaled.sock")
    if os.path.isfile(ts_bin):
        try:
            subprocess.run(
                [ts_bin, "--socket=" + socket_path, "logout"],
                capture_output=True,
                timeout=10,
            )
            _log("INFO", "Tailscale logout completed")
        except Exception as e:
            _log("WARN", f"Tailscale logout failed (best-effort): {e}")

    # 2. Kill tailscaled
    with _lock:
        proc = _tailscaled_proc
    if proc is not None:
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
            _log("INFO", "tailscaled stopped")
        except Exception as e:
            _log("WARN", f"Failed to stop tailscaled (best-effort): {e}")

    with _lock:
        _tailscaled_proc = None

    # 3. Clean up temp directory
    try:
        if os.path.isdir(_install_dir):
            shutil.rmtree(_install_dir, ignore_errors=True)
            _log("INFO", f"Cleaned up {_install_dir}")
    except Exception as e:
        _log("WARN", f"Cleanup of {_install_dir} failed (best-effort): {e}")


# =============================================================================
# Logging Utility
# =============================================================================


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [tailscale] {message}")
