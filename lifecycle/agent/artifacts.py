"""
Artifact Collection (#AGENT-07)

After run completion, scans workdir for files matching artifact patterns.
Uploads matched files to control plane blob storage.

Best-effort: log failures, never fail the run for artifact issues.
"""

from __future__ import annotations

import glob
import hashlib
import http.client
import json
import os
import time
import urllib.error
import urllib.request
from typing import List, Optional
from urllib.parse import urlparse

from http_client import http_post, get_control_plane_url

# Maximum single artifact size for upload (50MB)
MAX_ARTIFACT_SIZE = 50 * 1024 * 1024

# Maximum number of artifacts per run
MAX_ARTIFACTS_PER_RUN = 100


def collect_and_upload(run_id: int, workdir: str, patterns: List[str]) -> int:
    """
    Collect artifacts matching patterns and upload to control plane.

    Args:
        run_id: Run that produced these artifacts
        workdir: Working directory to scan
        patterns: Glob patterns (e.g., ["output/**", "*.csv"])

    Returns:
        Number of artifacts successfully uploaded
    """
    if not patterns:
        return 0

    matched = _find_matching_files(workdir, patterns)
    if not matched:
        _log("INFO", f"No artifacts found for run {run_id}")
        return 0

    _log("INFO", f"Found {len(matched)} artifacts for run {run_id}")

    # Prepare artifact entries
    artifacts = []
    for path in matched:
        if len(artifacts) >= MAX_ARTIFACTS_PER_RUN:
            _log("WARN", f"Artifact limit reached ({MAX_ARTIFACTS_PER_RUN}), skipping remaining")
            break

        entry = _prepare_artifact(workdir, path)
        if entry:
            artifacts.append(entry)

    if not artifacts:
        return 0

    # Upload one artifact at a time
    uploaded = 0
    for artifact in artifacts:
        if _upload_single(run_id, artifact):
            uploaded += 1

    _log("INFO", f"Uploaded {uploaded}/{len(artifacts)} artifacts for run {run_id}")
    return uploaded


def _find_matching_files(workdir: str, patterns: List[str]) -> List[str]:
    """Find files in workdir matching any of the glob patterns."""
    matched: set = set()

    for pattern in patterns:
        # Resolve pattern relative to workdir
        full_pattern = os.path.join(workdir, pattern)
        for path in glob.glob(full_pattern, recursive=True):
            if os.path.isfile(path):
                # Store relative to workdir
                rel_path = os.path.relpath(path, workdir)
                matched.add(rel_path)

    # Sort for deterministic ordering
    return sorted(matched)


def _prepare_artifact(workdir: str, rel_path: str) -> Optional[dict]:
    """Prepare a single artifact entry with checksum and raw bytes."""
    full_path = os.path.join(workdir, rel_path)

    try:
        size = os.path.getsize(full_path)
        if size > MAX_ARTIFACT_SIZE:
            _log("WARN", f"Artifact too large, skipping: {rel_path} ({size} bytes)")
            return None

        with open(full_path, "rb") as f:
            data = f.read()

        checksum = hashlib.sha256(data).hexdigest()

        return {
            "path": rel_path,
            "checksum": checksum,
            "size_bytes": size,
            "_workdir": workdir,  # Internal, not sent to server
            "_data": data,  # Keep raw bytes, not base64
        }

    except Exception as e:
        _log("WARN", f"Failed to prepare artifact {rel_path}: {e}")
        return None


def _put_binary(url: str, data: bytes, timeout: int = 30) -> None:
    """PUT raw binary data to a control plane URL (with auth header)."""
    from http_client import _control_plane_url, _auth_token  # type: ignore[attr-defined]

    parsed_base = urlparse(_control_plane_url)
    parsed_url = urlparse(url)

    # Determine host/port from the URL itself (may be relative path or full URL)
    host = parsed_url.hostname or parsed_base.hostname
    port = parsed_url.port or parsed_base.port
    scheme = parsed_url.scheme or parsed_base.scheme
    path = parsed_url.path
    if parsed_url.query:
        path = f"{path}?{parsed_url.query}"

    if scheme == "https":
        conn = http.client.HTTPSConnection(host, port or 443, timeout=timeout)
    else:
        conn = http.client.HTTPConnection(host, port or 80, timeout=timeout)

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(data)),
    }
    if _auth_token:
        headers["Authorization"] = f"Bearer {_auth_token}"

    try:
        conn.request("PUT", path, body=data, headers=headers)
        resp = conn.getresponse()
        resp.read()
        if resp.status not in (200, 201, 204):
            raise RuntimeError(f"PUT to control plane failed: HTTP {resp.status}")
    finally:
        conn.close()


def _put_binary_external(url: str, data: bytes, timeout: int = 60) -> None:
    """PUT raw binary data to an external presigned URL (no auth header)."""
    req = urllib.request.Request(
        url,
        data=data,
        method="PUT",
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(data)),
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        resp.read()
        if resp.status not in (200, 201, 204):
            raise RuntimeError(f"PUT to external URL failed: HTTP {resp.status}")


def _upload_single(run_id: int, artifact: dict) -> bool:
    """Upload a single artifact via presigned URL flow."""
    try:
        # Step 1: Negotiate upload URL
        negotiate_resp = http_post("/v1/agent/blobs/upload-url", {
            "checksum": artifact["checksum"],
            "size_bytes": artifact["size_bytes"],
            "run_id": run_id,
        }, timeout=10)
        negotiate_data = json.loads(negotiate_resp.read())

        if "error" in negotiate_data:
            _log("WARN", f"Upload URL negotiation failed: {negotiate_data['error']}")
            return False

        blob_id = negotiate_data["blob_id"]
        upload_url = negotiate_data["url"]
        is_inline = negotiate_data.get("inline", False)

        # Step 2: Upload data
        raw_data = artifact["_data"]

        if is_inline:
            # PUT to control plane inline endpoint (with auth)
            _put_binary(upload_url, raw_data, timeout=30)
        elif upload_url.startswith("/"):
            # Relative URL = control plane proxy endpoint (with auth)
            _put_binary(upload_url, raw_data, timeout=60)
        else:
            # Absolute URL = external presigned URL (no auth)
            try:
                _put_binary_external(upload_url, raw_data, timeout=60)
            except urllib.error.HTTPError as e:
                expired = e.code == 403 or (
                    e.code == 400 and "expired" in (e.read() or b"").decode("utf-8", errors="replace").lower()
                )
                if not expired:
                    raise
                _log("WARN", f"Presigned URL expired for {artifact['path']}, re-negotiating...")
                reneg_resp = http_post("/v1/agent/blobs/upload-url", {
                    "checksum": artifact["checksum"],
                    "size_bytes": artifact["size_bytes"],
                    "run_id": run_id,
                }, timeout=10)
                reneg_data = json.loads(reneg_resp.read())
                if "error" in reneg_data:
                    _log("WARN", f"Re-negotiation failed: {reneg_data['error']}")
                    return False
                blob_id = reneg_data["blob_id"]
                upload_url = reneg_data["url"]
                _put_binary_external(upload_url, raw_data, timeout=60)

        # Step 3: Confirm upload
        confirm_resp = http_post("/v1/agent/blobs/confirm", {
            "blob_id": blob_id,
            "checksum": artifact["checksum"],
        }, timeout=10)
        confirm_data = json.loads(confirm_resp.read())

        if not confirm_data.get("confirmed"):
            _log("WARN", f"Upload confirmation failed for {artifact['path']}")
            return False

        # Step 4: Register artifact (no content_base64)
        register_resp = http_post("/v1/agent/artifacts", {
            "run_id": run_id,
            "path": artifact["path"],
            "checksum": artifact["checksum"],
            "size_bytes": artifact["size_bytes"],
            "blob_id": blob_id,
        }, timeout=10)
        register_data = json.loads(register_resp.read())

        return register_data.get("ack", False)

    except Exception as e:
        _log("WARN", f"Presigned upload failed for {artifact['path']}: {e}")
        return False


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [artifacts] {message}")
