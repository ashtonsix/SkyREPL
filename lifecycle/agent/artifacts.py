"""
Artifact Collection (#AGENT-07)

After run completion, scans workdir for files matching artifact patterns.
Uploads matched files to control plane blob storage.

Best-effort: log failures, never fail the run for artifact issues.
"""

from __future__ import annotations

import base64
import glob
import hashlib
import json
import os
import time
from typing import List, Optional

from http_client import http_post

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
    """Prepare a single artifact entry with checksum and content."""
    full_path = os.path.join(workdir, rel_path)

    try:
        size = os.path.getsize(full_path)
        if size > MAX_ARTIFACT_SIZE:
            _log("WARN", f"Artifact too large, skipping: {rel_path} ({size} bytes)")
            return None

        with open(full_path, "rb") as f:
            data = f.read()

        checksum = hashlib.sha256(data).hexdigest()
        content_b64 = base64.b64encode(data).decode("ascii")

        return {
            "path": rel_path,
            "checksum": checksum,
            "size_bytes": size,
            "content_base64": content_b64,
        }

    except Exception as e:
        _log("WARN", f"Failed to prepare artifact {rel_path}: {e}")
        return None


def _upload_single(run_id: int, artifact: dict) -> bool:
    """Upload a single artifact to POST /v1/agent/artifacts. Returns True if stored."""
    payload = {
        "run_id": run_id,
        "path": artifact["path"],
        "checksum": artifact["checksum"],
        "size_bytes": artifact["size_bytes"],
        "content_base64": artifact["content_base64"],
    }

    try:
        resp = http_post("/v1/agent/artifacts", payload, timeout=30)
        resp.read()
        if resp.status == 200:
            return True
        else:
            _log("WARN", f"Artifact upload failed for {artifact['path']}: HTTP {resp.status}")
            return False
    except Exception as e:
        _log("WARN", f"Artifact upload error for {artifact['path']}: {e}")
        return False


def _log(level: str, message: str) -> None:
    """Internal logging (not sent to control plane)."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [artifacts] {message}")
