#!/usr/bin/env python3
"""
Cross-reference integrity checker for SkyREPL.

Finds every §Anchor in the codebase. If a filepath precedes it on the
same line, validates file exists + anchor resolves. Enforces §CamelCase
(no spaces in L2 section headings).

Also validates file-only refs (`See docs/...`, `See workshop/...`).

Exit code 0 = all valid, 1 = errors found.
"""

import re
import sys
from pathlib import Path

DIRS_TO_SCAN = ["lifecycle", "shell", "orbital", "scaffold", "contracts"]
EXCLUDE_SUBDIRS = {"scaffold/tasks/archive"}


def find_repo_root():
    p = Path(__file__).resolve().parent
    while p != p.parent:
        if (p / "lifecycle").is_dir():
            return p
        p = p.parent
    sys.exit("Could not find repo root (no lifecycle/ directory found)")


def is_excluded(filepath: Path, root: Path) -> bool:
    """Check if filepath falls under an excluded subdirectory."""
    rel = str(filepath.relative_to(root))
    return any(rel.startswith(p + "/") or rel == p for p in EXCLUDE_SUBDIRS)


def build_heading_registry(root: Path) -> dict[str, set[str]]:
    """Build {abs_filepath: {heading_names}} for all .txt files."""
    registry = {}
    heading_re = re.compile(r"^##\s+§?(.+)")

    for dirname in DIRS_TO_SCAN:
        dirpath = root / dirname
        if not dirpath.exists():
            continue
        for filepath in dirpath.rglob("*.txt"):
            if is_excluded(filepath, root):
                continue
            try:
                content = filepath.read_text()
            except (UnicodeDecodeError, PermissionError):
                continue
            anchors = set()
            for line in content.splitlines():
                m = heading_re.match(line)
                if m:
                    anchors.add(m.group(1).strip())
            if anchors:
                registry[str(filepath)] = anchors

    return registry


def scan_crossrefs(root: Path):
    """Find all §-references and file-only references."""
    refs = []

    # §Anchor — with optional preceding filepath (exclude §-prefixed strings as filepaths)
    ref_re = re.compile(r"(?:([^\s§]+\.[^\s§]+)\s+)?§(\S+)")

    # File-only: See docs/... or See workshop/... or lifecycle/... etc.
    file_only_re = re.compile(r"(?:^|[\s(])See\s+((?:lifecycle|shell|orbital|scaffold|contracts|docs|workshop)/\S+\.txt)")

    for dirname in DIRS_TO_SCAN:
        dirpath = root / dirname
        if not dirpath.exists():
            continue
        for filepath in sorted(dirpath.rglob("*.txt")):
            if is_excluded(filepath, root):
                continue
            try:
                lines = filepath.read_text().splitlines()
            except (UnicodeDecodeError, PermissionError):
                continue

            rel_source = str(filepath.relative_to(root))

            for lineno, line in enumerate(lines, 1):
                # Skip heading definitions
                if line.lstrip().startswith("## "):
                    continue

                for m in ref_re.finditer(line):
                    target_file = m.group(1)
                    anchor = m.group(2).rstrip(".,;:)")
                    if target_file:
                        target_file = target_file.rstrip(":,;")
                    refs.append({
                        "source": rel_source,
                        "line": lineno,
                        "target_file": target_file,
                        "anchor": anchor,
                        "type": "anchored" if target_file else "bare",
                    })

                for m in file_only_re.finditer(line):
                    refs.append({
                        "source": rel_source,
                        "line": lineno,
                        "target_file": m.group(1),
                        "anchor": None,
                        "type": "file_ref",
                    })

    return refs


def resolve_target(root: Path, ref: dict) -> Path | None:
    """Resolve a target file path, trying absolute then relative with .l2.txt suffix."""
    target_path = ref["target_file"]
    target = root / target_path
    if target.exists():
        return target

    # Relative path: resolve from source directory
    if "/" not in target_path:
        source_dir = (root / ref["source"]).parent
        for candidate in [target_path, target_path + ".l2.txt"]:
            alt = source_dir / candidate
            if alt.exists():
                return alt

    return None


NUMBERED_RE = re.compile(r"^\d+\.\d+")


def anchor_matches(anchor: str, anchors: set[str]) -> bool:
    """Check if anchor matches any heading — exact, then prefix for numbered refs.

    Numbered refs (§15.1, §9.6.4) prefix-match against headings like "15.1 Observability".
    Range refs (§15.1-15.4) use the start number. Sub-sections (§9.6.4) fall back to
    parent sections (9.6, then 9) when sub-heading doesn't exist.
    """
    if anchor in anchors:
        return True

    if not NUMBERED_RE.match(anchor):
        return False

    prefix = anchor.split("-")[0]  # "15.1-15.4" → "15.1"

    # Try exact prefix, then progressively strip last .N segment
    while prefix:
        for heading in anchors:
            if heading.startswith(prefix):
                return True
        # "9.6.4" → "9.6" → "9"
        if "." in prefix:
            prefix = prefix.rsplit(".", 1)[0]
        else:
            break

    return False


def validate_crossrefs(root: Path, refs: list, registry: dict):
    """Validate anchored + file-only references."""
    errors = []

    for ref in refs:
        if ref["type"] == "bare":
            continue  # Chapter/worklog refs (Ch6 §B5) — no file to validate

        target = resolve_target(root, ref)
        if target is None:
            errors.append({**ref, "error": f'File not found: {ref["target_file"]}'})
            continue

        if ref["anchor"]:
            anchors = registry.get(str(target), set())
            if not anchor_matches(ref["anchor"], anchors):
                errors.append({
                    **ref,
                    "error": f'Anchor not found: §{ref["anchor"]} in {ref["target_file"]}',
                })

    return errors


def check_camelcase_convention(root: Path, registry: dict):
    """Check that all L2 headings use CamelCase (no spaces)."""
    warnings = []
    for filepath_str, anchors in sorted(registry.items()):
        if ".l2." not in filepath_str:
            continue
        rel = str(Path(filepath_str).relative_to(root))
        for name in sorted(anchors):
            if " " in name:
                warnings.append(f"  {rel}: §{name}")
    return warnings


# --- Task ID cross-reference validation ---

TASK_ID_DEF_RE = re.compile(r"^## (#[A-Z]+-\d+):")
TASK_ID_REF_RE = re.compile(r"#[A-Z]+-\d+\b")
TASK_STATUS_RE = re.compile(r"\[CUT\]")


def build_task_registry(root: Path) -> dict[str, dict]:
    """Build {task_id: {file, cut}} from scaffold/tasks/epics/*.txt, archived epics, and scaffold/tasks/BACKLOG.txt."""
    registry = {}
    task_files = []
    epics_dir = root / "scaffold" / "tasks" / "epics"
    if epics_dir.exists():
        task_files.extend(sorted(epics_dir.glob("*.txt")))
    archived_epics_dir = root / "scaffold" / "tasks" / "archive" / "epics"
    if archived_epics_dir.exists():
        task_files.extend(sorted(archived_epics_dir.glob("*.txt")))
    backlog = root / "scaffold" / "tasks" / "BACKLOG.txt"
    if backlog.exists():
        task_files.append(backlog)
    archived_backlog = root / "scaffold" / "tasks" / "archive" / "BACKLOG_RESOLVED.txt"
    if archived_backlog.exists():
        task_files.append(archived_backlog)
    archived_misc_backlog = root / "scaffold" / "tasks" / "archive" / "misc" / "BACKLOG_RESOLVED.txt"
    if archived_misc_backlog.exists():
        task_files.append(archived_misc_backlog)
    for filepath in task_files:
        try:
            lines = filepath.read_text().splitlines()
        except (UnicodeDecodeError, PermissionError):
            continue
        rel = str(filepath.relative_to(root))
        for line in lines:
            m = TASK_ID_DEF_RE.match(line)
            if m:
                task_id = m.group(1)
                is_cut = bool(TASK_STATUS_RE.search(line))
                registry[task_id] = {"file": rel, "cut": is_cut}
    return registry


def scan_task_refs(root: Path) -> list[dict]:
    """Find all #EPIC-NN references (excluding their definitions)."""
    refs = []
    for dirname in DIRS_TO_SCAN:
        dirpath = root / dirname
        if not dirpath.exists():
            continue
        for filepath in sorted(dirpath.rglob("*.txt")):
            if is_excluded(filepath, root):
                continue
            try:
                lines = filepath.read_text().splitlines()
            except (UnicodeDecodeError, PermissionError):
                continue
            rel = str(filepath.relative_to(root))
            for lineno, line in enumerate(lines, 1):
                # Skip task ID definitions (## #WF-01: ...)
                if TASK_ID_DEF_RE.match(line):
                    continue
                for m in TASK_ID_REF_RE.finditer(line):
                    refs.append({
                        "source": rel,
                        "line": lineno,
                        "task_id": m.group(0),
                    })
    return refs


def validate_task_refs(task_refs: list, task_registry: dict) -> list[dict]:
    """Validate task ID references resolve and aren't dead."""
    errors = []
    for ref in task_refs:
        tid = ref["task_id"]
        if tid not in task_registry:
            errors.append({**ref, "error": f"Task not found: {tid}"})
        elif task_registry[tid]["cut"]:
            # References to CUT tasks are informational, not errors
            pass
    return errors


def main():
    root = find_repo_root()
    registry = build_heading_registry(root)
    refs = scan_crossrefs(root)
    errors = validate_crossrefs(root, refs, registry)
    convention_warnings = check_camelcase_convention(root, registry)

    # Task ID validation
    task_registry = build_task_registry(root)
    task_refs = scan_task_refs(root)
    task_errors = validate_task_refs(task_refs, task_registry)

    anchored = sum(1 for r in refs if r["type"] == "anchored")
    bare = sum(1 for r in refs if r["type"] == "bare")
    file_ref = sum(1 for r in refs if r["type"] == "file_ref")

    print(f"Scanned: {anchored} anchored, {bare} bare, {file_ref} file-only, {len(task_refs)} task-refs")
    print(f"Task registry: {len(task_registry)} tasks defined in {len(set(t['file'] for t in task_registry.values()))} epics")

    exit_code = 0

    if errors:
        print(f"\n{len(errors)} CROSS-REF ERROR(S):")
        for e in errors:
            print(f'  {e["source"]}:{e["line"]}: {e["error"]}')
        exit_code = 1

    if task_errors:
        print(f"\n{len(task_errors)} TASK-REF ERROR(S):")
        for e in task_errors:
            print(f'  {e["source"]}:{e["line"]}: {e["error"]}')
        exit_code = 1

    if convention_warnings:
        print(f"\n{len(convention_warnings)} CONVENTION WARNING(S) (spaces in L2 headings):")
        for w in convention_warnings:
            print(w)
        exit_code = 1

    if exit_code == 0:
        print("All cross-references valid. Convention clean.")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
