#!/usr/bin/env python3
"""
Documentation link validator for SkyREPL.

NOTE: This script was originally designed for INDEX.md, which provided
comprehensive reading paths for all zone docs. After the doc overhaul,
README.md became a minimal overview that delegates to zone-specific docs
rather than linking every doc file. The coverage check is no longer
meaningful with the new architecture.

This script validates that links in README.md resolve, but does NOT
check that every doc file is linked (intentionally incomplete coverage).

Exit code 0 = all valid, 1 = errors found.
"""

import re
import sys
from pathlib import Path

# Zone docs directories to check for coverage
DOC_DIRS = [
    "docs",
    "docs/providers",
]

# Files that are linked from INDEX.md but aren't in zone docs/
# (root files, workshop files, spec dir, etc.) — no coverage check needed
COVERAGE_EXEMPT_PATTERNS = [
    "README.md",
    "ARCHITECTURE.txt",
    "scaffold/workshop/",
]


def find_repo_root():
    p = Path(__file__).resolve().parent
    while p != p.parent:
        if (p / "lifecycle").is_dir():
            return p
        p = p.parent
    sys.exit("Could not find repo root (no lifecycle/ directory found)")


def extract_links(readme_path):
    """Extract all markdown link targets from README.md."""
    text = readme_path.read_text()
    # Match [text](path) — extract path part
    return re.findall(r'\[.*?\]\(([^)]+)\)', text)


def check_links_resolve(root, links):
    """Verify every link target exists as a file or directory."""
    errors = []
    for link in links:
        target = root / link
        if not target.exists():
            errors.append(f"  README.md: Link target not found: {link}")
    return errors


def check_coverage(root, links):
    """
    Verify every doc file in zone dirs appears in README.md.

    NOTE: Disabled after README.md replaced INDEX.md. The new README is
    intentionally minimal and delegates to zone docs rather than linking
    every file. Coverage checks are no longer applicable.
    """
    # Coverage check disabled - README.md is intentionally minimal
    return []


def main():
    root = find_repo_root()
    readme_path = root / "README.md"

    if not readme_path.exists():
        print("README.md not found at repo root")
        sys.exit(1)

    links = extract_links(readme_path)
    # Deduplicate while preserving order
    seen = set()
    unique_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)

    errors = []
    errors.extend(check_links_resolve(root, unique_links))
    errors.extend(check_coverage(root, unique_links))

    print(f"Checked: {len(unique_links)} unique links")
    print("(Coverage check disabled - README.md is intentionally minimal)")

    if errors:
        print(f"\n{len(errors)} DOC-LINK ERROR(S):")
        for e in errors:
            print(e)
        sys.exit(1)
    else:
        print("All doc links valid.")
        sys.exit(0)


if __name__ == "__main__":
    main()
