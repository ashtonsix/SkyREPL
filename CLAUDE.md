# CLAUDE.md

## Project

SkyREPL: cloud compute orchestration for lifecycle management and reproducibility. Control plane (VPS) + CLI client + Python agent (on instances).

**Current phase:** Post-MVK, parallel epic development. See `tasks/ROADMAP.txt`.

## Dispatch

You are an orchestrator. Delegate via subagents. Dispatch multiple agents in parallel when possible; use `run_in_background` for long-running ones.

| Task Profile                | Model   | Effort   |
| --------------------------- | ------- | -------- |
| Architectural / ambiguous   | opus    | high/max |
| Bounded multi-file analysis | opus    | medium   |
| Mechanical multi-file edits | sonnet  | —        |
| Single-file lookups         | haiku   | —        |
| Codebase navigation         | explore | —        |

Effort is global (`/effort`), not per-subagent. Default: opus high. Opus 4.6 overthinks simple tasks — use medium for routine dispatch, reserve high/max for hard problems.

Agent teams (experimental): multiple independent sessions with shared task lists + messaging. Use only when workers need to discuss/challenge each other (not for parallel independent work — subagents suffice).

## Specification

15 chapters in `spec/`. **Don't read spec chapters directly** — spawn subagents to extract what you need.

- Ch01-04: Foundation (Introduction, Material, Resources, Manifest)
- Ch05-07: Execution (Allocations, Workflows, Intents)
- Ch08-11: Interface (Providers, API, CLI, Agent)
- Ch12-15: Operations (Orphans, Types, Implementation, Operations)

Spec is normative (MUST/SHOULD/MAY). **No-regrowth policy:** new non-normative content goes to `docs/` or `workshop/`, not `spec/`. Spec gets a one-line pointer ("See docs/reference/X.txt"), never the content itself.

## Architecture

Three-tier: **MANIFEST → RESOURCE → MATERIAL**

- **Material**: RECORDS (SQL), OBJECTS (blobs), EXTERNAL (provider APIs)
- **Resources**: Instance, Run, Allocation, Workflow, Snapshot, Artifact
- **Allocation**: Run-to-instance binding. States: AVAILABLE → CLAIMED → ACTIVE → COMPLETE (+ FAILED)
- **Manifest**: Ownership boundary. States: DRAFT → SEALED
- **Workflow**: DAG execution with 4 patterns and compensation/rollback
- **Warm pool**: Allocations WHERE status='AVAILABLE'

## Code Layout

- **`impl/`** — L3 working code (Bun workspace monorepo). Structure: `control/`, `cli/`, `shared/`, `agent/`, `tests/`.
- **`impl-pseudo/`** — L1/L2 reference pseudocode. Subsumed as epics complete (tracked in `tasks/ROADMAP.txt`).
- **`tasks/`** — Project management: epics, worklogs, roadmap, conventions. See `tasks/CONVENTIONS.txt` for task ID format and way of working.

Stack: Bun runtime, bun:sqlite (WAL), ElysiaJS, TypeBox, Python stdlib-only agent.

CLI entry point: `impl/cli/bin/repl` (added to PATH via `~/.zshrc`). Run `repl` from anywhere.

**Source of truth:** Recent worklog > spec > impl and impl-pseudo tied.

## Testing

Mix of automated and interactive whole-system testing:

- **Automated:** `bun test` (unit + integration + simulated E2E). Near-comprehensive but slow (~2 min).
- **Interactive:** `demo/` project with `repl.toml` profiles for breadth-first human/agent exploration. Catches friction and qualitative issues that automated tests miss.

**Never run the full suite multiple times to grep different things.** Run once, write scrollback to disk, then read the file:
```bash
bun test 2>&1 | tee /tmp/skyrepl-test-output.txt
```
Then use Read/Grep on `/tmp/skyrepl-test-output.txt` for any analysis. The preload (`tests/preload-quiet.ts`) redirects console output to `/tmp/skyrepl-test-detail.log` and a shim (`tests/bun-test-shim.ts`) interleaves per-test `▶`/`◀` markers — useful for attributing log lines to tests and finding slow/hanging ones.

## OrbStack IP Exhaustion

If `orbctl create` fails with "missing IP address", do NOT run the recovery script yourself. Ask the user to exit this Claude session, run `bash scripts/orbstack-reset-networking.sh` from the macOS host, then resume.

## Task Tracking

See `tasks/CONVENTIONS.txt` for full details. Quick reference:

- **Task IDs:** `#EPIC-NN` (e.g., `#WF-01`, `#LIFE-07`). Defined in `tasks/epics/*.txt`.
- **Epics:** FND, LIFE, WF, AGENT, SNAP, API, CLI, ORPH, PROV, DX.
- **Worklogs:** hot at repo root, archived to `tasks/archive/`. Numbering from 018.
- **Cross-refs:** validated by `scripts/check-crossrefs.py` (task IDs + spec anchors).
