# CLAUDE.md

## Project

SkyREPL: cloud compute orchestration for lifecycle management and reproducibility. Control plane (VPS) + CLI client + Python agent (on instances).

**Current phase:** L3 Slice 2 complete + Intermission done. 282 tests pass (0 failures). Slice 3 not started. Backlog: `BACKLOG.txt`.

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

~9,500 lines across 15 chapters in `spec/`. **Don't read spec chapters directly** — spawn subagents to extract what you need.

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

Two directories:

- **`impl/`** — L3 working code (Bun workspace monorepo). Real `.ts`/`.py` files. Slices 1-2 + intermission complete.
- **`impl-pseudo/`** — L1/L2 reference pseudocode (154 files). Stale files excised; 16 marked "STILL USEFUL"; ~39 unimplemented reference preserved.

Structure (both dirs): `control/`, `cli/`, `shared/`, `agent/`, `tests/`

Stack: Bun runtime, bun:sqlite (WAL), ElysiaJS, TypeBox, Python stdlib-only agent.

**Source of truth:** Recent WORKLOG > spec > impl and impl-pseudo tied.

## Worklogs

`WORKLOG_*.txt` (root) for active work. `worklogs/NNN_WORKLOG_*.txt` for archived.

Short IDs: Ch8 (spec chapter), 007 (archived worklog), R2.Cr1 (issue severity).
