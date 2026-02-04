# CLAUDE.md

## Project Overview

SkyREPL is a cloud compute orchestration platform for lifecycle management and reproducibility. Part of the Sky Computing ecosystem (complementary to SkyPilot).

**Target users:** ML researchers and engineers who value reproducibility and cost-efficiency.

**The Core Insight:** Cloud orchestration isn't a compute problem—it's a lifecycle chaos problem. Tracking what's running, ensuring cleanup on failure, avoiding cost surprises.

**Components:** Control plane (VPS), CLI client, Python agent (on instances).

## Subagents

You are an orchestrator. Delegate work via subagents:

| Type    | Use For                                                                   |
| ------- | ------------------------------------------------------------------------- |
| opus    | Default. Complex analysis, architectural decisions, ambiguous tasks       |
| sonnet  | Bounded multi-file tasks with clear scope (e.g. "check all callers of X") |
| haiku   | Single-file lookups, simple grep/search, mechanical extraction            |
| explore | Codebase navigation and discovery (uses subagent_type=Explore)            |

Default to opus. Drop down when confident a lighter model handles the task. Dispatch multiple agents in parallel when possible.

## Specification

~15,000 lines across 15 chapters in `specification/`.

**Don't read spec chapters directly** (except 01_INTRODUCTION.txt). Spawn subagents to extract what you need.

Quick reference:

- Ch01-04: Foundation (Introduction, Material, Resources, Manifest)
- Ch05-07: Execution (Allocations, Workflows, Intents)
- Ch08-11: Interface (Providers, API, CLI, Agent)
- Ch12-15: Operations (Orphans, Types, Implementation, Operations)

## Architecture

Three-tier model: **MANIFEST → RESOURCE → MATERIAL**

**Material** (data sources): RECORDS (SQL), OBJECTS (blobs), EXTERNAL (provider APIs)

**Resources** (domain objects): Instance, Run, Allocation, Workflow, Snapshot, Artifact

**Key concepts:**

- **Allocation**: Run-to-instance binding. 5 states: AVAILABLE → CLAIMED → ACTIVE → COMPLETE (+ FAILED)
- **Manifest**: Ownership boundary. States: DRAFT → SEALED
- **Workflow**: DAG execution with 4 patterns and compensation/rollback
- **Warm pool**: Allocations WHERE status='AVAILABLE'

## Pseudo-Implementation (v2/)

The `v2/` directory contains pseudo-implementation with fidelity suffixes:

| Level | Suffix    | Content                |
| ----- | --------- | ---------------------- |
| 1     | `.l1.txt` | Bullet-point outlines  |
| 2     | `.l2.txt` | Detailed pseudocode    |
| 3     | `.l3.txt` | Partial implementation |
| 4     | (none)    | Working code           |

**Source of truth hierarchy:** Recent WORKLOG > spec > pseudo-impl. (Active worklog may contain changes not yet propagated to spec.)

Structure: `control/`, `cli/`, `shared/`, `agent/`, `tests/`

## Worklogs

For substantial tasks, create `WORKLOG_[TASK].txt` with entries for each step.

**Organization:**

- `WORKLOG_*.txt` (root): Active/hot work
- `worklogs/NNN_WORKLOG_*.txt`: Archived/cold (numbered by archive order)

Keep 2-3 hot worklogs max. Archive when complete.

**Short IDs for cross-referencing:**

- Questions: Q1, Q2
- Spec chapters: Ch8, Ch11
- Revision rounds: R1, R2
- Issues: R2.Cr1 (critical), R2.Md3 (medium), R2.Lo5 (low)
- Archived worklogs: Wl1, Wl2, Wl3

## Current Status

See `STATUS.txt` for project status, spec completion, and v2/ progress.
