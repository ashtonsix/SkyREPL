# CLAUDE.md

## Contents

- **[Project Overview](#project-overview)** — What SkyREPL is and why it exists
- **[Worklogs](#worklogs)** — Task tracking format and short ID conventions
- **[Subagents](#subagents)** — Model selection guide for delegation
- **[Specification](#specification)** — 15-chapter spec with chapter quick reference
- **[Architecture](#architecture)** — Three-tier model: Manifest → Resource → Material
- **[Implementation Status](#implementation-status)** — Current state
- **[Pseudo-Implementation](#pseudo-implementation-v2)** — v2/ directory structure and fidelity levels

---

## Project Overview

SkyREPL is a cloud compute orchestration platform that makes cloud compute access feel as natural as local development. Part of the Sky Computing ecosystem (complementary to SkyPilot), it focuses on lifecycle management and reproducibility for development workflows.

**Target users:** ML researchers and engineers who value reproducibility and cost-efficiency.

**The Core Insight:** Cloud compute orchestration isn't fundamentally a compute problem - it's a lifecycle chaos problem. Everyone can spin up instances. The hard part is tracking what's running, ensuring cleanup on failure, and avoiding cost surprises.

**System Components:**

- Control plane (deployed on VPS)
- CLI client (user interaction)
- Python agent (runs on remote instances)

## Worklogs

For big tasks, create worklogs `WORKLOG_[TASK].txt` with entries for each step.

See existing worklogs for example format when creating a new worklog.

### Worklog Organization

| Location                      | Status | Contents                                |
| ----------------------------- | ------ | --------------------------------------- |
| `WORKLOG_*.txt` (root)        | Hot    | Active work, frequently referenced      |
| `worklogs/WORKLOG_*.txt`      | Cold   | Completed tasks, archived for reference |
| `specification/WORKLOG_*.txt` | —      | Spec-specific history (e.g., revisions) |

**When to archive:** Move to `worklogs/` when worklog status is COMPLETE and no longer actively referenced; when moving prepend "NNN\_" to indicate order of archival, as in "001_WORKLOG\_[TASK].txt". Keep 2-3 hot worklogs at root max.

### Short ID Conventions

Use consistent short IDs throughout worklogs and conversations for easy cross-referencing:

| Type              | Format             | Examples                                         |
| ----------------- | ------------------ | ------------------------------------------------ |
| Questions to user | Q{n} or Q{n.m}     | Q1, Q4.5                                         |
| Spec chapters     | Ch{n}              | Ch8, Ch11                                        |
| Revision rounds   | R{n}               | R1, R2                                           |
| Issues/actions    | R{n}.{Priority}{n} | R2.Cr2 (critical), R2.Md7 (medium), R2.Lo3 (low) |
| Grouped items     | R{n}-{Group}{n}    | R3-A1, R3-B2 (phase A item 1, phase B item 2)    |

**Priority codes:** Cr = Critical, Hi = High, Md = Medium, Lo = Low

**Benefits:**

- Easy to reference in discussion ("see R2.Cr2")
- Trackable across rounds ("R1 → R2 → R3")
- Scannable in worklogs (grep for "R2.Cr" to find all critical items)

## Subagents

You are an orchestrator. You should get most of your work done by spawning subagents:

| Model  | Use For (guideline, not strict)                                       |
| ------ | --------------------------------------------------------------------- |
| haiku  | Grep/search, file finding, basic lookups, simple exploration          |
| sonnet | Multi-file code flow, research requiring synthesis, standard planning |
| opus   | Architectural analysis, nuanced decisions, deep reasoning             |

Default to the lightest model that can handle the task. Dispatch multiple agents in parallel when the work allows.

## Specification

~15,000 lines across 15 chapters. **Don't read spec chapters directly** (except 01_INTRODUCTION.txt, STATUS.txt, WORKLOG_REVISIONS.txt). Spawn subagents to extract what you need.

### Chapter Quick Reference

| Ch  | File                  | Content                                                 |
| --- | --------------------- | ------------------------------------------------------- |
| 01  | 01_INTRODUCTION.txt   | Overview, reading guide (OK to read directly)           |
| 02  | 02_MATERIAL.txt       | Database schemas, RECORDS/OBJECTS/EXTERNAL              |
| 03  | 03_RESOURCES.txt      | Resource types and materialization                      |
| 04  | 04_MANIFEST.txt       | Ownership boundaries and lifecycle                      |
| 05  | 05_ALLOCATIONS.txt    | Run-to-instance bindings, warm pool, state machine      |
| 06  | 06_WORKFLOWS.txt      | DAG execution, 4 patterns, compensation/rollback        |
| 07  | 07_INTENTS.txt        | launch, terminate, snapshot, cleanup operations         |
| 08  | 08_PROVIDERS.txt      | Cloud provider abstraction (AWS/Lambda/RunPod/OrbStack) |
| 09  | 09_API.txt            | HTTP endpoints, two-layer API design, ElysiaJS          |
| 10  | 10_CLI.txt            | Command structure, output formatting                    |
| 11  | 11_AGENT.txt          | Python agent on remote instances                        |
| 12  | 12_ORPHANS.txt        | Cleanup of untracked cloud resources                    |
| 13  | 13_TYPES.txt          | TypeScript type organization                            |
| 14  | 14_IMPLEMENTATION.txt | Rollout phases, MVK definition                          |
| 15  | 15_OPERATIONS.txt     | Observability, billing, deployment                      |
| --  | STATUS.txt            | Current state summary (OK to read directly)             |
| --  | WORKLOG_REVISIONS.txt | Revision history (OK to read directly)                  |

### Recommended Reading Order (for subagents)

1. Material (2) → Resource (3) → Manifest (4) - Foundation
2. Allocation (5) + Workflows (6) in parallel - Execution primitives
3. Intents (7) - Where allocations and workflows combine
4. Provider (8) → API (9) → CLI (10), Agent (11) - Interface layer
5. Orphans (12) → Types (13) → Implementation (14) → Operations (15) - Operational concerns

## Architecture

Three-tier ownership model: **MANIFEST → RESOURCE → MATERIAL**

### Material (data sources)

- **RECORDS**: SQL tables (instances, runs, allocations, workflows)
- **OBJECTS**: Blobs with tags (INTEGER PK, checksum for dedup)
- **EXTERNAL**: Provider state via API (cached in-memory)

### Resources (domain objects)

Instance, Run, Allocation, Workflow, Snapshot, Artifact - materialized from MATERIAL sources.

### Key Concepts

| Concept        | What It Is                                                                                                                                                           |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Allocation** | Run-to-instance binding. States: AVAILABLE → CLAIMED → ACTIVE → HELD → COMPLETE (+ FAILED). Warm pool = `WHERE status='AVAILABLE'`                                   |
| **Manifest**   | Ownership boundary between workflows. States: DRAFT → SEALED. Enables clean failure cleanup.                                                                         |
| **Workflow**   | DAG execution with 4 patterns: Insert-and-Reconverge, Conditional-Branch, Parallel-Fan-Out, Retry-with-Alternative. Each step has compensation actions for rollback. |
| **Intent**     | High-level operation: launch, terminate, snapshot, cleanup. Combines allocations and workflows.                                                                      |

### Providers

**Compute Providers** (create/destroy instances):

- Initial: AWS, Lambda Labs, RunPod, OrbStack (local testing)
- Vision: +GCP, Azure, CoreWeave, Vast.ai, SLURM

**Feature Providers** (attach to instances, cross-cutting concerns):

- Initial: Tailscale (networking/VPN)
- Vision: +WandB, VSCode Server

## Implementation Status

The spec is complete. A pseudo-implementation exists in `v2/`.

## Pseudo-Implementation (v2/)

The `v2/` directory contains a **pseudo-implementation**: real file structure with `.l{n}.txt` suffix indicating fidelity level (e.g., `src/db.ts.l1.txt` for level-1).

### Purpose

Bridge between spec and code. Easier to refactor structure when files are outlines rather than working code.

### Fidelity Levels

Files progress through fidelity levels as implementation matures:

| Level | Extension | Content                | When to Use                           |
| ----- | --------- | ---------------------- | ------------------------------------- |
| 1     | `.l1.txt` | Bullet-point outlines  | Structure exploration, early planning |
| 2     | `.l2.txt` | Detailed pseudocode    | Algorithm design, interface contracts |
| 3     | `.l3.txt` | Partial implementation | Core logic, stubbed dependencies      |
| 4     | (none)    | Working code           | Remove suffix entirely                |

### Working with Pseudo-Implementation

- **To understand structure:** Read `v2/` files directly (they're short)
- **To implement:** Pick a file, increase fidelity level, eventually remove suffix
- **To refactor:** Easier to move/rename/restructure while still pseudo-impl
- **Spec is source of truth:** If pseudo-impl contradicts spec, spec wins
