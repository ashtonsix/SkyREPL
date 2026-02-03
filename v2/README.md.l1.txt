# SkyREPL v2

## Overview
Cloud compute orchestration with lifecycle management and reproducibility.

## Architecture
Three-tier: MANIFEST → RESOURCE → MATERIAL

## Components
- Control plane (TypeScript/ElysiaJS)
- CLI (TypeScript)
- Agent (Python, stdlib-only)

## Key Concepts
- Allocation: Run-to-instance binding (AVAILABLE → CLAIMED → ACTIVE → COMPLETE)
- Manifest: Ownership boundary (DRAFT → SEALED)
- Workflow: DAG execution with 4 patterns
- Debug Hold: Optional timestamp on COMPLETE allocations to preserve for debugging

## Providers
AWS, Lambda Labs, RunPod, OrbStack (local testing)
