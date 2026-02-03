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
- Allocation: Run-to-instance binding (AVAILABLE → CLAIMED → ACTIVE → HELD → COMPLETE)
- Manifest: Ownership boundary (DRAFT → SEALED)
- Workflow: DAG execution with 4 patterns

## Providers
AWS, Lambda Labs, RunPod, OrbStack (local testing)
