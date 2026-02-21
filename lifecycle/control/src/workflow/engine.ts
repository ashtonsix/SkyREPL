// workflow/engine.ts - Re-export barrel preserving original import paths
// All implementation has moved to workflow/engine/ directory.
// This file exists solely so that existing imports like:
//   import { ... } from "./workflow/engine"
//   import { ... } from "../../control/src/workflow/engine"
// continue to resolve without any changes.

export * from "./engine/index";
