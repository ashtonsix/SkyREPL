// resource/index.ts - Materialization Barrier Barrel
//
// The recommended import path for business code. Re-exports materializers,
// write functions, and key utilities from all resource modules.
//
// NOT exported: getInstanceRecordRaw, getRunRecordRaw — raw read functions
// bypass the materialization boundary and are for infrastructure code only.
// If you find yourself wanting to import them, consider materializeInstance
// or materializeRun first.

// --- Instance ---
export {
  materializeInstance,
  materializeInstanceBatch,
  createInstanceRecord,
  updateInstanceRecord,
  listInstanceRecords,
  isInstanceHealthy,
  getInstancesByProvider,
  getActiveInstances,
  getStaleInstances,
  updateHeartbeat,
  detectStaleHeartbeats,
  heartbeatTimeoutCheck,
  isTerminalState,
} from "./instance";

// --- Run ---
export {
  materializeRun,
  materializeRunBatch,
  createRunRecord,
  updateRunRecord,
  listRunRecords,
  getActiveRuns,
  getRunsByInstance,
  isRunInProgress,
  getRunDuration,
} from "./run";

// --- Allocation ---
export {
  materializeAllocation,
  materializeAllocationBatch,
  ALLOCATION_TRANSITIONS,
  validateAllocationTransition,
  allowsSSHAccess,
  isInWarmPool,
  createAvailableAllocation,
  queryWarmPool,
  claimWarmPoolAllocation,
  transitionToComplete,
  checkReplenishmentEligibility,
  extendAllocationDebugHold,
  canTerminateInstance,
  reconcileClaimedAllocations,
  holdExpiryTask,
  warmPoolReconciliation,
} from "./allocation";

// --- Manifest ---
export {
  materializeManifest,
  materializeManifestBatch,
  sealManifestWithPolicy,
  claimResourceAtomic,
  claimWarmInstance,
  parentClaimSubworkflowResource,
  isManifestExpired,
  getManifestResourceSummary,
} from "./manifest";

// --- Workflow ---
export {
  materializeWorkflow,
  materializeWorkflowBatch,
} from "./workflow";

// --- Cache ---
export {
  cacheGet,
  cacheSet,
  cacheInvalidate,
  cacheInvalidateByPrefix,
  cacheClear,
  cacheStats,
  cacheResetStats,
} from "./cache";

// --- Materializer utilities ---
export {
  stampMaterialized,
  resolveOptions,
  cachedMaterialize,
  getTtlForTier,
} from "./materializer";
