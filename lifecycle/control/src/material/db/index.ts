// db/index.ts - Barrel re-export
// All consumers import from "../material/db" → resolves to this file.
// Zero import churn: every symbol previously exported by db.ts is re-exported here.

// Connection and migration management
export {
  initDatabase,
  closeDatabase,
  getMigrationVersion,
  runMigrations,
  walCheckpoint,
} from "./init";

// Query helpers
export {
  getDatabase,
  queryOne,
  queryMany,
  execute,
  transaction,
} from "./helpers";

// Instances
export {
  type Instance,
  getInstance,
  getInstanceByProviderId,
  getInstanceByTokenHash,
  getInstanceByIdempotencyKey,
  createInstance,
  updateInstance,
  listInstances,
  deleteInstance,
} from "./instances";

// Allocations
export {
  type Allocation,
  getAllocation,
  getAllocationByRunId,
  updateAllocation,
  createAllocation,
  deleteAllocation,
  findWarmAllocation,
  countInstanceAllocations,
  findStaleClaimed,
  findExpiredAvailable,
  getWarmPoolStats,
} from "./allocations";

// Runs
export {
  type Run,
  getRun,
  createRun,
  updateRun,
  listRuns,
  deleteRun,
} from "./runs";

// Workflows
export {
  type Workflow,
  type WorkflowNode,
  getWorkflow,
  createWorkflow,
  updateWorkflow,
  getWorkflowNodes,
  getWorkflowNode,
  createWorkflowNode,
  updateWorkflowNode,
  findReadyNodes,
  findActiveWorkflows,
  deleteWorkflow,
} from "./workflows";

// Manifests
export {
  type Manifest,
  type ManifestResource,
  getManifest,
  createManifest,
  addResourceToManifest,
  getManifestResources,
  deleteManifest,
  listExpiredManifests,
  getManifestObjectIds,
} from "./manifests";

// Objects and Blobs
export {
  type Blob,
  type StorageObject,
  getBlob,
  createBlob,
  findBlobByChecksum,
  updateBlobLastReferenced,
  updateBlobStorageKey,
  updateBlobSize,
  findOrphanedBlobs,
  deleteBlobBatch,
  deleteBlob,
  getObject,
  createObject,
  addObjectTag,
  findObjectByTag,
  deleteObject,
  updateObjectTimestamp,
  updateObjectMetadata,
  deleteObjectBatch,
  checkBlobsExist,
} from "./objects";

// Orphans
export {
  type OrphanScanResult,
  type WhitelistEntry,
  getTrackedInstanceIds,
  getActiveManifestIds,
  recordOrphanScan,
  addToWhitelist,
  isWhitelisted,
  getWhitelist,
  removeFromWhitelist,
  getRecentScans,
} from "./orphans";

// Usage
export {
  type UsageRecord,
  createUsageRecord,
  finishUsageRecord,
  getMonthlyCostByProvider,
  getActiveUsageRecords,
  getOpenUsageRecordsForInstance,
  getTotalCostByTenant,
  getUserCostByTenant,
} from "./usage";

// Tenants and Users
export {
  type Tenant,
  type User,
  getTenant,
  getTenantByName,
  createTenant,
  updateTenant,
  listTenants,
  getUser,
  getUserByEmail,
  getUserByApiKeyId,
  createUser,
  updateUser,
  removeUser,
  listTenantUsers,
  countTenantUsers,
} from "./tenants";
