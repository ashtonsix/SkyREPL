// api/routes/index.ts — barrel re-export for api/routes/

export { createServer } from "./server";
export type { ServerConfig, PaginationMeta, ListRequest } from "./server";

export { registerResourceRoutes } from "./resources";
export { registerOperationRoutes } from "./operations";
export { registerBlobRoutes } from "./blobs";
export { registerAdminRoutes, checkBudget } from "./admin";
export { registerPreflightRoutes } from "./preflight";

export {
  buildPaginatedQuery,
  encodeCursor,
  decodeCursor,
  buildPaginationMeta,
  paginatedList,
  VALID_TABLES,
  buildRunFilters,
  buildInstanceFilters,
  buildAllocationFilters,
  buildWorkflowFilters,
  buildManifestFilters,
  buildObjectFilters,
} from "./pagination";
