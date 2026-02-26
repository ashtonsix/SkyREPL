// api/routes/server.ts — createServer(), middleware wiring, global error handler, health check

import { Elysia } from "elysia";
import {
  SkyREPLError,
  httpStatusForError,
  errorToApiError,
} from "@skyrepl/contracts";
import {
  validateApiKey,
  setAuthContext,
  getAuthContext,
} from "../middleware/auth";
import { checkRateLimit, getRateLimitInfo } from "../middleware/rate-limit";
import { registerResourceRoutes } from "./resources";
import { registerOperationRoutes } from "./operations";
import { registerPreflightRoutes } from "./preflight";
import { registerOrphanRoutes } from "./orphans";
import { getSqlStorageSizeCache, SQL_STORAGE_ADVISORY_BYTES, SQL_STORAGE_STRONG_BYTES } from "../../material/storage";
import { getDefaultBlobProvider } from "../../provider/storage/registry";
import { SqlBlobProvider } from "../../provider/storage/sql-blob";

// =============================================================================
// Types
// =============================================================================

export interface ServerConfig {
  port: number;
  corsOrigins: string[];
  maxBodySize: number;
}

export interface PaginationMeta {
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
  nextCursor?: string;
}

export interface ListRequest {
  limit?: number;
  cursor?: string;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
}

// =============================================================================
// Server Setup
// =============================================================================

export function createServer(config: ServerConfig): Elysia {
  const app = new Elysia();

  // Global error handler: catch errors and return structured JSON
  app.onError(({ error, set }) => {
    // If it's a SkyREPLError, use its category for HTTP status
    if (error instanceof SkyREPLError) {
      set.status = httpStatusForError(error);
      return errorToApiError(error);
    }

    // Elysia validation errors (from TypeBox)
    if (error && typeof error === "object" && "code" in error) {
      const e = error as Record<string, unknown>;
      if (e.code === "VALIDATION" || e.code === "PARSE") {
        set.status = 400;
        return {
          error: {
            code: "INVALID_INPUT",
            message: String(e.message ?? "Validation error"),
            category: "validation",
          },
        };
      }
      if (e.code === "NOT_FOUND") {
        set.status = 404;
        return {
          error: {
            code: "RESOURCE_NOT_FOUND",
            message: String(e.message ?? "Not found"),
            category: "not_found",
          },
        };
      }
    }

    // Catch-all: internal error
    console.error("[routes] Unhandled error:", error);
    set.status = 500;
    return {
      error: {
        code: "INTERNAL_ERROR",
        message: error instanceof Error ? error.message : "Internal server error",
        category: "internal",
      },
    };
  });

  // API key authentication for user-facing routes
  app.onBeforeHandle(({ request, set }) => {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // Skip auth for public/agent-facing paths and WebSocket/SSE streams
    // (WS upgrade requests can't carry Authorization headers; streams will
    // get their own auth story in Shell's proxy layer)
    if (
      pathname === "/v1/health" ||
      pathname.startsWith("/v1/agent/") ||
      /^\/v1\/instances\/[^/]+\/panic$/.test(pathname) ||
      pathname.startsWith("/v1/blobs/by-checksum/") ||
      /^\/v1\/runs\/[^/]+\/logs$/.test(pathname) ||
      /^\/v1\/workflows\/[^/]+\/(stream|progress)$/.test(pathname) ||
      request.headers.get("upgrade") === "websocket"
    ) {
      return;
    }

    // All other /v1/* routes require a valid API key
    if (pathname.startsWith("/v1/")) {
      const authHeader = request.headers.get("authorization");
      if (!authHeader?.startsWith("Bearer ")) {
        set.status = 401;
        return {
          error: {
            code: "UNAUTHORIZED",
            message: "Missing API key",
            category: "auth",
          },
        };
      }

      const rawKey = authHeader.slice(7);
      const authCtx = validateApiKey(rawKey);
      if (!authCtx) {
        set.status = 401;
        return {
          error: {
            code: "UNAUTHORIZED",
            message: "Invalid or expired API key",
            category: "auth",
          },
        };
      }

      setAuthContext(request, authCtx);

      // Rate limiting: per-token sliding window (API-03)
      const limited = checkRateLimit(String(authCtx.userId));
      if (limited) {
        set.status = 429;
        set.headers["retry-after"] = String(limited.retryAfter);
        set.headers["x-ratelimit-limit"] = String(limited.limit);
        set.headers["x-ratelimit-remaining"] = "0";
        return {
          error: {
            code: "RATE_LIMITED",
            message: `Rate limit exceeded. Retry after ${limited.retryAfter}s.`,
            category: "rate_limit",
          },
        };
      }

      // Add rate limit headers to successful responses
      const rlInfo = getRateLimitInfo(String(authCtx.userId));
      set.headers["x-ratelimit-limit"] = String(rlInfo.limit);
      set.headers["x-ratelimit-remaining"] = String(rlInfo.remaining);
      set.headers["x-ratelimit-reset"] = String(rlInfo.reset);
    }
  });

  // Health check
  app.get("/v1/health", () => {
    const response: Record<string, unknown> = {
      status: "ok",
      timestamp: Date.now(),
    };

    // Include SQL storage size info when the default provider is SQL
    const provider = getDefaultBlobProvider();
    if (provider instanceof SqlBlobProvider) {
      const bytes = getSqlStorageSizeCache();
      const gb = (bytes / (1024 * 1024 * 1024)).toFixed(2);
      let storageStatus = "ok";
      if (bytes >= SQL_STORAGE_STRONG_BYTES) storageStatus = "warning_strong";
      else if (bytes >= SQL_STORAGE_ADVISORY_BYTES) storageStatus = "warning_advisory";
      response.storage = {
        provider: "sql",
        total_bytes: bytes,
        total_gb: gb,
        status: storageStatus,
      };
    }

    return response;
  });

  // Register route groups
  registerResourceRoutes(app);
  registerOperationRoutes(app);
  registerPreflightRoutes(app);
  registerOrphanRoutes(app);

  return app;
}
