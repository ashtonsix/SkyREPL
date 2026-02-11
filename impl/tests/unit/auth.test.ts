// tests/unit/auth.test.ts - Authentication Tests

import { describe, test, expect, beforeEach } from "bun:test";
import crypto from "crypto";
import {
  verifyInstanceToken,
  extractToken,
} from "../../control/src/api/middleware/auth";
import {
  initDatabase,
  runMigrations,
  closeDatabase,
  createInstance,
  updateInstance,
} from "../../control/src/material/db";

describe("Authentication Middleware", () => {
  beforeEach(() => {
    closeDatabase();
    initDatabase(":memory:");
    runMigrations();
  });

  describe("verifyInstanceToken", () => {
    test("returns false for nonexistent instance", () => {
      const result = verifyInstanceToken(999, "any-token");
      expect(result).toBe(false);
    });

    test("createInstance persists registration_token_hash (FND-08 regression)", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-hash-persist",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "spawn:complete",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: tokenHash,
        last_heartbeat: Date.now(),
      });

      // Hash should be persisted directly via createInstance (not updateInstance)
      const result = verifyInstanceToken(instance.id, rawToken);
      expect(result).toBe(true);

      // Wrong token should fail
      expect(verifyInstanceToken(instance.id, "wrong")).toBe(false);
    });

    test("returns true for instance with no token hash (backward compatibility)", () => {
      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-1",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "spawn:complete",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
      });

      const result = verifyInstanceToken(instance.id, "any-token");
      expect(result).toBe(true);
    });

    test("returns false for wrong token", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-2",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "spawn:complete",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
      });

      updateInstance(instance.id, { registration_token_hash: tokenHash });

      const result = verifyInstanceToken(instance.id, "wrong-token");
      expect(result).toBe(false);
    });

    test("returns true for correct token", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-3",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "spawn:complete",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
      });

      updateInstance(instance.id, { registration_token_hash: tokenHash });

      const result = verifyInstanceToken(instance.id, rawToken);
      expect(result).toBe(true);
    });

    test("handles string instance_id", () => {
      const rawToken = crypto.randomBytes(16).toString("hex");
      const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

      const instance = createInstance({
        provider: "orbstack",
        provider_id: "test-4",
        spec: "small",
        region: "local",
        ip: null,
        workflow_state: "spawn:complete",
        workflow_error: null,
        current_manifest_id: null,
        spawn_idempotency_key: null,
        is_spot: 0,
        spot_request_id: null,
        init_checksum: null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
      });

      updateInstance(instance.id, { registration_token_hash: tokenHash });

      const result = verifyInstanceToken(String(instance.id), rawToken);
      expect(result).toBe(true);
    });
  });

  describe("extractToken", () => {
    test("extracts token from Bearer header", () => {
      const request = new Request("http://example.com", {
        headers: {
          Authorization: "Bearer test-token-123",
        },
      });

      const token = extractToken(request);
      expect(token).toBe("test-token-123");
    });

    test("extracts token from query param", () => {
      const request = new Request("http://example.com");
      const query = { token: "query-token-456" };

      const token = extractToken(request, query);
      expect(token).toBe("query-token-456");
    });

    test("prefers Authorization header over query param", () => {
      const request = new Request("http://example.com", {
        headers: {
          Authorization: "Bearer header-token",
        },
      });
      const query = { token: "query-token" };

      const token = extractToken(request, query);
      expect(token).toBe("header-token");
    });

    test("returns null when neither present", () => {
      const request = new Request("http://example.com");

      const token = extractToken(request);
      expect(token).toBe(null);
    });

    test("returns null for malformed Authorization header", () => {
      const request = new Request("http://example.com", {
        headers: {
          Authorization: "NotBearer test-token",
        },
      });

      const token = extractToken(request);
      expect(token).toBe(null);
    });
  });
});

// Note: Full endpoint auth tests are in integration tests (e2e-smoke.test.ts)
// This unit test suite focuses on the auth middleware functions themselves.
