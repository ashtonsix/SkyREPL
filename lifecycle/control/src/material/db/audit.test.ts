// db/audit.test.ts - Unit tests for emitAuditEvent and getAuditEvents
//
// Covers:
//   1. emitAuditEvent unit tests (metering_start, ULID format, source required,
//      classification required, data as JSON)
//   2. Idempotency via dedupe_key
//   3. Append-only trigger tests (UPDATE → ABORT, DELETE → ABORT)
//   4. getAuditEvents filter tests

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../../tests/harness";
import { emitAuditEvent, getAuditEvents, type AuditEvent } from "./audit";
import { createInstance } from "./instances";
import { createAllocation } from "./allocations";
import { createRun } from "./runs";
import { queryOne } from "./helpers";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({}); });
afterEach(() => cleanup());

// =============================================================================
// Helpers
// =============================================================================

function seedInstance() {
  return createInstance({
    provider: "orbstack",
    provider_id: `test-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    spec: "4vcpu-8gb",
    region: "local",
    ip: null,
    workflow_state: "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: Date.now(),
    provider_metadata: null,
    display_name: null,
  });
}

// =============================================================================
// 1. emitAuditEvent unit tests
// =============================================================================

describe("emitAuditEvent", () => {
  test("emits metering_start → row in audit_log", () => {
    const inst = seedInstance();
    const now = Date.now();

    const event = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      instance_id: inst.id,
      provider: "orbstack",
      spec: "4vcpu-8gb",
      region: "local",
      source: "lifecycle",
      is_cost: true,
      is_usage: true,
      data: { provider_resource_id: inst.provider_id, metering_window_start_ms: now },
      occurred_at: now,
    });

    expect(event.id).toBeGreaterThan(0);
    expect(event.event_type).toBe("metering_start");
    expect(event.tenant_id).toBe(1);
    expect(event.instance_id).toBe(inst.id);
    expect(event.is_cost).toBe(1);
    expect(event.is_usage).toBe(1);
    expect(event.source).toBe("lifecycle");
    expect(event.occurred_at).toBe(now);
  });

  test("event_uuid is a valid ULID (26 chars, Crockford base32)", () => {
    const now = Date.now();
    const event = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: { test: true },
      occurred_at: now,
    });

    // ULID: 26 chars, only Crockford base32 chars
    expect(event.event_uuid).toHaveLength(26);
    expect(event.event_uuid).toMatch(/^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}$/);
  });

  test("consecutive ULIDs are monotonically sortable (same ms)", () => {
    const now = Date.now();
    const events = Array.from({ length: 5 }, (_, i) =>
      emitAuditEvent({
        event_type: "metering_start",
        tenant_id: 1,
        source: "test",
        is_cost: true,
        data: { seq: i },
        occurred_at: now,
      })
    );
    const uuids = events.map(e => e.event_uuid);
    const sorted = [...uuids].sort();
    expect(sorted).toEqual(uuids);
  });

  test("source is required → throws if missing", () => {
    expect(() =>
      emitAuditEvent({
        event_type: "metering_start",
        tenant_id: 1,
        source: "",
        is_cost: true,
        data: {},
        occurred_at: Date.now(),
      })
    ).toThrow(/source is required/i);
  });

  test("no classification booleans → throws (unless price_observation)", () => {
    expect(() =>
      emitAuditEvent({
        event_type: "metering_start",
        tenant_id: 1,
        source: "test",
        // no is_cost/is_usage/is_attribution/is_reconciliation
        data: {},
        occurred_at: Date.now(),
      })
    ).toThrow(/at least one of is_cost\/is_usage\/is_attribution\/is_reconciliation/i);
  });

  test("price_observation is exempt from classification requirement", () => {
    expect(() =>
      emitAuditEvent({
        event_type: "price_observation",
        tenant_id: 1,
        source: "heartbeat",
        // no classification flags
        data: { rate_per_hour: 0.5, currency: "USD" },
        occurred_at: Date.now(),
      })
    ).not.toThrow();
  });

  test("data is stored as JSON string in DB", () => {
    const payload = { foo: "bar", nested: { x: 1 } };
    const event = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: payload,
      occurred_at: Date.now(),
    });

    // data column is a JSON string
    expect(event.data).toBe(JSON.stringify(payload));
    expect(JSON.parse(event.data)).toEqual(payload);
  });
});

// =============================================================================
// 2. Idempotency tests
// =============================================================================

describe("emitAuditEvent idempotency", () => {
  test("same dedupe_key → returns existing row (not a new one)", () => {
    const dedupeKey = `test:idempotency:${Date.now()}`;
    const now = Date.now();

    const first = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: { attempt: 1 },
      dedupe_key: dedupeKey,
      occurred_at: now,
    });

    const second = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: { attempt: 2 }, // different data — should be ignored
      dedupe_key: dedupeKey,
      occurred_at: now + 1000,
    });

    // Must return the same row (same id and uuid)
    expect(second.id).toBe(first.id);
    expect(second.event_uuid).toBe(first.event_uuid);
    // data from second call must NOT be stored
    expect(JSON.parse(second.data).attempt).toBe(1);
  });

  test("different dedupe_key → new row", () => {
    const now = Date.now();

    const first = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      dedupe_key: `key-a-${now}`,
      occurred_at: now,
    });

    const second = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      dedupe_key: `key-b-${now}`,
      occurred_at: now,
    });

    expect(second.id).toBeGreaterThan(first.id);
    expect(second.event_uuid).not.toBe(first.event_uuid);
  });

  test("no dedupe_key → always inserts new row", () => {
    const now = Date.now();

    const first = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: now,
    });

    const second = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: now,
    });

    expect(second.id).toBeGreaterThan(first.id);
  });
});

// =============================================================================
// 3. Append-only trigger tests
// =============================================================================

describe("audit_log append-only triggers", () => {
  test("UPDATE on audit_log → ABORT (trigger fires)", () => {
    const event = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: Date.now(),
    });

    const { getDatabase } = require("./helpers");
    const db = getDatabase();

    expect(() => {
      db.prepare("UPDATE audit_log SET event_type = 'tampered' WHERE id = ?").run(event.id);
    }).toThrow(/append-only/i);
  });

  test("DELETE on audit_log → ABORT (trigger fires)", () => {
    const event = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: Date.now(),
    });

    const { getDatabase } = require("./helpers");
    const db = getDatabase();

    expect(() => {
      db.prepare("DELETE FROM audit_log WHERE id = ?").run(event.id);
    }).toThrow(/append-only/i);
  });
});

// =============================================================================
// 4. getAuditEvents filter tests
// =============================================================================

describe("getAuditEvents filters", () => {
  test("filter by tenant_id", () => {
    const now = Date.now();

    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: now,
    });

    const events = getAuditEvents({ tenant_id: 1 });
    expect(events.length).toBeGreaterThanOrEqual(1);
    for (const e of events) {
      expect(e.tenant_id).toBe(1);
    }
  });

  test("filter by event_type", () => {
    const now = Date.now();

    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      is_usage: true,
      data: {},
      occurred_at: now,
    });

    emitAuditEvent({
      event_type: "attribution_start",
      tenant_id: 1,
      source: "test",
      is_attribution: true,
      data: {},
      occurred_at: now,
    });

    const starts = getAuditEvents({ event_type: "metering_start" });
    expect(starts.every(e => e.event_type === "metering_start")).toBe(true);

    const attribs = getAuditEvents({ event_type: "attribution_start" });
    expect(attribs.every(e => e.event_type === "attribution_start")).toBe(true);
  });

  test("filter by instance_id", () => {
    const inst = seedInstance();
    const now = Date.now();

    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      instance_id: inst.id,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: now,
    });

    const events = getAuditEvents({ instance_id: inst.id });
    expect(events.length).toBeGreaterThanOrEqual(1);
    for (const e of events) {
      expect(e.instance_id).toBe(inst.id);
    }
  });

  test("filter by since_ms", () => {
    const pastMs = Date.now() - 100_000;
    const futureMs = Date.now() + 100_000;

    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: pastMs,
    });

    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: {},
      occurred_at: futureMs,
    });

    const recentOnly = getAuditEvents({ since_ms: Date.now() });
    expect(recentOnly.every(e => e.occurred_at >= Date.now() - 10_000)).toBe(true);
  });

  test("limit parameter is respected", () => {
    const now = Date.now();
    for (let i = 0; i < 5; i++) {
      emitAuditEvent({
        event_type: "metering_start",
        tenant_id: 1,
        source: "test",
        is_cost: true,
        data: { seq: i },
        occurred_at: now + i,
      });
    }

    const limited = getAuditEvents({ tenant_id: 1, limit: 2 });
    expect(limited.length).toBeLessThanOrEqual(2);
  });
});

// =============================================================================
// 5. Lifecycle integration test — full 4-event sequence
// =============================================================================

describe("lifecycle event sequence", () => {
  test("metering_start → attribution_start → attribution_end → metering_stop", () => {
    const inst = seedInstance();
    const now = Date.now();

    // Create run and allocation for FK references
    const run = createRun({
      command: "echo test",
      workdir: "/workspace",
      max_duration_ms: 60000,
      workflow_state: "launch-run:running",
      current_manifest_id: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      exit_code: null,
      started_at: null,
      finished_at: null,
      workflow_error: null,
    });

    const alloc = createAllocation({
      run_id: run.id,
      instance_id: inst.id,
      status: "ACTIVE",
      current_manifest_id: null,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });

    // 1. metering_start (instance becomes billable at boot:complete)
    const e1 = emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      instance_id: inst.id,
      allocation_id: alloc.id,
      run_id: run.id,
      provider: "orbstack",
      spec: "4vcpu-8gb",
      region: "local",
      source: "lifecycle",
      is_cost: true,
      is_usage: true,
      data: { provider_resource_id: inst.provider_id, metering_window_start_ms: now },
      dedupe_key: `orbstack:${inst.provider_id}:metering_start`,
      occurred_at: now,
    });

    // 2. attribution_start (agent reports sync_complete, CLAIMED → ACTIVE)
    const e2 = emitAuditEvent({
      event_type: "attribution_start",
      tenant_id: 1,
      instance_id: inst.id,
      allocation_id: alloc.id,
      run_id: run.id,
      source: "lifecycle",
      is_attribution: true,
      data: { attribution_weight: 1.0 },
      dedupe_key: `attribution_start:${alloc.id}`,
      occurred_at: now + 1000,
    });

    // 3. attribution_end (run completes, ACTIVE → COMPLETE)
    const e3 = emitAuditEvent({
      event_type: "attribution_end",
      tenant_id: 1,
      instance_id: inst.id,
      allocation_id: alloc.id,
      run_id: run.id,
      source: "lifecycle",
      is_attribution: true,
      data: { reason: "complete" },
      dedupe_key: `attribution_end:${alloc.id}`,
      occurred_at: now + 5000,
    });

    // 4. metering_stop (instance terminated)
    const e4 = emitAuditEvent({
      event_type: "metering_stop",
      tenant_id: 1,
      instance_id: inst.id,
      provider: "orbstack",
      spec: "4vcpu-8gb",
      region: "local",
      source: "lifecycle",
      is_cost: true,
      is_usage: true,
      data: { provider_resource_id: inst.provider_id, metering_window_end_ms: now + 6000 },
      dedupe_key: `orbstack:${inst.provider_id}:metering_stop`,
      occurred_at: now + 6000,
    });

    // Verify chronological ordering
    expect(e1.occurred_at).toBeLessThan(e2.occurred_at);
    expect(e2.occurred_at).toBeLessThan(e3.occurred_at);
    expect(e3.occurred_at).toBeLessThan(e4.occurred_at);

    // Verify ULID ordering matches chronological ordering
    expect(e1.event_uuid < e2.event_uuid).toBe(true);
    expect(e2.event_uuid < e3.event_uuid).toBe(true);
    expect(e3.event_uuid < e4.event_uuid).toBe(true);

    // Verify all events share the same instance FK
    expect(e1.instance_id).toBe(inst.id);
    expect(e2.instance_id).toBe(inst.id);
    expect(e3.instance_id).toBe(inst.id);
    expect(e4.instance_id).toBe(inst.id);

    // Verify allocation FK on attribution events
    expect(e2.allocation_id).toBe(alloc.id);
    expect(e3.allocation_id).toBe(alloc.id);

    // Verify classification booleans
    expect(e1.is_cost).toBe(1);
    expect(e1.is_usage).toBe(1);
    expect(e1.is_attribution).toBe(0);
    expect(e2.is_attribution).toBe(1);
    expect(e2.is_cost).toBe(0);
    expect(e3.is_attribution).toBe(1);
    expect(e4.is_cost).toBe(1);
    expect(e4.is_usage).toBe(1);

    // Verify all 4 events retrievable by instance_id
    const events = getAuditEvents({ instance_id: inst.id });
    expect(events.length).toBe(4);
    expect(events.map(e => e.event_type)).toEqual([
      "metering_start",
      "attribution_start",
      "attribution_end",
      "metering_stop",
    ]);
  });
});
