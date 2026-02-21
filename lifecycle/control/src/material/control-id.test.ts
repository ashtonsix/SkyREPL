import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "bun:sqlite";
import { setupTest } from "../../../tests/harness";
import { getDatabase } from "./db";
import {
  generateControlId,
  getControlId,
  formatResourceName,
  parseResourceName,
  isReplResource,
  _resetControlIdCache,
} from "./control-id";

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
  _resetControlIdCache();
});

afterEach(async () => {
  _resetControlIdCache();
  await cleanup();
});

// =============================================================================
// generateControlId
// =============================================================================

describe("generateControlId", () => {
  test("returns a 6-char lowercase base36 string", () => {
    const id = generateControlId();
    expect(id).toHaveLength(6);
    expect(/^[0-9a-z]{6}$/.test(id)).toBe(true);
  });

  test("returns distinct values on repeated calls", () => {
    const ids = new Set(Array.from({ length: 20 }, () => generateControlId()));
    // With 36^6 ≈ 2.1B possibilities, collisions across 20 draws are astronomically unlikely.
    expect(ids.size).toBeGreaterThan(1);
  });
});

// =============================================================================
// getControlId
// =============================================================================

describe("getControlId", () => {
  test("creates a control_id on first call and persists it to the DB", () => {
    const db = getDatabase();
    const id = getControlId(db);

    expect(id).toHaveLength(6);
    expect(/^[0-9a-z]{6}$/.test(id)).toBe(true);

    const row = db
      .prepare("SELECT value FROM settings WHERE key = 'control_id'")
      .get() as { value: string } | undefined;

    expect(row).not.toBeUndefined();
    expect(row!.value).toBe(id);
  });

  test("returns the same value on subsequent calls (in-memory cache)", () => {
    const db = getDatabase();
    const first = getControlId(db);
    const second = getControlId(db);
    expect(second).toBe(first);
  });

  test("reads persisted value from DB after cache is cleared", () => {
    const db = getDatabase();
    const first = getControlId(db);

    // Simulate a process restart by clearing the in-memory cache.
    _resetControlIdCache();

    const second = getControlId(db);
    expect(second).toBe(first);
  });

  test("does not insert a second row on re-entrant call after cache clear", () => {
    const db = getDatabase();
    getControlId(db);
    _resetControlIdCache();
    getControlId(db);

    const rows = db
      .prepare("SELECT COUNT(*) as n FROM settings WHERE key = 'control_id'")
      .get() as { n: number };

    expect(rows.n).toBe(1);
  });
});

// =============================================================================
// formatResourceName
// =============================================================================

describe("formatResourceName", () => {
  test("produces repl-<cid>-<mid36>-<rid36>", () => {
    // Decimal 255 = 0x ff = base36 "73"
    expect(formatResourceName("abc123", 255, 1)).toBe("repl-abc123-73-1");
  });

  test("encodes small integer IDs as bare base36 digits", () => {
    expect(formatResourceName("aaaaaa", 1, 1)).toBe("repl-aaaaaa-1-1");
  });

  test("encodes larger IDs correctly", () => {
    // 1296 in base36 is "100"
    expect(formatResourceName("zzzzzz", 1296, 1296)).toBe("repl-zzzzzz-100-100");
  });

  test("uses 'none' literal when manifestId is null", () => {
    expect(formatResourceName("abc123", null, 1)).toBe("repl-abc123-none-1");
  });
});

// =============================================================================
// parseResourceName
// =============================================================================

describe("parseResourceName", () => {
  test("round-trips with formatResourceName", () => {
    const name = formatResourceName("ab1cd2", 42, 7);
    const parsed = parseResourceName(name);

    expect(parsed).not.toBeNull();
    expect(parsed!.controlId).toBe("ab1cd2");
    expect(parsed!.manifestId).toBe(42);
    expect(parsed!.resourceId).toBe(7);
  });

  test("round-trips with larger IDs", () => {
    const name = formatResourceName("000000", 99999, 88888);
    const parsed = parseResourceName(name);

    expect(parsed).not.toBeNull();
    expect(parsed!.manifestId).toBe(99999);
    expect(parsed!.resourceId).toBe(88888);
  });

  test("returns null for a bare repl- prefix with no segments", () => {
    expect(parseResourceName("repl-")).toBeNull();
  });

  test("returns null for a name that does not start with repl-", () => {
    expect(parseResourceName("aws-abc123-1-1")).toBeNull();
    expect(parseResourceName("abc123")).toBeNull();
  });

  test("returns null for a control_id that is not exactly 6 chars", () => {
    expect(parseResourceName("repl-ab-1-1")).toBeNull();
    expect(parseResourceName("repl-abcdefg-1-1")).toBeNull();
  });

  test("returns null for legacy OrbStack names", () => {
    // Legacy: repl-<timestamp>
    expect(parseResourceName("repl-1234567890")).toBeNull();
    // Legacy: repl-i<instanceId>-<timestamp>
    expect(parseResourceName("repl-i42-1234567890")).toBeNull();
  });

  test("parses 'none' manifest segment as null", () => {
    const parsed = parseResourceName("repl-abc123-none-1");
    expect(parsed).not.toBeNull();
    expect(parsed!.controlId).toBe("abc123");
    expect(parsed!.manifestId).toBeNull();
    expect(parsed!.resourceId).toBe(1);
  });

  test("round-trips null manifestId through format/parse", () => {
    const name = formatResourceName("ab1cd2", null, 7);
    const parsed = parseResourceName(name);
    expect(parsed).not.toBeNull();
    expect(parsed!.manifestId).toBeNull();
    expect(parsed!.resourceId).toBe(7);
  });
});

// =============================================================================
// isReplResource
// =============================================================================

describe("isReplResource", () => {
  test("matches valid repl-<cid>-<mid>-<rid> names", () => {
    expect(isReplResource("repl-abc123-1-1")).toBe(true);
    expect(isReplResource(formatResourceName("zzzzzz", 1, 1))).toBe(true);
  });

  test("rejects names without the repl- prefix", () => {
    expect(isReplResource("aws-abc123-1-1")).toBe(false);
    expect(isReplResource("abc123")).toBe(false);
    expect(isReplResource("")).toBe(false);
  });

  test("rejects names with wrong control_id length", () => {
    expect(isReplResource("repl-ab-1-1")).toBe(false);
    expect(isReplResource("repl-abcdefg-1-1")).toBe(false);
  });

  test("rejects names missing one or more trailing segments", () => {
    expect(isReplResource("repl-abc123")).toBe(false);
    expect(isReplResource("repl-abc123-1")).toBe(false);
  });

  test("rejects legacy OrbStack names", () => {
    // repl-<timestamp> — 10 digit timestamp, NOT a 6-char control_id
    expect(isReplResource("repl-1234567890")).toBe(false);
    // repl-i<instanceId>-<timestamp>
    expect(isReplResource("repl-i42-1234567890")).toBe(false);
  });

  test("rejects names with uppercase letters", () => {
    expect(isReplResource("repl-ABC123-1-1")).toBe(false);
  });
});
