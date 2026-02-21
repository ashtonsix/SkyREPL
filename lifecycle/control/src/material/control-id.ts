/**
 * Control plane identity — generated once on first startup, persisted in DB.
 *
 * Format: 6-character base36 slug (lowercase alphanumeric).
 * Used in resource naming: repl-<control_id>-<manifest_id>-<resource_id>
 *
 * The naming convention enables orphan recovery even after total DB loss:
 * the control_id identifies which installation created the resource, while
 * manifest_id and resource_id (base36-encoded integers) allow re-association.
 */

import { Database } from "bun:sqlite";

// In-memory cache — set once, never changes for the lifetime of a process.
let cachedControlId: string | null = null;

/**
 * Generate a random 6-char base36 control_id.
 * Uses crypto.getRandomValues for cryptographic-quality randomness.
 */
export function generateControlId(): string {
  const bytes = new Uint8Array(4);
  crypto.getRandomValues(bytes);
  // Combine 4 random bytes into a 32-bit unsigned integer, convert to base36.
  // 36^6 = 2_176_782_336, well within uint32 range (4_294_967_295).
  const n = (bytes[0]! << 24 | bytes[1]! << 16 | bytes[2]! << 8 | bytes[3]!) >>> 0;
  return n.toString(36).padStart(6, "0").slice(-6).toLowerCase();
}

/**
 * Get or create the control_id for this installation.
 * First call creates and persists it. Subsequent calls return the cached value.
 *
 * Requires the settings table to exist (migration 003_settings.sql).
 */
export function getControlId(db: Database): string {
  if (cachedControlId !== null) {
    return cachedControlId;
  }

  const row = db
    .prepare("SELECT value FROM settings WHERE key = 'control_id'")
    .get() as { value: string } | undefined;

  if (row) {
    cachedControlId = row.value;
    return cachedControlId;
  }

  const id = generateControlId();
  db.prepare("INSERT INTO settings (key, value) VALUES ('control_id', ?)").run(id);
  cachedControlId = id;
  return cachedControlId;
}

/**
 * Reset the in-memory cache. Intended for tests only — do not call in production.
 * @internal
 */
export function _resetControlIdCache(): void {
  cachedControlId = null;
}

/**
 * Format a resource name per the naming convention:
 *   repl-<control_id>-<manifest_id_base36>-<resource_id_base36>
 *
 * manifest_id and resource_id are base36-encoded from their integer IDs so that
 * names stay compact while remaining human-readable and unambiguously parseable.
 * If manifestId is null (no manifest yet), the literal "none" is used.
 */
export function formatResourceName(
  controlId: string,
  manifestId: number | null,
  resourceId: number
): string {
  const mPart = manifestId === null ? "none" : manifestId.toString(36);
  return `repl-${controlId}-${mPart}-${resourceId.toString(36)}`;
}

// Compiled once: matches repl-<6 alnum>-<base36 or "none">-<base36>
const RESOURCE_NAME_RE = /^repl-([0-9a-z]{6})-([0-9a-z]+|none)-([0-9a-z]+)$/;

/**
 * Parse a resource name back to its components.
 * Returns null if the name doesn't match the naming convention.
 * manifestId is null when the name was created without a manifest ("none" segment).
 */
export function parseResourceName(
  name: string
): { controlId: string; manifestId: number | null; resourceId: number } | null {
  const match = RESOURCE_NAME_RE.exec(name);
  if (!match) return null;

  const controlId = match[1]!;
  const manifestPart = match[2]!;
  const manifestId = manifestPart === "none" ? null : parseInt(manifestPart, 36);
  const resourceId = parseInt(match[3]!, 36);

  if ((manifestId !== null && !isFinite(manifestId)) || !isFinite(resourceId)) return null;

  return { controlId, manifestId, resourceId };
}

/**
 * Check if a name matches the SkyREPL naming convention.
 * Legacy OrbStack names (repl-<timestamp>, repl-i<id>-<timestamp>) do NOT match.
 */
export function isReplResource(name: string): boolean {
  return RESOURCE_NAME_RE.test(name);
}
