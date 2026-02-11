// db/orphans.ts - Orphan scan and whitelist operations

import { queryOne, queryMany, execute } from "./helpers";

export interface OrphanScanResult {
  provider: string;
  scanned_at: number;
  orphans_found: number;
  orphan_ids: string[];
}

export function getTrackedInstanceIds(): Set<number> {
  const rows = queryMany<{ id: number }>("SELECT id FROM instances");
  return new Set(rows.map(r => r.id));
}

export function getActiveManifestIds(): Set<number> {
  const now = Date.now();
  const rows = queryMany<{ id: number }>(
    `SELECT id FROM manifests
     WHERE status = 'DRAFT' OR (status = 'SEALED' AND expires_at > ?)`,
    [now]
  );
  return new Set(rows.map(r => r.id));
}

export function recordOrphanScan(result: OrphanScanResult): void {
  execute(
    `INSERT INTO orphan_scans (provider, scanned_at, orphans_found, orphan_ids, created_at)
     VALUES (?, ?, ?, ?, ?)`,
    [result.provider, result.scanned_at, result.orphans_found, JSON.stringify(result.orphan_ids), Date.now()]
  );
}

export function addToWhitelist(
  provider: string,
  providerId: string,
  resourceType: string,
  reason: string,
  acknowledgedBy: string
): void {
  const now = Date.now();

  execute(
    `INSERT INTO orphan_whitelist (provider, provider_id, resource_type, reason, acknowledged_by, acknowledged_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT (provider, provider_id) DO UPDATE SET reason = excluded.reason, acknowledged_by = excluded.acknowledged_by, acknowledged_at = excluded.acknowledged_at`,
    [provider, providerId, resourceType, reason, acknowledgedBy, now]
  );
}

export function isWhitelisted(
  provider: string,
  providerId: string
): boolean {
  const result = queryOne<{ provider_id: string }>(
    "SELECT provider_id FROM orphan_whitelist WHERE provider = ? AND provider_id = ?",
    [provider, providerId]
  );
  return result !== null;
}
