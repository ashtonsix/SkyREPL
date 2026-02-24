// orphan/naming.ts - Naming convention enforcement
//
// Validates and audits resource names against the SkyREPL naming convention:
//   repl-<controlId>-<manifestId_base36>-<resourceId_base36>

import {
  parseResourceName,
  formatResourceName,
  isReplResource,
} from "../material/control-id";
import { queryMany } from "../material/db"; // raw-db: naming audit instance query (Bucket B), see WL-057
import { getProvider } from "../provider/registry";
import type { ProviderName } from "../provider/types";

// =============================================================================
// Types
// =============================================================================

export interface ParsedName {
  controlId: string;
  manifestId: number | null;
  resourceId: number;
}

export interface ValidationResult {
  valid: boolean;
  parsed: ParsedName | null;
  errors: string[];
}

export interface ComplianceCheck {
  compliant: boolean;
  actual: string;
  expected: string;
}

export interface NoncompliantEntry {
  id: number;
  providerId: string;
  actual: string;
  expected: string;
}

export interface AuditResult {
  total: number;
  compliant: number;
  noncompliant: NoncompliantEntry[];
}

// =============================================================================
// Functions
// =============================================================================

/**
 * Validate a resource name against the SkyREPL naming convention.
 * Returns validation status, parsed components, and any error messages.
 */
export function validateResourceName(name: string): ValidationResult {
  const errors: string[] = [];

  if (!name || typeof name !== "string") {
    errors.push("Name must be a non-empty string");
    return { valid: false, parsed: null, errors };
  }

  if (!isReplResource(name)) {
    errors.push(
      `Name '${name}' does not match convention repl-<6char>-<manifestId|none>-<resourceId>`
    );
    return { valid: false, parsed: null, errors };
  }

  const parsed = parseResourceName(name);
  if (!parsed) {
    errors.push(`Name '${name}' matched prefix but failed to parse components`);
    return { valid: false, parsed: null, errors };
  }

  return { valid: true, parsed, errors: [] };
}

/**
 * Check whether a specific provider resource uses the expected name.
 * Returns compliance status and both actual and expected names for diffing.
 */
export function enforceNamingConvention(
  _provider: string,
  _providerId: string,
  expectedName: string
): ComplianceCheck {
  // The "actual" name we check is the expectedName itself — callers pass in the
  // name they retrieved from the provider. This function validates that name
  // against the convention and returns whether it matches the expected form.
  const validation = validateResourceName(expectedName);

  if (!validation.valid || !validation.parsed) {
    return {
      compliant: false,
      actual: expectedName,
      expected: expectedName,
    };
  }

  const { controlId, manifestId, resourceId } = validation.parsed;
  const canonical = formatResourceName(controlId, manifestId, resourceId);

  return {
    compliant: canonical === expectedName,
    actual: expectedName,
    expected: canonical,
  };
}

// Row shape returned from DB for naming audit
interface InstanceRow {
  id: number;
  provider_id: string;
  current_manifest_id: number | null;
}

/**
 * Audit all tracked instances for a provider to check naming compliance.
 * Cross-references DB records with cloud instance names.
 *
 * An instance is non-compliant when its provider-reported name either doesn't
 * match the convention or doesn't encode the IDs we have in DB.
 */
export async function auditNamingCompliance(
  providerName: string
): Promise<AuditResult> {
  const provider = await getProvider(providerName as ProviderName);
  const cloudInstances = await provider.list({ includeTerminated: false });

  // Build a map of cloud provider_id → name
  const cloudNameMap = new Map<string, string>();
  for (const inst of cloudInstances) {
    const name = (inst.metadata?.name as string) ?? inst.id;
    cloudNameMap.set(inst.id, name);
  }

  const trackedInstances = queryMany<InstanceRow>(
    `SELECT id, provider_id, current_manifest_id FROM instances
     WHERE provider = ? AND provider_id != ''`,
    [providerName]
  );

  const noncompliant: NoncompliantEntry[] = [];

  for (const inst of trackedInstances) {
    const cloudName = cloudNameMap.get(inst.provider_id);
    if (cloudName === undefined) {
      // Not in cloud — can't audit naming (may be terminated/pending)
      continue;
    }

    const parsed = parseResourceName(cloudName);
    if (!parsed || parsed.resourceId !== inst.id) {
      // Doesn't parse correctly or resource ID mismatch
      const expectedName = formatResourceName(
        parsed?.controlId ?? "??????",
        inst.current_manifest_id ?? null,
        inst.id
      );
      noncompliant.push({
        id: inst.id,
        providerId: inst.provider_id,
        actual: cloudName,
        expected: expectedName,
      });
    }
  }

  const total = trackedInstances.filter(i => cloudNameMap.has(i.provider_id)).length;

  return {
    total,
    compliant: total - noncompliant.length,
    noncompliant,
  };
}
