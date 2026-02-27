// billing/reconciliation/alerts.ts — Discrepancy alert classification (WL-061-4A)
//
// Classifies billing reconciliation discrepancies into alert levels using
// two independent thresholds: absolute (cents) and percentage.
// A discrepancy triggers if EITHER threshold is exceeded (whichever fires first).

import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// Types
// =============================================================================

/** Alert level for a billing discrepancy */
export type AlertLevel = "none" | "warn" | "alert";

export interface DiscrepancyClassification {
  /** Alert level: "none" | "warn" | "alert" */
  level: AlertLevel;
  /** |ourCents - theirCents| in integer cents */
  absoluteDiff: number;
  /** |diff| / max(ourCents, theirCents) * 100, or 0 if both are 0 */
  percentageDiff: number;
}

// =============================================================================
// classifyDiscrepancy
// =============================================================================

/**
 * Classify a billing discrepancy between our metered cost and the provider's billed cost.
 *
 * Uses "whichever is greater" logic: if EITHER absolute OR percentage exceeds
 * the threshold, the corresponding alert level is triggered.
 *
 * Alert priority: alert > warn > none.
 *
 * Thresholds (configurable via TIMING constants):
 *   WARN:  |diff| >= RECONCILIATION_WARN_THRESHOLD_CENTS (default $1)
 *          OR percentage >= RECONCILIATION_WARN_THRESHOLD_PCT (default 1%)
 *   ALERT: |diff| >= RECONCILIATION_ALERT_THRESHOLD_CENTS (default $10)
 *          OR percentage >= RECONCILIATION_ALERT_THRESHOLD_PCT (default 5%)
 *
 * @param ourCents  - Our metered cost in integer cents (>= 0)
 * @param theirCents - Provider's billed cost in integer cents (>= 0)
 * @returns DiscrepancyClassification with level, absoluteDiff, and percentageDiff
 */
export function classifyDiscrepancy(
  ourCents: number,
  theirCents: number
): DiscrepancyClassification {
  const absoluteDiff = Math.abs(ourCents - theirCents);

  // Percentage: relative to the larger of the two values (avoids divide-by-zero
  // and ensures small discrepancies on large bills are still meaningful).
  const denominator = Math.max(ourCents, theirCents);
  const percentageDiff = denominator > 0 ? (absoluteDiff / denominator) * 100 : 0;

  // Check ALERT thresholds first (higher priority)
  const isAlert =
    absoluteDiff >= TIMING.RECONCILIATION_ALERT_THRESHOLD_CENTS ||
    percentageDiff >= TIMING.RECONCILIATION_ALERT_THRESHOLD_PCT;

  if (isAlert) {
    return { level: "alert", absoluteDiff, percentageDiff };
  }

  // Check WARN thresholds
  const isWarn =
    absoluteDiff >= TIMING.RECONCILIATION_WARN_THRESHOLD_CENTS ||
    percentageDiff >= TIMING.RECONCILIATION_WARN_THRESHOLD_PCT;

  if (isWarn) {
    return { level: "warn", absoluteDiff, percentageDiff };
  }

  return { level: "none", absoluteDiff, percentageDiff };
}
