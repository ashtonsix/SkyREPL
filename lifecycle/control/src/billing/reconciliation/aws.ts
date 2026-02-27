// billing/reconciliation/aws.ts — AWS Cost Explorer billing API adapter (WL-061-4A)
//
// Fetches billed costs from AWS Cost Explorer GetCostAndUsage API.
// Real API calls require AWS credentials (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY).
// Settlement window: 48 hours (AWS Cost Explorer reflects charges ~24-48h after usage).
//
// API reference: https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html

import type { ProviderCostEntry, ReconciliationProvider } from "./types";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// AWS Cost Explorer response shape (stubbed for now — real parsing on integration)
// =============================================================================

interface AwsCostAndUsageResult {
  ResultsByTime: Array<{
    TimePeriod: { Start: string; End: string };
    Groups: Array<{
      Keys: string[];
      Metrics: {
        BlendedCost?: { Amount: string; Unit: string };
        UnblendedCost?: { Amount: string; Unit: string };
      };
    }>;
    Total: Record<string, { Amount: string; Unit: string }>;
    Estimated: boolean;
  }>;
}

// =============================================================================
// Credential check
// =============================================================================

function checkAwsCredentials(): void {
  const hasKeyId = !!process.env.AWS_ACCESS_KEY_ID;
  const hasSecret = !!process.env.AWS_SECRET_ACCESS_KEY;
  if (!hasKeyId || !hasSecret) {
    const missing: string[] = [];
    if (!hasKeyId) missing.push("AWS_ACCESS_KEY_ID");
    if (!hasSecret) missing.push("AWS_SECRET_ACCESS_KEY");
    const err = new Error(
      `[billing-recon:aws] Missing AWS credentials: ${missing.join(", ")}. ` +
      `Set these environment variables to enable AWS billing reconciliation.`
    );
    (err as any).code = "MISSING_CREDENTIALS";
    throw err;
  }
}

// =============================================================================
// Parse AWS Cost Explorer response into ProviderCostEntry[]
// =============================================================================

function parseAwsCostResponse(
  response: AwsCostAndUsageResult,
  period: { start: string; end: string }
): ProviderCostEntry[] {
  const entries: ProviderCostEntry[] = [];

  for (const timePeriod of response.ResultsByTime) {
    const periodStartMs = new Date(timePeriod.TimePeriod.Start).getTime();
    const periodEndMs = new Date(timePeriod.TimePeriod.End).getTime();

    // Grouped results (e.g. grouped by resource ID via GROUP_BY)
    for (const group of timePeriod.Groups) {
      const resourceId = group.Keys[0] ?? "unknown";
      const cost = group.Metrics.UnblendedCost ?? group.Metrics.BlendedCost;
      if (!cost) continue;

      const amountUsd = parseFloat(cost.Amount);
      if (isNaN(amountUsd) || amountUsd < 0) continue;

      const billedCents = Math.round(amountUsd * 100);

      entries.push({
        provider: "aws",
        provider_resource_id: resourceId,
        period_start_ms: periodStartMs,
        period_end_ms: periodEndMs,
        billed_amount_cents: billedCents,
        currency: cost.Unit === "USD" ? "USD" : cost.Unit,
        raw_data: { group, timePeriod: timePeriod.TimePeriod, estimated: timePeriod.Estimated },
      });
    }

    // Total (when no grouping — treat as single aggregate entry)
    if (timePeriod.Groups.length === 0) {
      const cost = timePeriod.Total["UnblendedCost"] ?? timePeriod.Total["BlendedCost"];
      if (!cost) continue;

      const amountUsd = parseFloat(cost.Amount);
      if (isNaN(amountUsd) || amountUsd < 0) continue;

      const billedCents = Math.round(amountUsd * 100);

      entries.push({
        provider: "aws",
        provider_resource_id: "aggregate",
        period_start_ms: periodStartMs,
        period_end_ms: periodEndMs,
        billed_amount_cents: billedCents,
        currency: cost.Unit === "USD" ? "USD" : cost.Unit,
        raw_data: { total: timePeriod.Total, timePeriod: timePeriod.TimePeriod, estimated: timePeriod.Estimated },
      });
    }
  }

  return entries;
}

// =============================================================================
// fetchAwsBilledCost
// =============================================================================

/**
 * Fetch billed costs from AWS Cost Explorer for the given period.
 *
 * In production: calls GetCostAndUsage with GROUP_BY ResourceId so each
 * EC2 instance is a separate line item.
 *
 * Requires: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION (optional, defaults to us-east-1).
 *
 * @throws {Error} with code "MISSING_CREDENTIALS" if AWS credentials are not configured.
 */
export async function fetchAwsBilledCost(
  period: { start: string; end: string }
): Promise<ProviderCostEntry[]> {
  checkAwsCredentials();

  try {
    const { CostExplorerClient, GetCostAndUsageCommand } = await import("@aws-sdk/client-cost-explorer");
    const client = new CostExplorerClient({ region: process.env.AWS_REGION ?? "us-east-1" });
    const response = await client.send(new GetCostAndUsageCommand({
      TimePeriod: { Start: period.start, End: period.end },
      Granularity: "DAILY",
      Metrics: ["UnblendedCost"],
      GroupBy: [{ Type: "DIMENSION", Key: "RESOURCE_ID" }],
      Filter: {
        Dimensions: { Key: "SERVICE", Values: ["Amazon Elastic Compute Cloud - Compute"] }
      },
    }));
    return parseAwsCostResponse(response as AwsCostAndUsageResult, period);
  } catch (err: any) {
    // Re-throw credential errors as-is so callers can handle them
    if (err.code === "MISSING_CREDENTIALS") throw err;
    // Wrap SDK/import errors with a clear message — never silently return $0
    throw new Error(
      `[billing-recon:aws] AWS Cost Explorer call failed: ${err.message ?? String(err)}`
    );
  }
}

// =============================================================================
// ReconciliationProvider implementation
// =============================================================================

export const awsReconciliationProvider: ReconciliationProvider = {
  name: "aws",
  settlementWindowMs: TIMING.SETTLEMENT_WINDOW_AWS_MS,
  fetchBilledCosts: fetchAwsBilledCost,
};
