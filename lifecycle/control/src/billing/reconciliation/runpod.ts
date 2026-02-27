// billing/reconciliation/runpod.ts — RunPod billing API adapter (WL-061-4A)
//
// Fetches billed costs from RunPod GraphQL API (getGpuJobs query).
// Real API calls require a RunPod API key.
// Settlement window: 24 hours.
//
// API reference: https://docs.runpod.io/references/graphql-api

import type { ProviderCostEntry, ReconciliationProvider } from "./types";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// RunPod GraphQL response shape
// =============================================================================

interface RunPodGpuJob {
  /** RunPod pod/job ID */
  id: string;
  /** ISO 8601 timestamp when the job was created */
  createdAt: string;
  /** ISO 8601 timestamp when the job was terminated */
  terminatedAt?: string;
  /** Cost per hour in USD (float) */
  costPerHr?: number;
  /** Total cost in USD (float) */
  totalCost?: number;
  /** GPU type string */
  gpuTypeId?: string;
  /** Number of GPUs */
  gpuCount?: number;
  /** Region/datacenter */
  dataCenterId?: string;
}

interface RunPodGraphQLResponse {
  data?: {
    myself?: {
      pods?: RunPodGpuJob[];
    };
  };
  errors?: Array<{ message: string }>;
}

// =============================================================================
// Credential check
// =============================================================================

function checkRunPodCredentials(): void {
  if (!process.env.RUNPOD_API_KEY) {
    const err = new Error(
      `[billing-recon:runpod] Missing RunPod API key. ` +
      `Set RUNPOD_API_KEY to enable RunPod billing reconciliation.`
    );
    (err as any).code = "MISSING_CREDENTIALS";
    throw err;
  }
}

// =============================================================================
// Parse RunPod GraphQL job list into ProviderCostEntry[]
// =============================================================================

function parseRunPodJobs(
  jobs: RunPodGpuJob[],
  period: { start: string; end: string }
): ProviderCostEntry[] {
  const entries: ProviderCostEntry[] = [];
  const periodStartMs = new Date(period.start).getTime();
  const periodEndMs = new Date(period.end).getTime();

  for (const job of jobs) {
    const jobStartMs = new Date(job.createdAt).getTime();
    if (isNaN(jobStartMs)) continue;

    const jobEndMs = job.terminatedAt
      ? new Date(job.terminatedAt).getTime()
      : periodEndMs; // still running — use period end as estimate

    if (isNaN(jobEndMs)) continue;

    // Include jobs that overlap with the requested period
    if (jobStartMs >= periodEndMs || jobEndMs <= periodStartMs) continue;

    // Prefer totalCost if available; otherwise compute from costPerHr × duration
    let billedCents: number;
    if (typeof job.totalCost === "number") {
      billedCents = Math.round(job.totalCost * 100);
    } else if (typeof job.costPerHr === "number") {
      const durationMs = Math.min(jobEndMs, periodEndMs) - Math.max(jobStartMs, periodStartMs);
      const durationHrs = durationMs / 3_600_000;
      billedCents = Math.round(durationHrs * job.costPerHr * 100);
    } else {
      // No cost data — skip
      continue;
    }

    if (billedCents < 0) continue;

    entries.push({
      provider: "runpod",
      provider_resource_id: job.id,
      period_start_ms: Math.max(jobStartMs, periodStartMs),
      period_end_ms: Math.min(jobEndMs, periodEndMs),
      billed_amount_cents: billedCents,
      currency: "USD",
      raw_data: { job },
    });
  }

  return entries;
}

// =============================================================================
// fetchRunPodBilledCost
// =============================================================================

/**
 * Fetch billed costs from RunPod GraphQL API for the given period.
 *
 * In production: calls the RunPod GraphQL endpoint with a `myself { pods }` query
 * to retrieve all GPU jobs, then filters to the requested period.
 *
 * Requires: RUNPOD_API_KEY environment variable.
 *
 * @throws {Error} with code "MISSING_CREDENTIALS" if RunPod API key is not configured.
 */
export async function fetchRunPodBilledCost(
  period: { start: string; end: string }
): Promise<ProviderCostEntry[]> {
  checkRunPodCredentials();

  // In production this would be:
  //
  //   const apiKey = process.env.RUNPOD_API_KEY!;
  //   const query = `
  //     query {
  //       myself {
  //         pods {
  //           id
  //           createdAt
  //           terminatedAt
  //           costPerHr
  //           totalCost
  //           gpuTypeId
  //           gpuCount
  //           dataCenterId
  //         }
  //       }
  //     }
  //   `;
  //   const response = await fetch("https://api.runpod.io/graphql", {
  //     method: "POST",
  //     headers: {
  //       "Content-Type": "application/json",
  //       Authorization: `Bearer ${apiKey}`,
  //     },
  //     body: JSON.stringify({ query }),
  //   });
  //   if (!response.ok) {
  //     throw new Error(`[billing-recon:runpod] API error ${response.status}: ${await response.text()}`);
  //   }
  //   const data: RunPodGraphQLResponse = await response.json();
  //   if (data.errors?.length) {
  //     throw new Error(`[billing-recon:runpod] GraphQL errors: ${data.errors.map(e => e.message).join(", ")}`);
  //   }
  //   const jobs = data.data?.myself?.pods ?? [];
  //   return parseRunPodJobs(jobs, period);

  // Stub: return a mock response shaped like the real API
  const mockResponse: RunPodGraphQLResponse = {
    data: {
      myself: {
        pods: [],
      },
    },
  };

  const jobs = mockResponse.data?.myself?.pods ?? [];
  return parseRunPodJobs(jobs, period);
}

// =============================================================================
// ReconciliationProvider implementation
// =============================================================================

export const runpodReconciliationProvider: ReconciliationProvider = {
  name: "runpod",
  settlementWindowMs: TIMING.SETTLEMENT_WINDOW_RUNPOD_MS,
  fetchBilledCosts: fetchRunPodBilledCost,
};
