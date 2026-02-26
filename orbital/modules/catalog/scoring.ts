// orbital/modules/catalog/scoring.ts - Scoring interface + stub implementation
//
// D6 foundational: scoring contract types, stub scoring function,
// stub auto-proceed, stub "also consider".
//
// Real scoring is a day-two concern. This establishes the interface
// so that callers (orbital orchestrator, retry-alternative) can depend
// on a stable contract while scoring intelligence is developed iteratively.

import type { ResolvedSpec, ScoredSpec, UserConstraints } from "./types";

// =============================================================================
// Scoring Function (stub)
// =============================================================================

/**
 * Score a set of resolved specs against user constraints.
 *
 * Stub strategy: prefer exact match → cheapest matching → cheapest any.
 *
 * TODO: Replace with diversity+utility scoring.
 * Target: score all possibilities, select DIVERSE subset (not top-N
 * by price). Weight by utility (price, capacity, locality), diversity
 * (spread across providers/regions/tiers), and user history.
 * See docs/equivalence-classes.txt for design direction.
 * Backlog: #ADVISORY-04
 */
export function scoreSpecs(
  resolved: ResolvedSpec[],
  constraints?: UserConstraints,
): ScoredSpec[] {
  return resolved.map(r => {
    const matchScore = computeMatchScore(r);
    const priceScore = computePriceScore(r, constraints);
    const capabilityScore = computeCapabilityScore(r, constraints);

    // Composite: 50% match, 30% price, 20% capability
    const score = Math.round(matchScore * 0.5 + priceScore * 0.3 + capabilityScore * 0.2);

    return {
      resolved: r,
      score,
      priceScore,
      matchScore,
      capabilityScore,
      diversityContribution: 0, // Stub: real scoring uses provider/region/tier spread
      constraintViolations: collectViolations(r, constraints),
      reason: describeScore(r, matchScore, priceScore),
    };
  }).sort((a, b) => b.score - a.score);
}

// =============================================================================
// Auto-proceed (stub)
// =============================================================================

/**
 * Determine if the top-scored result should auto-proceed without user confirmation.
 *
 * Stub: always false — requires collaboratively established precedent.
 *
 * TODO: Auto-proceed when truly unambiguous or user has delegated.
 * Orbital builds knowledge of each user's equivalence classes over
 * time — where no equivalence relation has been identified, decisions
 * cannot be automated, only proposed. Automation requires collaboratively
 * established precedent. Users always decide strategy; orbital recommends
 * tactics.
 * Backlog: #ADVISORY-06
 */
export function shouldAutoProceed(
  _scored: ScoredSpec[],
  _constraints?: UserConstraints,
): boolean {
  // TODO: Check equivalence class precedent from user history
  return false;
}

// =============================================================================
// Also Consider (stub)
// =============================================================================

/**
 * Return alternative specs the user might want to consider.
 *
 * Stub: always empty array.
 *
 * TODO: Surface high-utility options that violate hard constraints
 * when appropriate for user to reconsider. E.g. "Your constraint says
 * us-east-1, but us-west-2 has 3x availability at 40% lower spot."
 * Backlog: #ADVISORY-07
 */
export function alsoConsider(
  _scored: ScoredSpec[],
  _constraints?: UserConstraints,
): ResolvedSpec[] {
  // TODO: Use GPU_ALIASES alternatives, cross-provider price comparison
  return [];
}

// =============================================================================
// Sub-scores
// =============================================================================

function computeMatchScore(r: ResolvedSpec): number {
  switch (r.matchType) {
    case "exact":      return 100;
    case "parse_spec": return r.confidence;
    case "gpu_name":   return r.confidence;
    case "fuzzy":      return r.confidence;
    default:           return 50;
  }
}

function computePriceScore(r: ResolvedSpec, constraints?: UserConstraints): number {
  const price = r.instance.onDemandHourly;
  if (price === null || price === 0) return 50; // Unknown or free (local)

  if (constraints?.maxHourlyBudget) {
    if (price > constraints.maxHourlyBudget) return 0;
    // Closer to budget = lower score
    return Math.round((1 - price / constraints.maxHourlyBudget) * 100);
  }

  // Without budget constraint, invert log-scale price → score
  // $0.01/hr → ~100, $100/hr → ~0
  return Math.max(0, Math.min(100, Math.round(100 - Math.log10(price * 100) * 25)));
}

function computeCapabilityScore(r: ResolvedSpec, constraints?: UserConstraints): number {
  let score = 50; // Base score
  const inst = r.instance;

  if (constraints?.requireGpu && !inst.hasGpu) return 0;
  if (constraints?.requireSpot && !inst.hasSpot) return 0;
  if (constraints?.requireLifecycle && !inst.hasLifecycle) return 0;
  if (constraints?.minGpuMemoryGib && inst.gpuMemoryGib < constraints.minGpuMemoryGib) return 0;
  if (constraints?.minGpuCount && inst.gpuCount < constraints.minGpuCount) return 0;
  if (constraints?.arch && inst.arch !== constraints.arch) return 0;

  if (inst.hasGpu) score += 10;
  if (inst.hasSpot) score += 10;
  if (inst.hasLifecycle) score += 10;
  if (inst.hasNvlink) score += 5;

  // Provider preference bonus
  if (constraints?.preferredProviders?.includes(inst.provider)) score += 15;

  return Math.min(100, score);
}

function collectViolations(r: ResolvedSpec, constraints?: UserConstraints): string[] {
  if (!constraints) return [];
  const violations: string[] = [];
  const inst = r.instance;
  if (constraints.requireGpu && !inst.hasGpu) violations.push("requires GPU");
  if (constraints.requireSpot && !inst.hasSpot) violations.push("requires spot pricing");
  if (constraints.requireLifecycle && !inst.hasLifecycle) violations.push("requires lifecycle support");
  if (constraints.minGpuMemoryGib && inst.gpuMemoryGib < constraints.minGpuMemoryGib)
    violations.push(`requires ${constraints.minGpuMemoryGib}+ GiB GPU memory`);
  if (constraints.minGpuCount && inst.gpuCount < constraints.minGpuCount)
    violations.push(`requires ${constraints.minGpuCount}+ GPUs`);
  if (constraints.arch && inst.arch !== constraints.arch)
    violations.push(`requires ${constraints.arch} architecture`);
  if (constraints.maxHourlyBudget && inst.onDemandHourly !== null && inst.onDemandHourly > constraints.maxHourlyBudget)
    violations.push(`exceeds $${constraints.maxHourlyBudget}/hr budget`);
  return violations;
}

function describeScore(r: ResolvedSpec, matchScore: number, priceScore: number): string {
  const parts: string[] = [];
  if (r.matchType === "exact") parts.push("exact match");
  else parts.push(`${r.matchType} match`);

  if (r.instance.onDemandHourly !== null) {
    parts.push(`$${r.instance.onDemandHourly.toFixed(2)}/hr`);
  }

  if (r.instance.gpuModel) {
    parts.push(`${r.instance.gpuCount}x ${r.instance.gpuModel}`);
  }

  return parts.join(", ");
}
