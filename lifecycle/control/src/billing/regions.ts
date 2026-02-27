// billing/regions.ts — Provider-specific region normalization
//
// Maps provider-specific region strings to canonical display names.

// =============================================================================
// Region Maps
// =============================================================================

const AWS_REGIONS: Record<string, string> = {
  "us-east-1":      "US East",
  "us-east-2":      "US East",
  "us-west-1":      "US West",
  "us-west-2":      "US West",
  "ca-central-1":   "Canada",
  "eu-west-1":      "EU West",
  "eu-west-2":      "EU West",
  "eu-west-3":      "EU West",
  "eu-central-1":   "EU Central",
  "eu-north-1":     "EU North",
  "ap-southeast-1": "Asia Pacific",
  "ap-southeast-2": "Asia Pacific",
  "ap-northeast-1": "Asia Pacific",
  "ap-northeast-2": "Asia Pacific",
  "ap-south-1":     "Asia Pacific",
  "sa-east-1":      "South America",
};

const DO_REGIONS: Record<string, string> = {
  nyc1: "US East",
  nyc2: "US East",
  nyc3: "US East",
  sfo1: "US West",
  sfo2: "US West",
  sfo3: "US West",
  tor1: "Canada",
  ams2: "EU West",
  ams3: "EU West",
  lon1: "EU West",
  fra1: "EU Central",
  blr1: "Asia Pacific",
  sgp1: "Asia Pacific",
  syd1: "Asia Pacific",
};

const LAMBDA_REGIONS: Record<string, string> = {
  "us-east-1":      "US East",
  "us-west-1":      "US West",
  "us-west-2":      "US West",
  "us-south-1":     "US South",
  "us-midwest-1":   "US Midwest",
  "eu-central-1":   "EU Central",
  "ap-southeast-1": "Asia Pacific",
  "me-west-1":      "Middle East",
  "asia-southeast-1": "Asia Pacific",
};

const RUNPOD_REGIONS: Record<string, string> = {
  US:  "US",
  EU:  "EU",
  CA:  "Canada",
  AU:  "Asia Pacific",
  SG:  "Asia Pacific",
};

// =============================================================================
// normalizeRegion
// =============================================================================

/**
 * Normalize a provider-specific region string to a canonical display name.
 * Falls back to the raw region string if no mapping found.
 */
export function normalizeRegion(provider: string, raw_region: string): string {
  const key = raw_region.trim();

  switch (provider) {
    case "aws": {
      return AWS_REGIONS[key] ?? key;
    }
    case "digitalocean": {
      // DO regions are lowercase city codes (nyc1, sfo2, etc.)
      return DO_REGIONS[key.toLowerCase()] ?? key;
    }
    case "lambda":
    case "lambdalabs": {
      return LAMBDA_REGIONS[key] ?? key;
    }
    case "runpod": {
      // RunPod uses short uppercase codes
      return RUNPOD_REGIONS[key.toUpperCase()] ?? key;
    }
    case "orbstack": {
      return "Local";
    }
    case "skyrepl_infra": {
      return "Global";
    }
    default:
      return key;
  }
}
