// provider/config.ts - Read ~/.repl/providers.toml provider configuration
//
// Supports a TOML-like configuration file for provider credentials and settings.
// Falls back to environment variables when config file is absent.

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { homedir } from "os";

// =============================================================================
// Provider Config Types
// =============================================================================

export interface ProviderConfig {
  aws?: AWSConfig;
  lambda?: LambdaConfig;
  digitalocean?: DigitalOceanConfig;
  orbstack?: OrbStackConfig;
}

export interface AWSConfig {
  enabled?: boolean;
  region?: string;
  access_key_id?: string;
  secret_access_key?: string;
  default_subnet_id?: string;
  default_security_group_ids?: string[];
  default_key_name?: string;
  default_iam_profile?: string;
}

export interface LambdaConfig {
  enabled?: boolean;
  api_key?: string;
  region?: string;
  ssh_key_name?: string;
}

export interface DigitalOceanConfig {
  enabled?: boolean;
  token?: string;
  region?: string;
  default_size?: string;
  ssh_key_ids?: number[];
  vpc_uuid?: string;
}

export interface OrbStackConfig {
  enabled?: boolean;
}

// =============================================================================
// Config File Path
// =============================================================================

function getConfigPath(): string {
  return process.env.SKYREPL_PROVIDERS_CONFIG ?? join(homedir(), ".repl", "providers.toml");
}

// =============================================================================
// Simple TOML Parser (subset: sections + key=value pairs)
// =============================================================================

/**
 * Parse a minimal TOML-like config. Supports:
 * - [section] headers
 * - key = value (strings, numbers, booleans)
 * - key = "quoted string"
 * - key = [array, of, values]
 * - # comments
 */
function parseSimpleToml(content: string): Record<string, Record<string, unknown>> {
  const result: Record<string, Record<string, unknown>> = {};
  let currentSection = "__global__";
  result[currentSection] = {};

  for (const rawLine of content.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;

    // Section header: [name]
    const sectionMatch = line.match(/^\[([a-zA-Z0-9_.-]+)\]$/);
    if (sectionMatch) {
      currentSection = sectionMatch[1]!;
      if (!result[currentSection]) result[currentSection] = {};
      continue;
    }

    // Key = value
    const eqIdx = line.indexOf("=");
    if (eqIdx === -1) continue;

    const key = line.slice(0, eqIdx).trim();
    let value = line.slice(eqIdx + 1).trim();

    if (!result[currentSection]) result[currentSection] = {};

    // Parse value
    result[currentSection]![key] = parseTomlValue(value);
  }

  return result;
}

/**
 * Strip an inline comment (text after an unquoted `#`) from a raw TOML value.
 * A `#` inside a double-quoted string is not a comment delimiter.
 * Must be applied before type dispatch so arrays and quoted strings are not
 * broken by trailing comments such as `[1, 2] # keys` or `"v" # note`.
 */
function stripInlineComment(value: string): string {
  let inQuote = false;
  for (let i = 0; i < value.length; i++) {
    if (value[i] === '"' && (i === 0 || value[i - 1] !== '\\')) inQuote = !inQuote;
    if (value[i] === '#' && !inQuote) return value.slice(0, i).trim();
  }
  return value.trim();
}

function parseTomlValue(value: string): unknown {
  // Strip inline comments before any type dispatch so that
  // `[1, 2] # comment` and `"str" # comment` are handled correctly.
  value = stripInlineComment(value);

  // Quoted string
  if ((value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))) {
    return value.slice(1, -1);
  }

  // Array: [a, b, c]
  if (value.startsWith("[") && value.endsWith("]")) {
    const inner = value.slice(1, -1).trim();
    if (!inner) return [];
    return inner.split(",").map(item => parseTomlValue(item.trim()));
  }

  // Boolean
  if (value === "true") return true;
  if (value === "false") return false;

  // Number
  const num = Number(value);
  if (!isNaN(num) && value !== "") return num;

  // Bare string (comment already stripped above)
  return value;
}

// =============================================================================
// Load Config
// =============================================================================

let _cachedConfig: ProviderConfig | null = null;

/**
 * Load provider configuration from ~/.repl/providers.toml.
 * Falls back to empty config if file doesn't exist.
 * Cached after first load.
 */
export function loadProviderConfig(): ProviderConfig {
  if (_cachedConfig) return _cachedConfig;

  const configPath = getConfigPath();
  if (!existsSync(configPath)) {
    _cachedConfig = {};
    return _cachedConfig;
  }

  try {
    const content = readFileSync(configPath, "utf-8");
    const raw = parseSimpleToml(content);
    _cachedConfig = mapToProviderConfig(raw);
  } catch (err) {
    console.warn(`[config] Failed to parse ${configPath}: ${err instanceof Error ? err.message : String(err)}`);
    _cachedConfig = {};
  }

  return _cachedConfig;
}

/** Clear the cached config. Test-only. */
export function clearConfigCache(): void {
  _cachedConfig = null;
}

function mapToProviderConfig(raw: Record<string, Record<string, unknown>>): ProviderConfig {
  const config: ProviderConfig = {};

  if (raw.aws) {
    config.aws = {
      enabled: raw.aws.enabled as boolean | undefined,
      region: raw.aws.region as string | undefined,
      access_key_id: raw.aws.access_key_id as string | undefined,
      secret_access_key: raw.aws.secret_access_key as string | undefined,
      default_subnet_id: raw.aws.default_subnet_id as string | undefined,
      default_security_group_ids: raw.aws.default_security_group_ids as string[] | undefined,
      default_key_name: raw.aws.default_key_name as string | undefined,
      default_iam_profile: raw.aws.default_iam_profile as string | undefined,
    };
  }

  if (raw.lambda) {
    config.lambda = {
      enabled: raw.lambda.enabled as boolean | undefined,
      api_key: raw.lambda.api_key as string | undefined,
      region: raw.lambda.region as string | undefined,
      ssh_key_name: raw.lambda.ssh_key_name as string | undefined,
    };
  }

  if (raw.digitalocean) {
    config.digitalocean = {
      enabled: raw.digitalocean.enabled as boolean | undefined,
      token: raw.digitalocean.token as string | undefined,
      region: raw.digitalocean.region as string | undefined,
      default_size: raw.digitalocean.default_size as string | undefined,
      ssh_key_ids: raw.digitalocean.ssh_key_ids as number[] | undefined,
      vpc_uuid: raw.digitalocean.vpc_uuid as string | undefined,
    };
  }

  if (raw.orbstack) {
    config.orbstack = {
      enabled: raw.orbstack.enabled as boolean | undefined,
    };
  }

  return config;
}

// =============================================================================
// Config Merge: TOML → env vars → defaults
// =============================================================================

/**
 * Get effective AWS config: providers.toml > env vars > defaults.
 */
export function getEffectiveAWSConfig(): AWSConfig {
  const file = loadProviderConfig().aws ?? {};
  return {
    enabled: file.enabled ?? (!!process.env.AWS_ACCESS_KEY_ID || !!process.env.AWS_DEFAULT_REGION),
    region: file.region ?? process.env.AWS_DEFAULT_REGION ?? process.env.AWS_REGION ?? "us-east-1",
    access_key_id: file.access_key_id ?? process.env.AWS_ACCESS_KEY_ID,
    secret_access_key: file.secret_access_key ?? process.env.AWS_SECRET_ACCESS_KEY,
    default_subnet_id: file.default_subnet_id ?? process.env.AWS_DEFAULT_SUBNET,
    default_key_name: file.default_key_name,
    default_iam_profile: file.default_iam_profile,
  };
}

/**
 * Get effective Lambda config: providers.toml > env vars > defaults.
 */
export function getEffectiveLambdaConfig(): LambdaConfig {
  const file = loadProviderConfig().lambda ?? {};
  return {
    enabled: file.enabled ?? !!process.env.LAMBDA_API_KEY,
    api_key: file.api_key ?? process.env.LAMBDA_API_KEY ?? "",
    region: file.region ?? process.env.LAMBDA_REGION ?? "us-east-1",
    ssh_key_name: file.ssh_key_name ?? process.env.LAMBDA_SSH_KEY_NAME ?? "",
  };
}

/**
 * Get effective DigitalOcean config: providers.toml > env vars > defaults.
 */
export function getEffectiveDigitalOceanConfig(): DigitalOceanConfig {
  const file = loadProviderConfig().digitalocean ?? {};
  const envKeyIds = process.env.DO_SSH_KEY_IDS
    ? process.env.DO_SSH_KEY_IDS.split(",").map(Number).filter(n => !isNaN(n))
    : undefined;
  return {
    enabled: file.enabled ?? !!process.env.DIGITALOCEAN_TOKEN,
    token: file.token ?? process.env.DIGITALOCEAN_TOKEN ?? "",
    region: file.region ?? process.env.DO_DEFAULT_REGION ?? "nyc3",
    default_size: file.default_size ?? process.env.DO_DEFAULT_SIZE,
    ssh_key_ids: file.ssh_key_ids ?? envKeyIds,
    vpc_uuid: file.vpc_uuid ?? process.env.DO_VPC_UUID,
  };
}

/**
 * Get effective OrbStack config: providers.toml > defaults.
 */
export function getEffectiveOrbStackConfig(): OrbStackConfig {
  const file = loadProviderConfig().orbstack ?? {};
  return {
    enabled: file.enabled ?? true, // OrbStack is always enabled by default (local dev)
  };
}

/**
 * List which providers are enabled.
 */
export function getEnabledProviders(): string[] {
  const enabled: string[] = [];
  if (getEffectiveOrbStackConfig().enabled) enabled.push("orbstack");
  if (getEffectiveAWSConfig().enabled) enabled.push("aws");
  if (getEffectiveLambdaConfig().enabled) enabled.push("lambda");
  if (getEffectiveDigitalOceanConfig().enabled) enabled.push("digitalocean");
  return enabled;
}
