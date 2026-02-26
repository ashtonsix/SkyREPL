// tests/unit/provider-config.test.ts - Provider config tests (D3)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, rmSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "path";
import { mkdtemp } from "fs/promises";
import { tmpdir } from "os";
import {
  loadProviderConfig,
  clearConfigCache,
  getEffectiveAWSConfig,
  getEffectiveLambdaConfig,
  getEffectiveDigitalOceanConfig,
  getEffectiveOrbStackConfig,
  getEnabledProviders,
} from "../../control/src/provider/config";

let tempDir: string;

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), "skyrepl-config-test-"));
  clearConfigCache();
});

afterEach(() => {
  delete process.env.SKYREPL_PROVIDERS_CONFIG;
  delete process.env.AWS_ACCESS_KEY_ID;
  delete process.env.AWS_SECRET_ACCESS_KEY;
  delete process.env.AWS_DEFAULT_REGION;
  delete process.env.LAMBDA_API_KEY;
  delete process.env.DIGITALOCEAN_TOKEN;
  clearConfigCache();
  if (existsSync(tempDir)) {
    rmSync(tempDir, { recursive: true });
  }
});

describe("loadProviderConfig", () => {
  test("returns empty config when file does not exist", () => {
    process.env.SKYREPL_PROVIDERS_CONFIG = join(tempDir, "nonexistent.toml");
    const config = loadProviderConfig();
    expect(config).toEqual({});
  });

  test("parses TOML config file", () => {
    const configPath = join(tempDir, "providers.toml");
    writeFileSync(configPath, `
[aws]
enabled = true
region = "us-west-2"
access_key_id = "AKIA123"
secret_access_key = "secret123"

[lambda]
enabled = true
api_key = "lambda-key-123"
region = "us-east-1"
ssh_key_name = "my-key"

[digitalocean]
enabled = false
token = "do-token-123"
region = "nyc3"

[orbstack]
enabled = true
`);
    process.env.SKYREPL_PROVIDERS_CONFIG = configPath;
    const config = loadProviderConfig();

    expect(config.aws?.enabled).toBe(true);
    expect(config.aws?.region).toBe("us-west-2");
    expect(config.aws?.access_key_id).toBe("AKIA123");
    expect(config.aws?.secret_access_key).toBe("secret123");
    expect(config.lambda?.api_key).toBe("lambda-key-123");
    expect(config.digitalocean?.enabled).toBe(false);
    expect(config.orbstack?.enabled).toBe(true);
  });

  test("handles comments and empty lines", () => {
    const configPath = join(tempDir, "providers.toml");
    writeFileSync(configPath, `
# This is a comment
[aws]
# region comment
enabled = true
region = "eu-west-1"
`);
    process.env.SKYREPL_PROVIDERS_CONFIG = configPath;
    const config = loadProviderConfig();

    expect(config.aws?.enabled).toBe(true);
    expect(config.aws?.region).toBe("eu-west-1");
  });

  test("caches config after first load", () => {
    const configPath = join(tempDir, "providers.toml");
    writeFileSync(configPath, `
[aws]
enabled = true
region = "us-east-1"
`);
    process.env.SKYREPL_PROVIDERS_CONFIG = configPath;

    const first = loadProviderConfig();
    // Modify the file
    writeFileSync(configPath, `
[aws]
enabled = false
region = "eu-west-1"
`);
    const second = loadProviderConfig();

    // Should be same (cached)
    expect(second.aws?.enabled).toBe(true);
    expect(second.aws?.region).toBe("us-east-1");

    // After clearing cache, should reload
    clearConfigCache();
    const third = loadProviderConfig();
    expect(third.aws?.enabled).toBe(false);
    expect(third.aws?.region).toBe("eu-west-1");
  });
});

describe("getEffectiveAWSConfig", () => {
  test("TOML overrides env vars", () => {
    const configPath = join(tempDir, "providers.toml");
    writeFileSync(configPath, `
[aws]
enabled = true
region = "eu-central-1"
`);
    process.env.SKYREPL_PROVIDERS_CONFIG = configPath;
    process.env.AWS_DEFAULT_REGION = "us-east-1";

    const config = getEffectiveAWSConfig();
    expect(config.region).toBe("eu-central-1");
  });

  test("falls back to env vars when no TOML", () => {
    process.env.SKYREPL_PROVIDERS_CONFIG = join(tempDir, "nonexistent.toml");
    process.env.AWS_DEFAULT_REGION = "ap-southeast-1";
    process.env.AWS_ACCESS_KEY_ID = "AKIA_ENV";

    const config = getEffectiveAWSConfig();
    expect(config.region).toBe("ap-southeast-1");
    expect(config.access_key_id).toBe("AKIA_ENV");
    expect(config.enabled).toBe(true);
  });

  test("defaults to us-east-1 with no config", () => {
    process.env.SKYREPL_PROVIDERS_CONFIG = join(tempDir, "nonexistent.toml");
    const config = getEffectiveAWSConfig();
    expect(config.region).toBe("us-east-1");
    expect(config.enabled).toBe(false);
  });
});

describe("getEnabledProviders", () => {
  test("orbstack enabled by default", () => {
    process.env.SKYREPL_PROVIDERS_CONFIG = join(tempDir, "nonexistent.toml");
    const enabled = getEnabledProviders();
    expect(enabled).toContain("orbstack");
  });

  test("providers enabled from TOML", () => {
    const configPath = join(tempDir, "providers.toml");
    writeFileSync(configPath, `
[aws]
enabled = true

[lambda]
enabled = true

[digitalocean]
enabled = false

[orbstack]
enabled = true
`);
    process.env.SKYREPL_PROVIDERS_CONFIG = configPath;

    const enabled = getEnabledProviders();
    expect(enabled).toContain("aws");
    expect(enabled).toContain("lambda");
    expect(enabled).toContain("orbstack");
    expect(enabled).not.toContain("digitalocean");
  });
});
