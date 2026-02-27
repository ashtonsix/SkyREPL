// billing/regions.test.ts — Region normalization tests

import { describe, test, expect } from "bun:test";
import { normalizeRegion } from "./regions";

describe("AWS region normalization", () => {
  test("us-east-1 => US East", () => {
    expect(normalizeRegion("aws", "us-east-1")).toBe("US East");
  });
  test("us-west-2 => US West", () => {
    expect(normalizeRegion("aws", "us-west-2")).toBe("US West");
  });
  test("eu-central-1 => EU Central", () => {
    expect(normalizeRegion("aws", "eu-central-1")).toBe("EU Central");
  });
  test("ap-southeast-1 => Asia Pacific", () => {
    expect(normalizeRegion("aws", "ap-southeast-1")).toBe("Asia Pacific");
  });
  test("unknown region falls back to raw value", () => {
    expect(normalizeRegion("aws", "af-south-1")).toBe("af-south-1");
  });
});

describe("DigitalOcean region normalization", () => {
  test("nyc1 => US East", () => {
    expect(normalizeRegion("digitalocean", "nyc1")).toBe("US East");
  });
  test("sfo3 => US West", () => {
    expect(normalizeRegion("digitalocean", "sfo3")).toBe("US West");
  });
  test("ams3 => EU West", () => {
    expect(normalizeRegion("digitalocean", "ams3")).toBe("EU West");
  });
  test("sgp1 => Asia Pacific", () => {
    expect(normalizeRegion("digitalocean", "sgp1")).toBe("Asia Pacific");
  });
  test("unknown region falls back to raw value", () => {
    expect(normalizeRegion("digitalocean", "xyz1")).toBe("xyz1");
  });
});

describe("Lambda Labs region normalization", () => {
  test("us-east-1 => US East", () => {
    expect(normalizeRegion("lambda", "us-east-1")).toBe("US East");
  });
  test("eu-central-1 => EU Central", () => {
    expect(normalizeRegion("lambda", "eu-central-1")).toBe("EU Central");
  });
  test("lambdalabs alias", () => {
    expect(normalizeRegion("lambdalabs", "us-east-1")).toBe("US East");
  });
});

describe("RunPod region normalization", () => {
  test("US => US", () => {
    expect(normalizeRegion("runpod", "US")).toBe("US");
  });
  test("EU => EU", () => {
    expect(normalizeRegion("runpod", "EU")).toBe("EU");
  });
  test("lowercase us => US", () => {
    expect(normalizeRegion("runpod", "us")).toBe("US");
  });
});

describe("OrbStack region normalization", () => {
  test("any region => Local", () => {
    expect(normalizeRegion("orbstack", "local")).toBe("Local");
    expect(normalizeRegion("orbstack", "any-region")).toBe("Local");
  });
});

describe("skyrepl_infra region normalization", () => {
  test("any region => Global", () => {
    expect(normalizeRegion("skyrepl_infra", "us")).toBe("Global");
  });
});

describe("unknown provider", () => {
  test("falls back to raw region", () => {
    expect(normalizeRegion("unknown_provider", "us-west-1")).toBe("us-west-1");
  });
});
