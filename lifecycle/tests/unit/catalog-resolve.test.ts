// tests/unit/catalog-resolve.test.ts - Lifecycle-side spec validation tests
//
// Tests validateSpec (provider prefix bypass, 503 fallback to passthrough).
// Orbital-internal tests (resolveSpec, scoring, data integrity) live in
// orbital/modules/catalog/catalog.test.ts.

import { describe, test, expect } from "bun:test";
import { validateSpec } from "../../control/src/intent/launch-run";

// =============================================================================
// Power-user prefix bypass (D4)
// =============================================================================

describe("validateSpec: power-user prefix bypass", () => {
  test("aws:p4d.24xlarge bypasses orbital and sets provider", async () => {
    const input = { runId: 1, command: "echo test", spec: "aws:p4d.24xlarge" };
    const result = await validateSpec(input);
    expect(result.provider).toBe("aws");
    expect(result.spec).toBe("p4d.24xlarge");
  });

  test("lambda:gpu_1x_a100 bypasses orbital and sets provider", async () => {
    const input = { runId: 1, command: "echo test", spec: "lambda:gpu_1x_a100" };
    const result = await validateSpec(input);
    expect(result.provider).toBe("lambda");
    expect(result.spec).toBe("gpu_1x_a100");
  });

  test("orbstack:ubuntu:noble:arm64 bypasses orbital and sets provider", async () => {
    const input = { runId: 1, command: "echo test", spec: "orbstack:ubuntu:noble:arm64" };
    const result = await validateSpec(input);
    expect(result.provider).toBe("orbstack");
    expect(result.spec).toBe("ubuntu:noble:arm64");
  });

  test("non-provider prefix is not treated as bypass", async () => {
    // "gpu:a100:1" has colons but "gpu" is not a provider name
    const input = { runId: 1, command: "echo test", spec: "gpu:a100:1" };
    // This should fall through to orbital (which will be unavailable in tests = passthrough)
    const result = await validateSpec(input);
    // When orbital is down, passthrough means input unchanged
    expect(result.spec).toBe("gpu:a100:1");
  });
});

// =============================================================================
// 503 fallback: orbital down → passthrough (D2/D4)
// =============================================================================

describe("validateSpec: orbital down fallback", () => {
  test("when orbital is unavailable, spec passes through unchanged", async () => {
    // In test environment, orbital is not running → resolveSpecViaOrbital returns null
    const input = { runId: 1, command: "echo test", spec: "some-raw-spec" };
    const result = await validateSpec(input);
    // Passthrough: spec unchanged, no error
    expect(result.spec).toBe("some-raw-spec");
    expect(result.provider).toBeUndefined();
  });
});
