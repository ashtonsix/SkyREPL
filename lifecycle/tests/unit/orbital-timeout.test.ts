import { describe, test, expect } from "bun:test";
import { resolveSpecViaOrbital } from "../../control/src/provider/orbital";

describe("orbital timeout", () => {
  test("orbital call returns quickly (100ms timeout in tests)", async () => {
    const start = Date.now();
    const result = await resolveSpecViaOrbital("ubuntu", "orbstack", "local");
    const elapsed = Date.now() - start;
    expect(result).toBeNull(); // orbital not running
    expect(elapsed).toBeLessThan(500); // should be ~100ms not 3000ms
  });
});
