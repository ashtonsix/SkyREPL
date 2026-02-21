import { describe, it, expect, beforeEach } from "bun:test";
import { checkRateLimit, getRateLimitInfo, resetRateLimits } from "./rate-limit";

describe("rate-limit", () => {
  beforeEach(() => {
    resetRateLimits();
  });

  it("allows requests under the limit", () => {
    const config = { maxRequests: 5, windowMs: 60_000 };
    for (let i = 0; i < 5; i++) {
      expect(checkRateLimit("token-1", config)).toBeNull();
    }
  });

  it("blocks requests over the limit", () => {
    const config = { maxRequests: 3, windowMs: 60_000 };
    expect(checkRateLimit("token-1", config)).toBeNull();
    expect(checkRateLimit("token-1", config)).toBeNull();
    expect(checkRateLimit("token-1", config)).toBeNull();

    const result = checkRateLimit("token-1", config);
    expect(result).not.toBeNull();
    expect(result!.remaining).toBe(0);
    expect(result!.retryAfter).toBeGreaterThan(0);
    expect(result!.limit).toBe(3);
  });

  it("tracks tokens independently", () => {
    const config = { maxRequests: 2, windowMs: 60_000 };
    expect(checkRateLimit("token-a", config)).toBeNull();
    expect(checkRateLimit("token-a", config)).toBeNull();
    expect(checkRateLimit("token-a", config)).not.toBeNull(); // blocked

    // Different token still has capacity
    expect(checkRateLimit("token-b", config)).toBeNull();
    expect(checkRateLimit("token-b", config)).toBeNull();
  });

  it("reports remaining correctly", () => {
    const config = { maxRequests: 5, windowMs: 60_000 };
    checkRateLimit("token-1", config);
    checkRateLimit("token-1", config);

    const info = getRateLimitInfo("token-1", config);
    expect(info.limit).toBe(5);
    expect(info.remaining).toBe(3);
    expect(info.reset).toBeGreaterThan(0);
  });

  it("returns full capacity for unknown token", () => {
    const config = { maxRequests: 10, windowMs: 60_000 };
    const info = getRateLimitInfo("unknown", config);
    expect(info.remaining).toBe(10);
  });
});
