// db/credits.test.ts — Tests for credit wallet operations (WL-061-3B §8)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../../tests/harness";
import { creditDeposit, creditDebit, getCreditBalance } from "./credits";
import { getAuditEvents } from "./audit";

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({}); });
afterEach(() => cleanup());

describe("getCreditBalance", () => {
  test("returns 0 for nonexistent wallet", () => {
    const balance = getCreditBalance(1);
    expect(balance).toBe(0);
  });
});

describe("creditDeposit", () => {
  test("increases balance from 0 to deposited amount", () => {
    creditDeposit(1, 5000);
    expect(getCreditBalance(1)).toBe(5000);
  });

  test("accumulates multiple deposits", () => {
    creditDeposit(1, 1000);
    creditDeposit(1, 2000);
    expect(getCreditBalance(1)).toBe(3000);
  });

  test("creates wallet row if it does not exist", () => {
    // Balance should be 0 before deposit
    expect(getCreditBalance(1)).toBe(0);
    creditDeposit(1, 500);
    expect(getCreditBalance(1)).toBe(500);
  });

  test("emits credit_purchase audit event on deposit", () => {
    creditDeposit(1, 2500);
    const events = getAuditEvents({ tenant_id: 1, event_type: "credit_purchase" });
    expect(events.length).toBe(1);
    const evt = events[0]!;
    expect(evt.is_attribution).toBe(1);
    const data = JSON.parse(evt.data);
    expect(data.amount_cents).toBe(2500);
    expect(data.currency).toBe("USD");
  });

  test("throws for zero or negative amount", () => {
    expect(() => creditDeposit(1, 0)).toThrow();
    expect(() => creditDeposit(1, -100)).toThrow();
  });
});

describe("creditDebit", () => {
  test("decreases balance", () => {
    creditDeposit(1, 5000);
    creditDebit(1, 2000);
    expect(getCreditBalance(1)).toBe(3000);
  });

  test("allows overdraft (balance goes negative)", () => {
    creditDeposit(1, 100);
    creditDebit(1, 500);
    expect(getCreditBalance(1)).toBe(-400);
  });

  test("creates wallet row if it does not exist (debiting from zero)", () => {
    creditDebit(1, 300);
    expect(getCreditBalance(1)).toBe(-300);
  });

  test("throws for zero or negative amount", () => {
    expect(() => creditDebit(1, 0)).toThrow();
    expect(() => creditDebit(1, -100)).toThrow();
  });
});
