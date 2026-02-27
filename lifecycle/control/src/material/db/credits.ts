// db/credits.ts — Credit wallet operations (WL-061-3B §2)
//
// Credit wallets hold pre-purchased balance (cents) for a tenant.
// All amounts are integer cents; no floating-point dollars.

import { queryOne, execute } from "./helpers";
import { emitAuditEvent } from "./audit";

// =============================================================================
// Credit Wallet Operations
// =============================================================================

/**
 * Deposit funds into a tenant's USD credit wallet.
 * Creates the wallet row if it does not yet exist (upsert pattern).
 * Emits a credit_purchase audit event with is_attribution: true.
 */
export function creditDeposit(tenantId: number, amountCents: number): void {
  if (amountCents <= 0) {
    throw new Error("creditDeposit: amountCents must be positive");
  }

  const now = Date.now();

  // INSERT OR IGNORE to create row if absent, then UPDATE balance
  execute(
    `INSERT OR IGNORE INTO credit_wallets (tenant_id, currency, balance_cents, updated_at)
     VALUES (?, 'USD', 0, ?)`,
    [tenantId, now]
  );

  execute(
    `UPDATE credit_wallets
     SET balance_cents = balance_cents + ?, updated_at = ?
     WHERE tenant_id = ? AND currency = 'USD'`,
    [amountCents, now, tenantId]
  );

  // Emit audit event for the deposit
  emitAuditEvent({
    event_type: "credit_purchase",
    tenant_id: tenantId,
    source: "credit_wallet",
    is_attribution: true,
    data: {
      amount_cents: amountCents,
      currency: "USD",
    },
    occurred_at: now,
  });
}

/**
 * Debit funds from a tenant's USD credit wallet.
 * Does not enforce a floor — balance may go negative (overdraft tracking).
 */
export function creditDebit(tenantId: number, amountCents: number): void {
  if (amountCents <= 0) {
    throw new Error("creditDebit: amountCents must be positive");
  }

  const now = Date.now();

  // INSERT OR IGNORE ensures the wallet row exists before the UPDATE
  execute(
    `INSERT OR IGNORE INTO credit_wallets (tenant_id, currency, balance_cents, updated_at)
     VALUES (?, 'USD', 0, ?)`,
    [tenantId, now]
  );

  execute(
    `UPDATE credit_wallets
     SET balance_cents = balance_cents - ?, updated_at = ?
     WHERE tenant_id = ? AND currency = 'USD'`,
    [amountCents, now, tenantId]
  );
}

/**
 * Return the current USD balance in cents for a tenant.
 * Returns 0 if no wallet exists.
 */
export function getCreditBalance(tenantId: number): number {
  const row = queryOne<{ balance_cents: number }>(
    `SELECT balance_cents FROM credit_wallets
     WHERE tenant_id = ? AND currency = 'USD'`,
    [tenantId]
  );
  return row?.balance_cents ?? 0;
}
