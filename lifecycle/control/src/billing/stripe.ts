// billing/stripe.ts — Stripe integration (WL-061-4B §4)
//
// All Stripe operations are behind this module for easy mocking in tests.
// The Stripe client is lazy-initialized from STRIPE_SECRET_KEY.
// If the env var is not set, all operations throw a clear error.

import type { SettlementBatch, SettlementLineItem } from "./settlement";
import { execute } from "../material/db";

// =============================================================================
// Types
// =============================================================================

export interface StripeInvoiceResult {
  invoice_id: string;
  status: string;
  hosted_invoice_url: string | null;
}

// =============================================================================
// Stripe Client (lazy singleton, injectable for tests)
// =============================================================================

let _stripeClient: any = null;
let _stripeClientFactory: ((secretKey: string) => any) | null = null;

/**
 * Override the Stripe client factory for testing.
 * Call before createStripeInvoice() in tests to inject a mock.
 */
export function setStripeClientFactory(factory: (secretKey: string) => any): void {
  _stripeClientFactory = factory;
  _stripeClient = null; // reset cached client so factory is used next time
}

/**
 * Reset to the real Stripe client (clears test injection).
 */
export function resetStripeClientFactory(): void {
  _stripeClientFactory = null;
  _stripeClient = null;
}

/**
 * Get (or lazy-create) the Stripe client.
 * Throws a clear error if STRIPE_SECRET_KEY is not set.
 */
export async function getStripeClient(): Promise<any> {
  if (_stripeClient) return _stripeClient;

  const secretKey = process.env.STRIPE_SECRET_KEY;
  if (!secretKey) {
    throw new Error(
      "Stripe not configured: STRIPE_SECRET_KEY environment variable is not set"
    );
  }

  if (_stripeClientFactory) {
    _stripeClient = _stripeClientFactory(secretKey);
  } else {
    // Real Stripe client — dynamic import avoids loading at module parse time
    // (allows tests that don't use Stripe to run without the env var)
    const { default: Stripe } = await import("stripe");
    _stripeClient = new Stripe(secretKey, { apiVersion: "2026-02-25.clover" });
  }

  return _stripeClient;
}

// =============================================================================
// createStripeInvoice
// =============================================================================

/**
 * Create a Stripe invoice for a settled batch.
 * Each line item becomes a Stripe invoice item.
 * All line items and the invoice carry skyrepl_settlement_batch_id in metadata.
 *
 * Requires:
 *   - STRIPE_SECRET_KEY env var
 *   - STRIPE_CUSTOMER_ID env var (v1: single customer per deployment)
 *
 * After successful finalization, updates settlement_batches in SQLite:
 *   status = 'invoiced', stripe_invoice_id = <invoice.id>
 */
export async function createStripeInvoice(
  batch: SettlementBatch,
  lineItems: SettlementLineItem[]
): Promise<StripeInvoiceResult> {
  const stripe = await getStripeClient();

  // v1: require STRIPE_CUSTOMER_ID env var
  const customerId = process.env.STRIPE_CUSTOMER_ID;
  if (!customerId) {
    throw new Error(
      "Stripe not configured: STRIPE_CUSTOMER_ID environment variable is not set"
    );
  }

  const metadata = { skyrepl_settlement_batch_id: batch.batch_uuid };

  // 1. Create the invoice FIRST
  const invoice = await stripe.invoices.create({
    customer: customerId,
    metadata,
    auto_advance: false, // we finalize explicitly
    collection_method: "charge_automatically",
    description: `SkyREPL billing: period ${new Date(batch.period_start_ms).toISOString().slice(0, 10)} to ${new Date(batch.period_end_ms).toISOString().slice(0, 10)}`,
  });

  // 2. Create invoice items bound to the invoice
  for (const item of lineItems) {
    const totalCents = item.amount_cents + item.fee_cents;
    // Stripe requires amount >= 1 cent; skip zero-amount lines
    if (totalCents <= 0) continue;

    await stripe.invoiceItems.create({
      customer: customerId,
      invoice: invoice.id,
      amount: totalCents,
      currency: (item.currency ?? "USD").toLowerCase(),
      description: item.description,
      metadata,
    });
  }

  // 3. Finalize the invoice (locks the line items)
  const finalized = await stripe.invoices.finalizeInvoice(invoice.id);

  // Update SQLite settlement_batches
  execute(
    `UPDATE settlement_batches
     SET status = 'invoiced', stripe_invoice_id = ?
     WHERE batch_uuid = ?`,
    [finalized.id, batch.batch_uuid]
  );

  return {
    invoice_id: finalized.id,
    status: finalized.status ?? "draft",
    hosted_invoice_url: finalized.hosted_invoice_url ?? null,
  };
}
