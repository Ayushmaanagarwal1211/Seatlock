import { Pool } from 'pg';
import { withTransaction } from '../../shared/db';
import { GatewayClient } from '../../shared/gateway-client';
import { GatewayEvent, GatewayPayment } from '../../shared/gateway-types';
import { log } from '../../shared/log';
import { HoldStrategy } from '../holds/hold-strategy';
import { holdStatus } from '../holds/holds.service';
import type { HoldRow } from '../holds/hold-strategy';
import {
  applyPaymentFailed,
  applyPaymentSucceeded,
  applyRefundSucceeded,
  PaymentRow,
  SucceededOutcome,
} from './payment-outcomes';

export type CheckoutResult =
  | { kind: 'ok'; payment: PaymentRow; payUrl: string }
  | { kind: 'not_found' }
  | { kind: 'not_active'; status: string }
  | { kind: 'already_paid' }
  | { kind: 'gateway_error'; message: string };

/**
 * Starts (or resumes) paying for a hold. Safe to call again: it reuses the hold's live
 * payment, and the gateway call is idempotent on Seatlock's payment id.
 */
export async function startCheckout(
  pool: Pool,
  gateway: GatewayClient,
  strategy: HoldStrategy,
  holdId: string,
  userId: string,
  email: string,
): Promise<CheckoutResult> {
  const { rows } = await pool.query<HoldRow>('SELECT * FROM holds WHERE id = $1 AND user_id = $2', [holdId, userId]);
  const hold = rows[0];
  if (!hold) return { kind: 'not_found' };
  const status = holdStatus(hold);
  if (status === 'booked') return { kind: 'already_paid' };
  if (status !== 'active') return { kind: 'not_active', status };

  let payment = await findLivePayment(pool, holdId);
  if (payment && payment.status !== 'pending') return { kind: 'already_paid' };

  // A pending payment whose webhook hasn't arrived may already have failed at the gateway
  // (the user saw "card declined" and came back to try again). Ask the gateway.
  if (payment?.gateway_payment_id) {
    const outcome = await refreshFromGateway(pool, gateway, strategy, payment);
    if (outcome === 'succeeded') return { kind: 'already_paid' };
    if (outcome === 'failed') payment = null;
  }

  if (!payment) {
    // The partial unique index allows one live payment per hold; if a concurrent checkout
    // created it first, ON CONFLICT finds it and we use that one.
    const inserted = await pool.query<PaymentRow>(
      `INSERT INTO payments (hold_id, amount_paise, contact_email) VALUES ($1, $2, $3)
       ON CONFLICT (hold_id) WHERE status <> 'failed' DO NOTHING
       RETURNING *`,
      [holdId, hold.amount_paise, email],
    );
    payment = inserted.rows[0] ?? (await findLivePayment(pool, holdId));
    if (!payment || payment.status !== 'pending') return { kind: 'already_paid' };
  }

  if (!payment.gateway_payment_id) {
    let created: GatewayPayment;
    try {
      created = await gateway.createPayment(payment.id, payment.amount_paise);
    } catch (err) {
      // The payment row stays pending without a gateway id; the next checkout call retries
      // with the same idempotency key, so the gateway can't end up with two payments.
      return { kind: 'gateway_error', message: (err as Error).message };
    }
    const updated = await pool.query<PaymentRow>(
      `UPDATE payments SET gateway_payment_id = $2, contact_email = $3, updated_at = now()
        WHERE id = $1 RETURNING *`,
      [payment.id, created.id, email],
    );
    payment = updated.rows[0];
  }

  return { kind: 'ok', payment, payUrl: gateway.payUrl(payment.gateway_payment_id!) };
}

async function findLivePayment(pool: Pool, holdId: string): Promise<PaymentRow | null> {
  const { rows } = await pool.query<PaymentRow>(`SELECT * FROM payments WHERE hold_id = $1 AND status <> 'failed'`, [
    holdId,
  ]);
  return rows[0] ?? null;
}

/**
 * Asks the gateway for a pending payment's real status and applies it, through the same
 * code as a webhook. Used by checkout and by the reconciler (for webhooks that never came).
 */
export async function refreshFromGateway(
  pool: Pool,
  gateway: GatewayClient,
  strategy: HoldStrategy,
  payment: PaymentRow,
): Promise<'succeeded' | 'failed' | 'pending'> {
  const remote = await gateway.getPayment(payment.gateway_payment_id!);
  if (remote.status === 'succeeded' || remote.status === 'refunded') {
    const outcome = await withTransaction(pool, (client) => applyPaymentSucceeded(client, payment.id, remote.id));
    await afterSucceeded(strategy, outcome);
    return 'succeeded';
  }
  if (remote.status === 'failed') {
    await withTransaction(pool, (client) => applyPaymentFailed(client, payment.id));
    return 'failed';
  }
  return 'pending';
}

// Keeps the Redis gate (if any) in step with what just happened in Postgres.
async function afterSucceeded(strategy: HoldStrategy, outcome: SucceededOutcome): Promise<void> {
  if (outcome.kind === 'booked') await strategy.afterBooking(outcome.eventId, outcome.seatIds);
  if (outcome.kind === 'refunding') await strategy.afterRelease(outcome.eventId, outcome.releasedSeatIds, outcome.holdId);
}

export type WebhookResult = { duplicate: true } | { duplicate: false; outcome: string };

/**
 * Handles one webhook event from the gateway, exactly once in effect:
 *
 *   1. Record the event id in webhook_events. A duplicate delivery finds it already there
 *      (primary key) and stops here. Two copies arriving at the same moment: the second
 *      INSERT waits for the first transaction, then sees the conflict.
 *   2. Apply it. Each apply function also checks the payment's current status, so even an
 *      event we never saw (e.g. the reconciler got there first) can't be applied twice.
 *
 * Both happen in one transaction: if applying fails, the event isn't recorded either, we
 * answer 500, and the gateway delivers it again later.
 */
export async function handleGatewayEvent(pool: Pool, strategy: HoldStrategy, event: GatewayEvent): Promise<WebhookResult> {
  const result = await withTransaction(pool, async (client) => {
    const recorded = await client.query(
      'INSERT INTO webhook_events (id, type, payload) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING',
      [event.id, event.type, event],
    );
    if (recorded.rowCount === 0) return { duplicate: true } as const;

    const paymentId = event.data.reference;
    switch (event.type) {
      case 'payment.succeeded': {
        const outcome = await applyPaymentSucceeded(client, paymentId, event.data.paymentId);
        return { duplicate: false, outcome } as const;
      }
      case 'payment.failed': {
        const applied = await applyPaymentFailed(client, paymentId);
        return { duplicate: false, outcome: { kind: applied ? 'failed' : 'ignored' } } as const;
      }
      case 'refund.succeeded': {
        const applied = await applyRefundSucceeded(client, paymentId);
        return { duplicate: false, outcome: { kind: applied ? 'refunded' : 'ignored' } } as const;
      }
      default:
        return { duplicate: false, outcome: { kind: 'ignored' } } as const;
    }
  });

  if (result.duplicate) return { duplicate: true };
  if (event.type === 'payment.succeeded') await afterSucceeded(strategy, result.outcome as SucceededOutcome);
  if (result.outcome.kind === 'ignored') log.info(`[webhook] ${event.type} ${event.id} ignored`);
  return { duplicate: false, outcome: result.outcome.kind };
}
