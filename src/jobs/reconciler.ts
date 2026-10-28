import { Pool } from 'pg';
import { refreshFromGateway } from '../api/payments/payments.service';
import { applyRefundSucceeded, type PaymentRow } from '../api/payments/payment-outcomes';
import { withTransaction } from '../shared/db';
import { HoldStrategy } from '../api/holds/hold-strategy';
import { GatewayClient } from '../shared/gateway-client';

export interface ReconcileResult {
  checked: number;
  succeeded: number;
  failed: number;
  abandoned: number;
  refunded: number; // refunds the gateway made but whose webhook never came
}

/**
 * Webhooks can be lost: the gateway gave up retrying, our API was down for an hour, a bug
 * returned 200 without doing anything. So we don't rely on them alone. Every few seconds
 * this asks the gateway about payments that have been pending (or waiting for a refund)
 * for a while, and applies the real outcome through the same code path as a webhook.
 */
export async function reconcileOnce(
  pool: Pool,
  gateway: GatewayClient,
  strategy: HoldStrategy,
  olderThanMs: number,
  limit = 100,
): Promise<ReconcileResult> {
  const result: ReconcileResult = { checked: 0, succeeded: 0, failed: 0, abandoned: 0, refunded: 0 };

  // A payment the gateway never heard of (checkout crashed before or during the gateway
  // call, and the user never came back): nobody can pay it, so it has failed.
  const abandoned = await pool.query(
    `UPDATE payments SET status = 'failed', updated_at = now()
      WHERE status = 'pending' AND gateway_payment_id IS NULL
        AND created_at < now() - make_interval(secs => $1)`,
    [olderThanMs / 1000],
  );
  result.abandoned = abandoned.rowCount ?? 0;

  const { rows } = await pool.query<PaymentRow>(
    `SELECT * FROM payments
      WHERE status = 'pending' AND gateway_payment_id IS NOT NULL
        AND created_at < now() - make_interval(secs => $1)
      ORDER BY created_at
      LIMIT $2`,
    [olderThanMs / 1000, limit],
  );
  for (const payment of rows) {
    result.checked++;
    const outcome = await refreshFromGateway(pool, gateway, strategy, payment);
    if (outcome === 'succeeded') result.succeeded++;
    if (outcome === 'failed') result.failed++;
  }

  // Refunds are confirmed by webhook too, and those can be lost just the same.
  const { rows: refunding } = await pool.query<PaymentRow>(
    `SELECT * FROM payments
      WHERE status = 'refund_pending' AND updated_at < now() - make_interval(secs => $1)
      ORDER BY updated_at
      LIMIT $2`,
    [olderThanMs / 1000, limit],
  );
  for (const payment of refunding) {
    result.checked++;
    const remote = await gateway.getPayment(payment.gateway_payment_id!);
    if (remote.status === 'refunded') {
      await withTransaction(pool, (client) => applyRefundSucceeded(client, payment.id));
      result.refunded++;
    }
  }
  return result;
}
