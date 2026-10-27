import { PoolClient } from 'pg';
import type { HoldRow } from '../holds/hold-strategy';

export interface PaymentRow {
  id: string;
  hold_id: string;
  amount_paise: number;
  status: 'pending' | 'succeeded' | 'failed' | 'refund_pending' | 'refunded';
  gateway_payment_id: string | null;
  contact_email: string | null;
  created_at: Date;
  updated_at: Date;
}

export type SucceededOutcome =
  | { kind: 'booked'; bookingId: string; eventId: string; seatIds: number[] }
  | { kind: 'refunding'; eventId: string; releasedSeatIds: number[]; holdId: string } // paid too late, seats gone
  | { kind: 'ignored'; reason: string };

/**
 * A payment succeeded. Turn the hold into a booking, or refund if the seats are gone.
 * Runs inside the caller's transaction (the webhook handler or the reconciler), so the
 * booking, the seat changes and the outbox rows are saved together or not at all.
 *
 * Idempotent by state: payment status only moves forward (pending → succeeded), so a
 * second "succeeded" for the same payment, from a duplicate webhook or the reconciler,
 * finds it already succeeded and does nothing.
 */
export async function applyPaymentSucceeded(
  client: PoolClient,
  paymentId: string,
  gatewayPaymentId: string,
): Promise<SucceededOutcome> {
  const { rows } = await client.query<PaymentRow>('SELECT * FROM payments WHERE id = $1 FOR UPDATE', [paymentId]);
  const payment = rows[0];
  if (!payment) return { kind: 'ignored', reason: 'unknown payment' };
  if (payment.status !== 'pending') return { kind: 'ignored', reason: `payment already ${payment.status}` };

  await client.query(
    `UPDATE payments SET status = 'succeeded', gateway_payment_id = COALESCE(gateway_payment_id, $2), updated_at = now()
      WHERE id = $1`,
    [paymentId, gatewayPaymentId],
  );

  const hold = (await client.query<HoldRow>('SELECT * FROM holds WHERE id = $1 FOR UPDATE', [payment.hold_id])).rows[0];
  // Lock the seats in id order, like the pessimistic hold strategy, so this can't deadlock
  // with a hold being placed on the same seats right now.
  const { rows: seats } = await client.query<{ id: string; label: string; status: string; hold_id: string | null }>(
    'SELECT id, label, status, hold_id FROM seats WHERE id = ANY($1::bigint[]) ORDER BY id FOR UPDATE',
    [hold.seat_ids],
  );

  // Are the seats still ours? If the hold expired but nobody took the seats, they still
  // point at this hold: book them anyway, the user paid.
  const stillOurs =
    hold.status === 'active' &&
    seats.length === hold.seat_ids.length &&
    seats.every((s) => s.status === 'held' && s.hold_id === hold.id);

  const event = (await client.query<{ name: string }>('SELECT name FROM events WHERE id = $1', [hold.event_id])).rows[0];
  const seatIds = seats.map((s) => Number(s.id));

  if (stillOurs) {
    await client.query(
      `UPDATE seats SET status = 'booked', held_until = NULL, version = version + 1 WHERE id = ANY($1::bigint[])`,
      [seatIds],
    );
    const booking = await client.query<{ id: string }>(
      `INSERT INTO bookings (event_id, user_id, hold_id, payment_id, amount_paise)
       VALUES ($1, $2, $3, $4, $5) RETURNING id`,
      [hold.event_id, hold.user_id, hold.id, payment.id, payment.amount_paise],
    );
    const bookingId = booking.rows[0].id;
    // UNIQUE (seat_id) here is the last line of defence: a double booking would fail
    // this insert and roll back the whole transaction.
    await client.query(
      'INSERT INTO booking_seats (booking_id, seat_id) SELECT $1, unnest($2::bigint[])',
      [bookingId, seatIds],
    );
    await client.query(`UPDATE holds SET status = 'booked', updated_at = now() WHERE id = $1`, [hold.id]);
    await addToOutbox(client, 'booking.confirmed', {
      bookingId,
      email: payment.contact_email,
      eventName: event.name,
      seats: seats.map((s) => s.label),
      amountPaise: payment.amount_paise,
    });
    return { kind: 'booked', bookingId, eventId: hold.event_id, seatIds };
  }

  // Paid too late: at least one seat now belongs to someone else. The user asked for these
  // seats together, so book none of them: give back the ones still ours, and refund.
  const { rows: released } = await client.query<{ id: string }>(
    `UPDATE seats SET status = 'available', hold_id = NULL, held_until = NULL, version = version + 1
      WHERE hold_id = $1 AND status = 'held'
      RETURNING id`,
    [hold.id],
  );
  await client.query(
    `UPDATE holds SET status = CASE WHEN status = 'active' THEN 'expired' ELSE status END, updated_at = now() WHERE id = $1`,
    [hold.id],
  );
  await client.query(`UPDATE payments SET status = 'refund_pending', updated_at = now() WHERE id = $1`, [payment.id]);
  await addToOutbox(client, 'payment.refund', { paymentId: payment.id, gatewayPaymentId });
  await addToOutbox(client, 'booking.failed', {
    paymentId: payment.id,
    email: payment.contact_email,
    eventName: event.name,
    amountPaise: payment.amount_paise,
  });
  return { kind: 'refunding', eventId: hold.event_id, releasedSeatIds: released.map((r) => Number(r.id)), holdId: hold.id };
}

// The card was declined, or the user gave up on the payment page. The hold stays active
// until it expires, so the user can try again with another card.
export async function applyPaymentFailed(client: PoolClient, paymentId: string): Promise<boolean> {
  const { rowCount } = await client.query(
    `UPDATE payments SET status = 'failed', updated_at = now() WHERE id = $1 AND status = 'pending'`,
    [paymentId],
  );
  return rowCount === 1;
}

export async function applyRefundSucceeded(client: PoolClient, paymentId: string): Promise<boolean> {
  const { rowCount } = await client.query(
    `UPDATE payments SET status = 'refunded', updated_at = now() WHERE id = $1 AND status = 'refund_pending'`,
    [paymentId],
  );
  return rowCount === 1;
}

export async function addToOutbox(client: PoolClient, topic: string, payload: object): Promise<void> {
  await client.query('INSERT INTO outbox (topic, payload) VALUES ($1, $2)', [topic, payload]);
}
