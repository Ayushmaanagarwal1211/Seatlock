import { Pool } from 'pg';
import { withTransaction } from '../shared/db';
import { GatewayClient, GatewayError } from '../shared/gateway-client';
import { log } from '../shared/log';
import { EmailSender, PermanentError } from './email-sender';

interface OutboxRow {
  id: string;
  topic: string;
  payload: Record<string, unknown>;
  attempts: number;
}

export interface DispatchResult {
  picked: number;
  sent: number;
  retrying: number;
  parked: number;
}

export interface DispatcherDeps {
  pool: Pool;
  email: EmailSender;
  gateway: GatewayClient;
  batchSize: number;
  concurrency: number;
}

const rupees = (paise: unknown) => `₹${(Number(paise) / 100).toLocaleString('en-IN')}`;

/**
 * What to do for each outbox topic. Every call carries an idempotency key built from ids
 * that never change, so delivering the same row twice has the effect of delivering it once.
 */
async function deliver(deps: DispatcherDeps, row: OutboxRow): Promise<void> {
  const p = row.payload;
  switch (row.topic) {
    case 'booking.confirmed':
      if (!p.email) return;
      await deps.email.send({
        idempotencyKey: `seatlock:booking-confirmed:${p.bookingId}`,
        to: String(p.email),
        subject: `Your tickets for ${p.eventName}`,
        body:
          `Your booking is confirmed.\n\nEvent: ${p.eventName}\nSeats: ${(p.seats as string[]).join(', ')}\n` +
          `Paid: ${rupees(p.amountPaise)}\nBooking id: ${p.bookingId}\n`,
      });
      return;
    case 'booking.failed':
      if (!p.email) return;
      await deps.email.send({
        idempotencyKey: `seatlock:booking-failed:${p.paymentId}`,
        to: String(p.email),
        subject: `We couldn't book your seats for ${p.eventName}`,
        body:
          `Your payment arrived after your seats were released, and some of them were booked by someone else.\n\n` +
          `We've refunded ${rupees(p.amountPaise)}. It usually reaches your account in 5-7 working days.\n`,
      });
      return;
    case 'payment.refund':
      try {
        await deps.gateway.refund(String(p.paymentId), String(p.gatewayPaymentId));
      } catch (err) {
        // 404/409 from the gateway: this payment can't be refunded (a bug or manual action).
        // A human needs to look; retrying won't change the answer.
        if (err instanceof GatewayError && (err.status === 404 || err.status === 409)) {
          throw new PermanentError(err.message);
        }
        throw err;
      }
      return;
    default:
      throw new PermanentError(`unknown outbox topic ${row.topic}`);
  }
}

// 5s, 10s, 20s, ... up to 10 minutes, with ±20% jitter so failed rows don't all retry together.
export function retryDelayMs(attempts: number): number {
  const base = Math.min(5000 * 2 ** (attempts - 1), 600_000);
  return Math.round(base * (0.8 + Math.random() * 0.4));
}

/**
 * Delivers one batch of due outbox rows.
 *
 * FOR UPDATE SKIP LOCKED: several dispatchers can run at once; each takes rows the others
 * haven't locked. A row is marked done only after its call succeeded, in the same
 * transaction that locked it. If the dispatcher crashes mid-batch, the transaction rolls
 * back and the rows are delivered again later: at least once, made safe by idempotency keys.
 *
 * A failed row is retried later with a growing delay; it doesn't block the rows behind it.
 * A row that can never succeed (PermanentError) is parked: next_attempt_at = infinity,
 * with the error kept in last_error for a human to look at.
 */
export async function dispatchOnce(deps: DispatcherDeps): Promise<DispatchResult> {
  return withTransaction(deps.pool, async (client) => {
    const { rows } = await client.query<OutboxRow>(
      `SELECT id, topic, payload, attempts FROM outbox
        WHERE published_at IS NULL AND next_attempt_at <= now()
        ORDER BY next_attempt_at, id
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [deps.batchSize],
    );
    const result: DispatchResult = { picked: rows.length, sent: 0, retrying: 0, parked: 0 };
    if (rows.length === 0) return result;

    const outcomes = await mapWithConcurrency(rows, deps.concurrency, async (row) => {
      try {
        await deliver(deps, row);
        return { row, ok: true as const };
      } catch (err) {
        return { row, ok: false as const, permanent: err instanceof PermanentError, error: (err as Error).message };
      }
    });

    for (const outcome of outcomes) {
      if (outcome.ok) {
        result.sent++;
        await client.query('UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = $1', [
          outcome.row.id,
        ]);
      } else if (outcome.permanent) {
        result.parked++;
        log.error(`[dispatcher] outbox ${outcome.row.id} (${outcome.row.topic}) parked: ${outcome.error}`);
        await client.query(
          `UPDATE outbox SET attempts = attempts + 1, next_attempt_at = 'infinity', last_error = $2 WHERE id = $1`,
          [outcome.row.id, outcome.error],
        );
      } else {
        result.retrying++;
        await client.query(
          `UPDATE outbox SET attempts = attempts + 1, last_error = $2,
                  next_attempt_at = now() + make_interval(secs => $3)
            WHERE id = $1`,
          [outcome.row.id, outcome.error, retryDelayMs(outcome.row.attempts + 1) / 1000],
        );
      }
    }
    return result;
  });
}

// Runs fn over items with at most `limit` running at the same time.
async function mapWithConcurrency<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i]);
    }
  });
  await Promise.all(workers);
  return results;
}
