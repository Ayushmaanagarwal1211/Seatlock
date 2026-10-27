import { Pool } from 'pg';
import { withTransaction } from '../../shared/db';
import { DEADLOCK_DETECTED, pgErrorCode, SERIALIZATION_FAILURE } from '../../shared/pg-errors';
import { uuidFromName } from '../../shared/uuid';
import { getEvent } from '../events/events.service';
import { hashRequest } from '../request-hash';
import { HoldRow, HoldStrategy } from './hold-strategy';

export type CreateHoldResult =
  | { kind: 'created'; hold: HoldRow }
  | { kind: 'replayed'; hold: HoldRow } // same key, same request: a client retry
  | { kind: 'key_reused' } // same key, different request: a client bug
  | { kind: 'event_not_found' }
  | { kind: 'too_many_seats'; max: number }
  | { kind: 'invalid_seats'; seatIds: number[] }
  | { kind: 'unavailable'; seatIds: number[] }
  | { kind: 'contention' }; // lost a lock race (deadlock victim); trying again may work

export interface CreateHoldInput {
  eventId: string;
  userId: string;
  idempotencyKey: string;
  seatIds: number[];
}

/**
 * Holds seats for a user, all or nothing. Idempotent: a retried request with the same
 * Idempotency-Key gets the same hold back instead of a 409 for its own seats.
 */
export async function createHold(pool: Pool, strategy: HoldStrategy, input: CreateHoldInput): Promise<CreateHoldResult> {
  const seatIds = [...input.seatIds].sort((a, b) => a - b);
  const requestHash = hashRequest([input.eventId, ...seatIds]);

  // Fast path for retries: this key was already used.
  const existing = await findByKey(pool, input.userId, input.idempotencyKey);
  if (existing) return replay(existing, requestHash);

  const event = await getEvent(pool, input.eventId);
  if (!event) return { kind: 'event_not_found' };
  if (seatIds.length > event.max_seats_per_hold) return { kind: 'too_many_seats', max: event.max_seats_per_hold };

  let result;
  try {
    result = await strategy.hold({
      // Derived from (user, key): a retry of the same request gets the same hold id, which
      // lets the Redis gate recognise seats it reserved for this very request.
      holdId: uuidFromName(`hold:${input.userId}:${input.idempotencyKey}`),
      eventId: input.eventId,
      userId: input.userId,
      idempotencyKey: input.idempotencyKey,
      requestHash,
      seatIds,
      holdSeconds: event.hold_seconds,
    });
  } catch (err) {
    // Postgres broke a deadlock by aborting this transaction. The seats were contested;
    // the client can retry (with the same key) or pick other seats.
    const code = pgErrorCode(err);
    if (code === DEADLOCK_DETECTED || code === SERIALIZATION_FAILURE) return { kind: 'contention' };
    throw err;
  }

  if (result.kind === 'held') return { kind: 'created', hold: result.hold };

  // Two requests with the SAME key raced, and the other one won: its seats now look taken
  // to us. That's not a conflict, it's our own hold: return it.
  if (result.kind === 'key_taken' || result.kind === 'unavailable') {
    const winner = await findByKey(pool, input.userId, input.idempotencyKey);
    if (winner) return replay(winner, requestHash);
  }
  if (result.kind === 'key_taken') throw new Error('idempotency key conflict without a stored hold');
  return result;
}

function replay(hold: HoldRow, requestHash: string): CreateHoldResult {
  return hold.request_hash === requestHash ? { kind: 'replayed', hold } : { kind: 'key_reused' };
}

async function findByKey(pool: Pool, userId: string, key: string): Promise<HoldRow | null> {
  const { rows } = await pool.query<HoldRow>('SELECT * FROM holds WHERE user_id = $1 AND idempotency_key = $2', [
    userId,
    key,
  ]);
  return rows[0] ?? null;
}

// A hold is only visible to the user who owns it.
export async function getHold(pool: Pool, holdId: string, userId: string): Promise<HoldRow | null> {
  const { rows } = await pool.query<HoldRow>('SELECT * FROM holds WHERE id = $1 AND user_id = $2', [holdId, userId]);
  return rows[0] ?? null;
}

export type ReleaseHoldResult =
  | { kind: 'released'; hold: HoldRow }
  | { kind: 'not_found' }
  | { kind: 'not_active'; status: string }
  | { kind: 'payment_in_progress' };

// The user gives the seats back before paying.
export async function releaseHold(
  pool: Pool,
  strategy: HoldStrategy,
  holdId: string,
  userId: string,
): Promise<ReleaseHoldResult> {
  const result = await withTransaction(pool, async (client): Promise<ReleaseHoldResult> => {
    const { rows } = await client.query<HoldRow>('SELECT * FROM holds WHERE id = $1 AND user_id = $2 FOR UPDATE', [
      holdId,
      userId,
    ]);
    const hold = rows[0];
    if (!hold) return { kind: 'not_found' };
    if (hold.status !== 'active' || hold.expires_at <= new Date()) {
      return { kind: 'not_active', status: holdStatus(hold) };
    }
    // Once a payment has started, the payment's outcome decides what happens to the seats.
    const payment = await client.query(`SELECT 1 FROM payments WHERE hold_id = $1 AND status <> 'failed'`, [holdId]);
    if (payment.rows.length > 0) return { kind: 'payment_in_progress' };

    await client.query(
      `UPDATE seats SET status = 'available', hold_id = NULL, held_until = NULL, version = version + 1
        WHERE hold_id = $1 AND status = 'held'`,
      [holdId],
    );
    const updated = await client.query<HoldRow>(
      `UPDATE holds SET status = 'released', updated_at = now() WHERE id = $1 RETURNING *`,
      [holdId],
    );
    return { kind: 'released', hold: updated.rows[0] };
  });

  if (result.kind === 'released') {
    await strategy.afterRelease(result.hold.event_id, result.hold.seat_ids.map(Number), result.hold.id);
  }
  return result;
}

// 'active' past its expiry time is reported as 'expired' (holds expire lazily).
export function holdStatus(hold: HoldRow): string {
  return hold.status === 'active' && hold.expires_at <= new Date() ? 'expired' : hold.status;
}

export function toHoldResponse(hold: HoldRow) {
  return {
    id: hold.id,
    eventId: hold.event_id,
    seatIds: hold.seat_ids.map(Number),
    amountPaise: hold.amount_paise,
    status: holdStatus(hold),
    expiresAt: hold.expires_at,
    createdAt: hold.created_at,
  };
}
