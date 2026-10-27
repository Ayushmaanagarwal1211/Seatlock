import { Pool } from 'pg';
import { createPool } from '../../src/shared/db';
import { testDatabaseUrl } from './test-db';

// The schema's constraints are the last line of defence: whatever the application code
// does, Postgres must refuse a double booking or a second live payment. These tests
// write rows directly, bypassing the application, to prove the database itself says no.

const UNIQUE_VIOLATION = '23505';
const CHECK_VIOLATION = '23514';

let pool: Pool;

beforeAll(() => {
  pool = createPool(testDatabaseUrl());
});

afterAll(async () => {
  await pool.end();
});

beforeEach(async () => {
  await pool.query(
    'TRUNCATE events, holds, seats, payments, webhook_events, bookings, booking_seats, outbox CASCADE',
  );
});

async function createEvent(): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO events (name, venue, starts_at) VALUES ('Concert', 'Stadium', now() + interval '7 days')
     RETURNING id`,
  );
  return rows[0].id;
}

async function createSeat(eventId: string, label: string): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO seats (event_id, label, section, price_paise) VALUES ($1, $2, 'Gold', 150000) RETURNING id`,
    [eventId, label],
  );
  return rows[0].id;
}

async function createHold(eventId: string, userId: string, seatIds: string[], key = `key-${userId}`) {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO holds (event_id, user_id, idempotency_key, request_hash, seat_ids, amount_paise, expires_at)
     VALUES ($1, $2, $3, 'hash', $4, 150000, now() + interval '10 minutes')
     RETURNING id`,
    [eventId, userId, key, seatIds],
  );
  return rows[0].id;
}

async function createPayment(holdId: string, status = 'succeeded'): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO payments (hold_id, amount_paise, status) VALUES ($1, 150000, $2) RETURNING id`,
    [holdId, status],
  );
  return rows[0].id;
}

async function book(eventId: string, userId: string, seatId: string) {
  const holdId = await createHold(eventId, userId, [seatId]);
  const paymentId = await createPayment(holdId);
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO bookings (event_id, user_id, hold_id, payment_id, amount_paise)
     VALUES ($1, $2, $3, $4, 150000) RETURNING id`,
    [eventId, userId, holdId, paymentId],
  );
  await pool.query('INSERT INTO booking_seats (booking_id, seat_id) VALUES ($1, $2)', [rows[0].id, seatId]);
}

describe('schema safety nets', () => {
  it('refuses to put the same seat in two bookings', async () => {
    const eventId = await createEvent();
    const seatId = await createSeat(eventId, 'A-1');

    await book(eventId, 'alice', seatId);
    await expect(book(eventId, 'bob', seatId)).rejects.toMatchObject({ code: UNIQUE_VIOLATION });

    const { rows } = await pool.query('SELECT count(*)::int AS n FROM booking_seats WHERE seat_id = $1', [seatId]);
    expect(rows[0].n).toBe(1);
  });

  it('refuses a seat state that mixes up available, held and booked', async () => {
    const eventId = await createEvent();
    const seatId = await createSeat(eventId, 'A-1');
    const holdId = await createHold(eventId, 'alice', [seatId]);

    // held, but with no expiry time
    await expect(
      pool.query(`UPDATE seats SET status = 'held', hold_id = $2 WHERE id = $1`, [seatId, holdId]),
    ).rejects.toMatchObject({ code: CHECK_VIOLATION });

    // available, but still pointing at a hold
    await expect(pool.query(`UPDATE seats SET hold_id = $2 WHERE id = $1`, [seatId, holdId])).rejects.toMatchObject({
      code: CHECK_VIOLATION,
    });

    // a correct hold is accepted
    await pool.query(
      `UPDATE seats SET status = 'held', hold_id = $2, held_until = now() + interval '10 minutes' WHERE id = $1`,
      [seatId, holdId],
    );
  });

  it('allows only one live payment per hold, but a new one after a failure', async () => {
    const eventId = await createEvent();
    const seatId = await createSeat(eventId, 'A-1');
    const holdId = await createHold(eventId, 'alice', [seatId]);

    await createPayment(holdId, 'failed');
    await createPayment(holdId, 'pending'); // retry with another card: allowed
    await expect(createPayment(holdId, 'pending')).rejects.toMatchObject({ code: UNIQUE_VIOLATION });
  });

  it('scopes hold idempotency keys to the user', async () => {
    const eventId = await createEvent();
    const seatId = await createSeat(eventId, 'A-1');

    await createHold(eventId, 'alice', [seatId], 'same-key');
    await createHold(eventId, 'bob', [seatId], 'same-key'); // another user may pick the same key
    await expect(createHold(eventId, 'alice', [seatId], 'same-key')).rejects.toMatchObject({
      code: UNIQUE_VIOLATION,
    });
  });

  it('records each webhook event only once', async () => {
    const insert = () =>
      pool.query(
        `INSERT INTO webhook_events (id, type, payload) VALUES ('evt_1', 'payment.succeeded', '{}')
         ON CONFLICT (id) DO NOTHING`,
      );

    expect((await insert()).rowCount).toBe(1);
    expect((await insert()).rowCount).toBe(0); // the gateway delivered it again: ignored
  });
});
