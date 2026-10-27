import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { Pool } from 'pg';
import { HoldStrategy } from '../../src/api/holds/hold-strategy';
import { createHold, CreateHoldResult, releaseHold } from '../../src/api/holds/holds.service';
import { RedisGateStrategy } from '../../src/api/holds/redis-gate';
import { createHoldStrategy, HoldStrategyName } from '../../src/api/holds/strategies';
import { createTestEvent, createTestPool, createTestRedis, resetDatabase } from './helpers';

let pool: Pool;
let redis: Redis;

beforeAll(() => {
  pool = createTestPool();
  redis = createTestRedis();
});

afterAll(async () => {
  await pool.end();
  redis.disconnect();
});

beforeEach(async () => {
  await resetDatabase(pool);
  await redis.flushdb();
});

function hold(strategy: HoldStrategy, eventId: string, userId: string, seatIds: number[], key: string = randomUUID()) {
  return createHold(pool, strategy, { eventId, userId, idempotencyKey: key, seatIds });
}

async function seatRows(seatIds: number[]) {
  const { rows } = await pool.query<{ id: string; status: string; hold_id: string | null }>(
    'SELECT id, status, hold_id FROM seats WHERE id = ANY($1::bigint[]) ORDER BY id',
    [seatIds],
  );
  return rows;
}

const STRATEGIES: HoldStrategyName[] = ['pessimistic', 'optimistic', 'redis'];

describe.each(STRATEGIES)('%s strategy', (name) => {
  let strategy: HoldStrategy;

  beforeEach(() => {
    strategy = createHoldStrategy(name, pool, redis);
  });

  it('holds free seats and charges their total price', async () => {
    const { event, seatIds } = await createTestEvent(pool);

    const result = await hold(strategy, event.id, 'alice', [seatIds[0], seatIds[1]]);

    expect(result.kind).toBe('created');
    if (result.kind !== 'created') return;
    expect(result.hold.amount_paise).toBe(200_000);
    const seats = await seatRows([seatIds[0], seatIds[1]]);
    expect(seats.every((s) => s.status === 'held' && s.hold_id === result.hold.id)).toBe(true);
  });

  it('refuses a seat someone else holds, and says which one', async () => {
    const { event, seatIds } = await createTestEvent(pool);
    await hold(strategy, event.id, 'alice', [seatIds[0]]);

    const result = await hold(strategy, event.id, 'bob', [seatIds[0]]);

    expect(result).toEqual({ kind: 'unavailable', seatIds: [seatIds[0]] });
  });

  it('is all or nothing: if one seat is taken, none are held', async () => {
    const { event, seatIds } = await createTestEvent(pool);
    await hold(strategy, event.id, 'alice', [seatIds[1]]);

    const result = await hold(strategy, event.id, 'bob', [seatIds[0], seatIds[1]]);

    expect(result.kind).toBe('unavailable');
    const [first] = await seatRows([seatIds[0]]);
    expect(first.status).toBe('available');
    const holds = await pool.query(`SELECT 1 FROM holds WHERE user_id = 'bob'`);
    expect(holds.rows).toHaveLength(0);
  });

  it('gives a seat to exactly one of 50 users clicking it at the same moment', async () => {
    const { event, seatIds } = await createTestEvent(pool);

    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) => hold(strategy, event.id, `user-${i}`, [seatIds[0]])),
    );

    const created = results.filter((r) => r.kind === 'created');
    expect(created).toHaveLength(1);
    expect(results.every((r) => ['created', 'unavailable', 'contention'].includes(r.kind))).toBe(true);
    const [seat] = await seatRows([seatIds[0]]);
    expect(seat.hold_id).toBe((created[0] as Extract<CreateHoldResult, { kind: 'created' }>).hold.id);
  });

  it('never gives one seat to two holds when many overlapping multi-seat requests race', async () => {
    const { event, seatIds } = await createTestEvent(pool, 12);
    // 60 users each want 2-3 seats from the same 12, in different orders: lots of overlap,
    // which is exactly the situation that can deadlock if locks are taken carelessly.
    const requests = Array.from({ length: 60 }, (_, i) => {
      const start = (i * 5) % 11;
      const wanted = [seatIds[start], seatIds[start + 1], ...(i % 2 ? [seatIds[(start + 7) % 12]] : [])];
      return hold(strategy, event.id, `user-${i}`, [...new Set(wanted)].reverse());
    });
    const results = await Promise.all(requests);

    expect(results.every((r) => ['created', 'unavailable', 'contention'].includes(r.kind))).toBe(true);
    const won = results.filter((r) => r.kind === 'created') as Extract<CreateHoldResult, { kind: 'created' }>[];
    const heldSeats = won.flatMap((r) => r.hold.seat_ids.map(Number));
    expect(new Set(heldSeats).size).toBe(heldSeats.length); // no seat in two winning holds

    // And the seats table agrees with the winners, seat by seat.
    const seats = await seatRows(seatIds);
    for (const seat of seats) {
      const owner = won.find((r) => r.hold.seat_ids.map(Number).includes(Number(seat.id)));
      expect(seat.hold_id).toBe(owner ? owner.hold.id : null);
    }
  });

  it('lets someone else take a seat once its hold has expired', async () => {
    const { event, seatIds } = await createTestEvent(pool);
    const first = await hold(strategy, event.id, 'alice', [seatIds[0]]);
    if (first.kind !== 'created') throw new Error('setup failed');
    // Make Alice's hold expire, in Postgres and in the Redis gate.
    await pool.query(`UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = $1`, [first.hold.id]);
    await pool.query(`UPDATE seats SET held_until = now() - interval '1 second' WHERE hold_id = $1`, [first.hold.id]);
    await redis.flushdb();

    const second = await hold(strategy, event.id, 'bob', [seatIds[0]]);

    expect(second.kind).toBe('created');
  });

  it('returns the same hold when a request is retried with the same key', async () => {
    const { event, seatIds } = await createTestEvent(pool);
    const first = await hold(strategy, event.id, 'alice', [seatIds[0], seatIds[1]], 'key-1');
    // Same seats in a different order: still the same request.
    const retry = await hold(strategy, event.id, 'alice', [seatIds[1], seatIds[0]], 'key-1');

    expect(retry.kind).toBe('replayed');
    if (first.kind === 'created' && retry.kind === 'replayed') expect(retry.hold.id).toBe(first.hold.id);
  });

  it('creates one hold when the same request arrives 10 times at once', async () => {
    const { event, seatIds } = await createTestEvent(pool);

    const results = await Promise.all(
      Array.from({ length: 10 }, () => hold(strategy, event.id, 'alice', [seatIds[0]], 'same-key')),
    );

    const ids = new Set(results.map((r) => (r.kind === 'created' || r.kind === 'replayed' ? r.hold.id : r.kind)));
    expect(ids.size).toBe(1);
    expect(results.filter((r) => r.kind === 'created')).toHaveLength(1);
  });

  it('rejects a key reused for different seats', async () => {
    const { event, seatIds } = await createTestEvent(pool);
    await hold(strategy, event.id, 'alice', [seatIds[0]], 'key-1');

    expect((await hold(strategy, event.id, 'alice', [seatIds[1]], 'key-1')).kind).toBe('key_reused');
  });

  it('rejects seats that belong to another event', async () => {
    const { event } = await createTestEvent(pool);
    const other = await createTestEvent(pool);

    const result = await hold(strategy, event.id, 'alice', [other.seatIds[0]]);

    expect(result).toEqual({ kind: 'invalid_seats', seatIds: [other.seatIds[0]] });
  });

  it('puts released seats back on sale', async () => {
    const { event, seatIds } = await createTestEvent(pool);
    const first = await hold(strategy, event.id, 'alice', [seatIds[0]]);
    if (first.kind !== 'created') throw new Error('setup failed');

    expect((await releaseHold(pool, strategy, first.hold.id, 'alice')).kind).toBe('released');
    expect((await hold(strategy, event.id, 'bob', [seatIds[0]])).kind).toBe('created');
  });

  it("doesn't let another user release a hold", async () => {
    const { event, seatIds } = await createTestEvent(pool);
    const first = await hold(strategy, event.id, 'alice', [seatIds[0]]);
    if (first.kind !== 'created') throw new Error('setup failed');

    expect((await releaseHold(pool, strategy, first.hold.id, 'mallory')).kind).toBe('not_found');
  });
});

describe('redis strategy: Postgres has the final say', () => {
  it('still refuses a held seat when Redis has lost its keys', async () => {
    const strategy = createHoldStrategy('redis', pool, redis);
    const { event, seatIds } = await createTestEvent(pool);
    await hold(strategy, event.id, 'alice', [seatIds[0]]);
    await redis.flushdb(); // e.g. Redis restarted without persistence

    const result = await hold(strategy, event.id, 'bob', [seatIds[0]]);

    expect(result.kind).toBe('unavailable');
    // Bob's key was given back, so it doesn't block the seat until the TTL runs out.
    expect(await redis.exists(RedisGateStrategy.seatKey(event.id, seatIds[0]))).toBe(0);
  });

  it('keeps working on Postgres alone when Redis is down', async () => {
    // Every Redis command fails, as if the server were unreachable.
    const down = () => Promise.reject(new Error('connect ECONNREFUSED'));
    const deadRedis = { defineCommand() {}, seatTryHold: down, seatRelease: down } as unknown as Redis;
    const strategy = createHoldStrategy('redis', pool, deadRedis);
    const { event, seatIds } = await createTestEvent(pool);

    expect((await hold(strategy, event.id, 'alice', [seatIds[0]])).kind).toBe('created');
    expect((await hold(strategy, event.id, 'bob', [seatIds[0]])).kind).toBe('unavailable');
  });
});
