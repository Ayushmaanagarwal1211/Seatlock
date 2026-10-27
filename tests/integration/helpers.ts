import Redis from 'ioredis';
import { Pool } from 'pg';
import { createEvent } from '../../src/api/events/events.service';
import { createPool } from '../../src/shared/db';
import { testDatabaseUrl, testRedisUrl } from './test-db';

export function createTestPool(): Pool {
  return createPool(testDatabaseUrl(), 20);
}

export function createTestRedis(): Redis {
  return new Redis(testRedisUrl(), { maxRetriesPerRequest: 1 });
}

export async function resetDatabase(pool: Pool): Promise<void> {
  const { rows } = await pool.query<{ tablename: string }>(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`,
  );
  await pool.query(`TRUNCATE ${rows.map((r) => r.tablename).join(', ')} RESTART IDENTITY CASCADE`);
}

// An event with `seats` seats in one section, priced 1,000 rupees each.
export async function createTestEvent(pool: Pool, seats = 10, options: { holdSeconds?: number } = {}) {
  const { event } = await createEvent(pool, {
    name: 'Test concert',
    venue: 'Test stadium',
    startsAt: new Date(Date.now() + 7 * 24 * 3600_000),
    holdSeconds: options.holdSeconds ?? 600,
    maxSeatsPerHold: 6,
    sections: [{ name: 'Gold', rows: 1, seatsPerRow: seats, pricePaise: 100_000 }],
  });
  const { rows } = await pool.query<{ id: string }>('SELECT id FROM seats WHERE event_id = $1 ORDER BY id', [event.id]);
  return { event, seatIds: rows.map((r) => Number(r.id)) };
}
