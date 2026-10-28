import type { Express } from 'express';
import type { Server } from 'node:http';
import { createServer, type AddressInfo } from 'node:net';
import Redis from 'ioredis';
import { Pool } from 'pg';
import type { AppDeps } from '../../src/api/app';
import { createEvent } from '../../src/api/events/events.service';
import { PostgresFlashStrategy } from '../../src/api/flash/flash-strategies';
import { SeatMapCache } from '../../src/api/events/seat-map';
import { createHoldStrategy } from '../../src/api/holds/strategies';
import { createPool } from '../../src/shared/db';
import { GatewayClient } from '../../src/shared/gateway-client';
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

export const TEST_WEBHOOK_SECRET = 'whsec_test';
export const TEST_GATEWAY_KEY = 'sk_test';

// App dependencies for tests. The seat map cache is off (0ms) so tests see changes at once.
export function testAppDeps(pool: Pool, redis: Redis, overrides: Partial<AppDeps> = {}): AppDeps {
  return {
    pool,
    redis,
    holdStrategy: createHoldStrategy('pessimistic', pool, redis),
    flashStrategy: new PostgresFlashStrategy(pool),
    seatMap: new SeatMapCache(pool, 0),
    gateway: new GatewayClient('http://localhost:1', TEST_GATEWAY_KEY),
    webhookSecret: TEST_WEBHOOK_SECRET,
    webhookToleranceSeconds: 300,
    ...overrides,
  };
}

// Starts an HTTP server on a free port.
export async function listen(app: Express): Promise<{ url: string; close: () => Promise<void> }> {
  const server: Server = await new Promise((resolve) => {
    const s = app.listen(0, () => resolve(s));
  });
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://localhost:${port}`,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}

// Polls until check() returns something truthy (webhooks arrive asynchronously).
export async function waitFor<T>(check: () => Promise<T>, timeoutMs = 5000): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value as NonNullable<T>;
    if (Date.now() > deadline) throw new Error('timed out waiting');
    await new Promise((r) => setTimeout(r, 25));
  }
}

// A free TCP port, for servers that must know each other's URL before they start.
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, () => {
      const { port } = server.address() as AddressInfo;
      server.close(() => resolve(port));
    });
    server.on('error', reject);
  });
}
