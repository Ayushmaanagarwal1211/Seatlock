import Redis from 'ioredis';
import { Pool } from 'pg';
import { createApp } from '../../src/api/app';
import { handleGatewayEvent } from '../../src/api/payments/payments.service';
import { createGateway } from '../../src/gateway/gateway-app';
import { WebhookChaos } from '../../src/gateway/webhook-sender';
import { dispatchOnce } from '../../src/jobs/dispatcher';
import { LogEmailSender } from '../../src/jobs/email-sender';
import { reconcileOnce } from '../../src/jobs/reconciler';
import { GatewayClient } from '../../src/shared/gateway-client';
import { GatewayEvent } from '../../src/shared/gateway-types';
import { SIGNATURE_HEADER, signPayload } from '../../src/shared/webhook-signature';
import {
  createTestEvent,
  createTestPool,
  createTestRedis,
  freePort,
  listen,
  resetDatabase,
  TEST_GATEWAY_KEY,
  TEST_WEBHOOK_SECRET,
  testAppDeps,
  waitFor,
} from './helpers';

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
});

const NO_CHAOS: WebhookChaos = { duplicateRate: 0, maxDelayMs: 0, dropRate: 0 };

// Seatlock's API and the mock gateway, each on its own port, sending each other requests
// and webhooks like the real thing.
async function startSystem(chaos: WebhookChaos = NO_CHAOS) {
  const seatlockPort = await freePort();
  const gateway = createGateway({
    apiKey: TEST_GATEWAY_KEY,
    webhookUrl: `http://localhost:${seatlockPort}/v1/webhooks/gateway`,
    webhookSecret: TEST_WEBHOOK_SECRET,
    chaos,
    paymentExpiryMs: 60_000,
  });
  const gatewayServer = await listen(gateway.app);
  const gatewayClient = new GatewayClient(gatewayServer.url, TEST_GATEWAY_KEY);
  const deps = testAppDeps(pool, redis, { gateway: gatewayClient });
  const app = createApp(deps);
  const seatlock = await new Promise<import('node:http').Server>((resolve) => {
    const s = app.listen(seatlockPort, () => resolve(s));
  });
  const url = `http://localhost:${seatlockPort}`;

  const call = async (method: string, path: string, body?: object, user = 'alice', key?: string) => {
    const res = await fetch(`${url}${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        'X-User-Id': user,
        ...(key ? { 'Idempotency-Key': key } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    return { status: res.status, body: await res.json() };
  };

  return {
    url,
    gatewayUrl: gatewayServer.url,
    gatewayClient,
    deps,
    call,
    async holdSeats(eventId: string, seatIds: number[], user = 'alice') {
      const res = await call('POST', `/v1/events/${eventId}/holds`, { seatIds }, user, `key-${user}-${seatIds.join('-')}`);
      expect(res.status).toBe(201);
      return res.body.id as string;
    },
    async checkout(holdId: string, user = 'alice') {
      return call('POST', `/v1/holds/${holdId}/checkout`, { email: `${user}@example.com` }, user);
    },
    async pay(gatewayPaymentId: string, outcome: 'succeeded' | 'failed') {
      const res = await fetch(`${gatewayServer.url}/v1/payments/${gatewayPaymentId}/complete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ outcome }),
      });
      expect(res.status).toBe(200);
    },
    async stats() {
      return (await fetch(`${gatewayServer.url}/v1/stats`)).json();
    },
    async close() {
      gateway.close();
      await gatewayServer.close();
      await new Promise((resolve) => seatlock.close(resolve));
    },
  };
}

type System = Awaited<ReturnType<typeof startSystem>>;
let system: System;

afterEach(async () => {
  await system?.close();
});

const paymentStatus = async (paymentId: string) =>
  (await pool.query<{ status: string }>('SELECT status FROM payments WHERE id = $1', [paymentId])).rows[0]?.status;

const bookingFor = async (holdId: string) =>
  (await pool.query<{ id: string }>('SELECT id FROM bookings WHERE hold_id = $1', [holdId])).rows[0]?.id;

async function expireHold(holdId: string) {
  await pool.query(`UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = $1`, [holdId]);
  await pool.query(`UPDATE seats SET held_until = now() - interval '1 second' WHERE hold_id = $1`, [holdId]);
}

function signedEvent(event: GatewayEvent, timestamp?: number) {
  const body = JSON.stringify(event);
  return {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', [SIGNATURE_HEADER]: signPayload(TEST_WEBHOOK_SECRET, body, timestamp) },
    body,
  };
}

describe('checkout and payment', () => {
  it('books the seats when the payment succeeds, and queues the confirmation email', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0], seatIds[1]]);

    const checkout = await system.checkout(holdId);
    expect(checkout.status).toBe(201);
    expect(checkout.body).toMatchObject({ amountPaise: 200_000, status: 'pending' });
    await system.pay(checkout.body.gatewayPaymentId, 'succeeded');

    const bookingId = await waitFor(() => bookingFor(holdId));
    const seats = await pool.query(`SELECT status FROM seats WHERE id = ANY($1::bigint[])`, [[seatIds[0], seatIds[1]]]);
    expect(seats.rows.every((s) => s.status === 'booked')).toBe(true);
    const hold = await system.call('GET', `/v1/holds/${holdId}`);
    expect(hold.body).toMatchObject({ status: 'booked', bookingId, payment: { status: 'succeeded' } });
    const booking = await system.call('GET', `/v1/bookings/${bookingId}`);
    expect(booking.body.seats.map((s: { label: string }) => s.label)).toEqual(['Gold A1', 'Gold A2']);

    const outbox = await pool.query(`SELECT topic, payload FROM outbox`);
    expect(outbox.rows).toEqual([
      {
        topic: 'booking.confirmed',
        payload: expect.objectContaining({ bookingId, email: 'alice@example.com', seats: ['Gold A1', 'Gold A2'] }),
      },
    ]);
  });

  it('reuses the payment when checkout is called twice', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);

    const first = await system.checkout(holdId);
    const second = await system.checkout(holdId);

    expect(second.body.paymentId).toBe(first.body.paymentId);
    expect(second.body.gatewayPaymentId).toBe(first.body.gatewayPaymentId);
    expect((await system.stats()).payments).toEqual({ created: 1 });
  });

  it('keeps the hold after a failed payment, so the user can pay again', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const first = await system.checkout(holdId);

    await system.pay(first.body.gatewayPaymentId, 'failed');
    await waitFor(async () => (await paymentStatus(first.body.paymentId)) === 'failed');

    const retry = await system.checkout(holdId);
    expect(retry.status).toBe(201);
    expect(retry.body.paymentId).not.toBe(first.body.paymentId);
    await system.pay(retry.body.gatewayPaymentId, 'succeeded');
    await waitFor(() => bookingFor(holdId));
  });

  it('refuses checkout for an expired hold', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    await expireHold(holdId);

    expect((await system.checkout(holdId)).status).toBe(409);
  });
});

describe('webhooks', () => {
  it('books once when the gateway delivers every webhook twice', async () => {
    system = await startSystem({ duplicateRate: 1, maxDelayMs: 0, dropRate: 0 });
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const checkout = await system.checkout(holdId);

    await system.pay(checkout.body.gatewayPaymentId, 'succeeded');

    await waitFor(async () => (await system.stats()).webhooks.deliveries === 2);
    const bookings = await pool.query('SELECT id FROM bookings');
    expect(bookings.rows).toHaveLength(1);
    const outbox = await pool.query('SELECT id FROM outbox');
    expect(outbox.rows).toHaveLength(1); // one confirmation email, not two
  });

  it('books once when the same event arrives 5 times at the same moment', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const checkout = await system.checkout(holdId);
    const gatewayEvent: GatewayEvent = {
      id: 'evt_same',
      type: 'payment.succeeded',
      createdAt: new Date().toISOString(),
      data: { paymentId: checkout.body.gatewayPaymentId, reference: checkout.body.paymentId, amountPaise: 100_000 },
    };

    const responses = await Promise.all(
      Array.from({ length: 5 }, () => fetch(`${system.url}/v1/webhooks/gateway`, signedEvent(gatewayEvent)).then((r) => r.json())),
    );

    expect(responses.filter((r) => r.duplicate)).toHaveLength(4);
    expect((await pool.query('SELECT id FROM bookings')).rows).toHaveLength(1);
  });

  it('rejects a bad signature and an old one', async () => {
    system = await startSystem();
    const gatewayEvent: GatewayEvent = {
      id: 'evt_1',
      type: 'payment.succeeded',
      createdAt: new Date().toISOString(),
      data: { paymentId: 'pay_x', reference: '00000000-0000-0000-0000-000000000000', amountPaise: 1 },
    };

    const tampered = signedEvent(gatewayEvent);
    tampered.body = tampered.body.replace('"amountPaise":1', '"amountPaise":2');
    const forged = await fetch(`${system.url}/v1/webhooks/gateway`, tampered);
    expect(forged.status).toBe(400);

    const tenMinutesAgo = Math.floor(Date.now() / 1000) - 600;
    const replayed = await fetch(`${system.url}/v1/webhooks/gateway`, signedEvent(gatewayEvent, tenMinutesAgo));
    expect(replayed.status).toBe(400);
    expect(await replayed.json()).toEqual({ error: 'invalid signature (expired)' });

    const unsigned = await fetch(`${system.url}/v1/webhooks/gateway`, { method: 'POST', body: JSON.stringify(gatewayEvent) });
    expect(unsigned.status).toBe(400);
  });

  it('ignores a "failed" that arrives after "succeeded" (status only moves forward)', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const checkout = await system.checkout(holdId);
    await system.pay(checkout.body.gatewayPaymentId, 'succeeded');
    await waitFor(() => bookingFor(holdId));

    const late = await handleGatewayEvent(pool, system.deps.holdStrategy, {
      id: 'evt_stale',
      type: 'payment.failed',
      createdAt: new Date().toISOString(),
      data: { paymentId: checkout.body.gatewayPaymentId, reference: checkout.body.paymentId, amountPaise: 100_000 },
    });

    expect(late).toEqual({ duplicate: false, outcome: 'ignored' });
    expect(await paymentStatus(checkout.body.paymentId)).toBe('succeeded');
  });
});

describe('late payments', () => {
  it('still books the seats if the hold expired but nobody took them', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const checkout = await system.checkout(holdId);
    await expireHold(holdId);

    await system.pay(checkout.body.gatewayPaymentId, 'succeeded');

    await waitFor(() => bookingFor(holdId));
  });

  it('refunds, and books none of the seats, if someone else took one of them', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0], seatIds[1]]);
    const checkout = await system.checkout(holdId);
    await expireHold(holdId);
    const bobsHold = await system.holdSeats(event.id, [seatIds[1]], 'bob'); // Bob takes seat 2

    await system.pay(checkout.body.gatewayPaymentId, 'succeeded');

    await waitFor(async () => (await paymentStatus(checkout.body.paymentId)) === 'refund_pending');
    expect(await bookingFor(holdId)).toBeUndefined();
    const seats = await pool.query<{ status: string; hold_id: string | null }>(
      'SELECT status, hold_id FROM seats WHERE id = ANY($1::bigint[]) ORDER BY id',
      [[seatIds[0], seatIds[1]]],
    );
    expect(seats.rows).toEqual([
      { status: 'available', hold_id: null }, // Alice's other seat goes back on sale
      { status: 'held', hold_id: bobsHold }, // Bob keeps his
    ]);
    const outbox = await pool.query<{ topic: string }>('SELECT topic FROM outbox ORDER BY id');
    expect(outbox.rows.map((r) => r.topic)).toEqual(['payment.refund', 'booking.failed']);

    // The dispatcher asks the gateway for the refund; the gateway confirms it by webhook.
    const dispatched = await dispatchOnce({
      pool,
      email: new LogEmailSender(),
      gateway: system.gatewayClient,
      batchSize: 10,
      concurrency: 2,
    });
    expect(dispatched).toMatchObject({ sent: 2 });
    await waitFor(async () => (await paymentStatus(checkout.body.paymentId)) === 'refunded');
    expect(await system.stats()).toMatchObject({ capturedPaise: 200_000, refundedPaise: 200_000 });
  });
});

describe('reconciler', () => {
  it('books the seats when the success webhook never arrives', async () => {
    system = await startSystem({ duplicateRate: 0, maxDelayMs: 0, dropRate: 1 });
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const checkout = await system.checkout(holdId);
    await system.pay(checkout.body.gatewayPaymentId, 'succeeded');
    expect(await paymentStatus(checkout.body.paymentId)).toBe('pending');

    const result = await reconcileOnce(pool, system.gatewayClient, system.deps.holdStrategy, 0);

    expect(result).toMatchObject({ checked: 1, succeeded: 1 });
    expect(await bookingFor(holdId)).toBeDefined();
  });

  it('lets the user pay again when the failure webhook never arrived', async () => {
    system = await startSystem({ duplicateRate: 0, maxDelayMs: 0, dropRate: 1 });
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const first = await system.checkout(holdId);
    await system.pay(first.body.gatewayPaymentId, 'failed');

    // Checkout asks the gateway about the pending payment, finds it failed, starts a new one.
    const retry = await system.checkout(holdId);

    expect(retry.status).toBe(201);
    expect(retry.body.paymentId).not.toBe(first.body.paymentId);
    expect(await paymentStatus(first.body.paymentId)).toBe('failed');
  });

  it('fails payments the gateway never heard of', async () => {
    system = await startSystem();
    const { event, seatIds } = await createTestEvent(pool);
    const holdId = await system.holdSeats(event.id, [seatIds[0]]);
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO payments (hold_id, amount_paise) VALUES ($1, 100000) RETURNING id`,
      [holdId],
    );

    const result = await reconcileOnce(pool, system.gatewayClient, system.deps.holdStrategy, 0);

    expect(result.abandoned).toBe(1);
    expect(await paymentStatus(rows[0].id)).toBe('failed');
  });
});
