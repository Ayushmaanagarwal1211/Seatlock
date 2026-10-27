import { Pool } from 'pg';
import { dispatchOnce, DispatcherDeps } from '../../src/jobs/dispatcher';
import { RelayEmailSender } from '../../src/jobs/email-sender';
import { GatewayClient } from '../../src/shared/gateway-client';
import { startFakeRelay } from './fake-relay';
import { createTestPool, resetDatabase, TEST_GATEWAY_KEY } from './helpers';

let pool: Pool;
let relay: Awaited<ReturnType<typeof startFakeRelay>>;

beforeAll(() => {
  pool = createTestPool();
});

afterAll(async () => {
  await pool.end();
});

beforeEach(async () => {
  await resetDatabase(pool);
  relay = await startFakeRelay();
});

afterEach(async () => {
  await relay.close();
});

function deps(overrides: Partial<DispatcherDeps> = {}): DispatcherDeps {
  return {
    pool,
    email: new RelayEmailSender(relay.url),
    gateway: new GatewayClient('http://localhost:1', TEST_GATEWAY_KEY),
    batchSize: 100,
    concurrency: 10,
    ...overrides,
  };
}

async function addConfirmation(bookingId: string) {
  await pool.query(`INSERT INTO outbox (topic, payload) VALUES ('booking.confirmed', $1)`, [
    { bookingId, email: 'alice@example.com', eventName: 'Coldplay', seats: ['Gold A1', 'Gold A2'], amountPaise: 900_000 },
  ]);
}

const outboxRow = async () =>
  (await pool.query('SELECT published_at, attempts, next_attempt_at, last_error FROM outbox')).rows[0];

describe('outbox dispatcher', () => {
  it("sends the confirmation through Relay's API, with an idempotency key, and marks it done", async () => {
    await addConfirmation('b-1');

    expect(await dispatchOnce(deps())).toEqual({ picked: 1, sent: 1, retrying: 0, parked: 0 });

    expect(relay.requests).toEqual([
      {
        idempotencyKey: 'seatlock:booking-confirmed:b-1',
        body: expect.objectContaining({ to: 'alice@example.com', subject: 'Your tickets for Coldplay', priority: 'transactional' }),
      },
    ]);
    expect(relay.requests[0].body.body).toContain('Gold A1, Gold A2');
    expect(relay.requests[0].body.body).toContain('₹9,000');
    expect((await outboxRow()).published_at).not.toBeNull();
    expect((await dispatchOnce(deps())).picked).toBe(0); // nothing left to send
  });

  it('retries later with a growing delay when Relay is down, then succeeds', async () => {
    await addConfirmation('b-1');
    relay.failNext(1, 503);

    expect(await dispatchOnce(deps())).toMatchObject({ retrying: 1 });
    const row = await outboxRow();
    expect(row).toMatchObject({ published_at: null, attempts: 1, last_error: expect.stringContaining('503') });
    expect(row.next_attempt_at.getTime()).toBeGreaterThan(Date.now() + 3000);
    expect((await dispatchOnce(deps())).picked).toBe(0); // not due yet

    await pool.query('UPDATE outbox SET next_attempt_at = now()');
    expect(await dispatchOnce(deps())).toMatchObject({ sent: 1 });
  });

  it("parks a row that can never succeed instead of retrying it forever", async () => {
    await addConfirmation('b-1');
    relay.failNext(1, 422);

    expect(await dispatchOnce(deps())).toMatchObject({ parked: 1 });
    const { rows } = await pool.query(`SELECT 1 FROM outbox WHERE next_attempt_at = 'infinity'`);
    expect(rows).toHaveLength(1);
  });

  it("doesn't let one failing row hold up the others", async () => {
    await addConfirmation('b-1');
    await addConfirmation('b-2');
    relay.failNext(1);

    expect(await dispatchOnce(deps({ concurrency: 1 }))).toMatchObject({ sent: 1, retrying: 1 });
  });

  it('never sends a row twice when two dispatchers run at once (SKIP LOCKED)', async () => {
    for (let i = 0; i < 50; i++) await addConfirmation(`b-${i}`);

    const results = await Promise.all([dispatchOnce(deps({ batchSize: 30 })), dispatchOnce(deps({ batchSize: 30 }))]);

    expect(results[0].sent + results[1].sent).toBe(50);
    const keys = relay.requests.map((r) => r.idempotencyKey);
    expect(new Set(keys).size).toBe(50);
    expect(keys).toHaveLength(50);
  });

  it('skips the email when the payment had no contact address', async () => {
    await pool.query(`INSERT INTO outbox (topic, payload) VALUES ('booking.confirmed', '{"bookingId":"b-1"}')`);

    expect(await dispatchOnce(deps())).toMatchObject({ sent: 1 });
    expect(relay.requests).toHaveLength(0);
  });
});
