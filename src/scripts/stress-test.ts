import { ChildProcess, execSync, spawn } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { availableParallelism } from 'node:os';
import path from 'node:path';
import express from 'express';
import Redis from 'ioredis';
import { Pool } from 'pg';
import { flashKeys } from '../api/flash/flash-strategies';
import { reconcileFlashSale } from '../flash/reconcile-flash';
import { createPool } from '../shared/db';
import { BatchingProducer, consumerLag, createProducer } from '../shared/kafka';
import { runMigrations } from '../shared/migrations';

/**
 * npm run stress -- <scenario>
 *
 *   seated          users compete for the seats of one event: hold, pay (some decline, some
 *                   abandon, some pay too late), get a booking and a confirmation email
 *   flash           a crowd tries to buy limited stock
 *   compare-holds   `seated` once per hold strategy (pessimistic, optimistic, redis)
 *   compare-flash   `flash` once per flash strategy (postgres, redis-kafka)
 *
 * It starts the whole system (API processes, mock gateway, background jobs, order writer)
 * against its own database (seatlock_load), Redis database (2) and Kafka topic, runs k6,
 * waits until every payment, refund, email and order has settled, and then CHECKS the
 * result: no seat sold twice, no item oversold, every paying user got seats or a refund,
 * the money adds up. The report goes to load-results/<time>-<scenario>/report.md.
 *
 * Settings (environment variables): RATE, DURATION, SEATS, STOCK, HOLD_STRATEGY,
 * FLASH_STRATEGY, API_PROCESSES, CHAOS=true (kill -9 a background process mid-load).
 */

const ROOT = path.join(__dirname, '..', '..');
const env = process.env;
const scenario = process.argv[2] ?? 'seated';
const CORES = availableParallelism();

const settings = {
  apiProcesses: Number(env.API_PROCESSES ?? Math.max(2, Math.floor(CORES / 2))),
  chaos: env.CHAOS === 'true',
  databaseUrl: env.LOAD_DATABASE_URL ?? 'postgres://seatlock:seatlock@localhost:5433/seatlock_load',
  redisUrl: env.LOAD_REDIS_URL ?? 'redis://localhost:6380/2',
  kafkaBrokers: env.KAFKA_BROKERS ?? 'localhost:9092',
  k6: env.K6_BIN ?? 'k6',
  apiPort: 4000,
  gatewayPort: 4100,
};
const API_URL = `http://localhost:${settings.apiPort}`;
const GATEWAY_URL = `http://localhost:${settings.gatewayPort}`;

interface SeatedOptions {
  holdStrategy: string;
  rate: number;
  duration: string;
  seats: number;
  holdSeconds: number;
}
interface FlashOptions {
  flashStrategy: string;
  rate: number;
  duration: string;
  stock: number;
}

// ---------- processes ----------

const running = new Set<Proc>();

class Proc {
  child: ChildProcess | null = null;
  constructor(
    readonly name: string,
    private script: string,
    private procEnv: Record<string, string>,
  ) {}
  start() {
    running.add(this);
    this.child = spawn('node', [path.join(ROOT, 'dist', this.script)], {
      env: { PATH: env.PATH ?? '', HOME: env.HOME ?? '', LOG_LEVEL: 'warn', ...this.procEnv },
      stdio: ['ignore', 'ignore', 'inherit'],
    });
  }
  kill9() {
    this.child?.kill('SIGKILL');
  }
  async stop() {
    running.delete(this);
    if (!this.child || this.child.exitCode !== null) return;
    const exited = new Promise((resolve) => this.child!.once('exit', resolve));
    this.child.kill('SIGTERM');
    await Promise.race([exited, sleep(10_000)]);
    this.child.kill('SIGKILL');
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitUntil(what: string, check: () => Promise<boolean>, timeoutMs: number): Promise<number> {
  const start = Date.now();
  while (!(await check().catch(() => false))) {
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await sleep(250);
  }
  return Date.now() - start;
}

const isUp = (url: string) => () => fetch(url).then((r) => r.ok);

// Stands in for Relay: answers 202 and counts each Idempotency-Key, so the report can show
// how many distinct emails were requested and how many requests were retries.
async function startFakeRelay() {
  const keys = new Map<string, number>();
  const app = express();
  app.post('/v1/emails', (req, res) => {
    const key = req.get('Idempotency-Key') ?? '';
    keys.set(key, (keys.get(key) ?? 0) + 1);
    res.status(202).json({ status: 'queued' });
  });
  const server = app.listen(0);
  await new Promise((resolve) => server.once('listening', resolve));
  const port = (server.address() as { port: number }).port;
  return { url: `http://localhost:${port}`, keys, close: () => server.close() };
}

function processEnv(extra: Record<string, string>): Record<string, string> {
  return {
    DATABASE_URL: settings.databaseUrl,
    REDIS_URL: settings.redisUrl,
    KAFKA_BROKERS: settings.kafkaBrokers,
    GATEWAY_URL,
    DB_POOL_SIZE: '20',
    ...extra,
  };
}

async function resetState(pool: Pool, redis: Redis) {
  await runMigrations(pool);
  const { rows } = await pool.query<{ tablename: string }>(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`,
  );
  await pool.query(`TRUNCATE ${rows.map((r) => r.tablename).join(', ')} RESTART IDENTITY CASCADE`);
  await redis.flushdb();
}

// ---------- k6 ----------

function runK6(script: string, k6Env: Record<string, string>): Promise<Record<string, number>> {
  return new Promise((resolve, reject) => {
    const child = spawn(settings.k6, ['run', '--quiet', path.join(ROOT, 'load', script)], {
      env: { ...env, ...k6Env, API_URL },
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    let output = '';
    child.stdout.on('data', (chunk) => (output += chunk));
    child.on('error', reject);
    child.on('exit', () => {
      const line = output.split('\n').find((l) => l.startsWith('K6_RESULT '));
      if (!line) return reject(new Error(`k6 printed no result:\n${output}`));
      resolve(JSON.parse(line.slice('K6_RESULT '.length)));
    });
  });
}

// Database work during the run, from Postgres's own counters.
async function dbCounters(pool: Pool) {
  const db = await pool.query<{ xacts: string; deadlocks: string }>(
    `SELECT xact_commit + xact_rollback AS xacts, deadlocks FROM pg_stat_database WHERE datname = current_database()`,
  );
  const tables = await pool.query<{ relname: string; n_tup_upd: string }>(
    `SELECT relname, n_tup_upd FROM pg_stat_user_tables WHERE relname IN ('seats', 'flash_sales')`,
  );
  const updates = Object.fromEntries(tables.rows.map((r) => [r.relname, Number(r.n_tup_upd)]));
  return { transactions: Number(db.rows[0].xacts), deadlocks: Number(db.rows[0].deadlocks), seatUpdates: updates.seats ?? 0, saleRowUpdates: updates.flash_sales ?? 0 };
}

// ---------- seated ----------

async function runSeated(o: SeatedOptions) {
  const pool = createPool(settings.databaseUrl, 5);
  const redis = new Redis(settings.redisUrl);
  await resetState(pool, redis);
  const relay = await startFakeRelay();

  const api = new Proc('api', 'api/server.js', processEnv({
    PORT: String(settings.apiPort),
    API_PROCESSES: String(settings.apiProcesses),
    HOLD_STRATEGY: o.holdStrategy,
    FLASH_STRATEGY: 'postgres',
  }));
  const gateway = new Proc('gateway', 'gateway/server.js', {
    GATEWAY_PORT: String(settings.gatewayPort),
    GATEWAY_WEBHOOK_URL: `${API_URL}/v1/webhooks/gateway`,
    // Real gateways misbehave; so does this one.
    WEBHOOK_DUPLICATE_RATE: '0.1',
    WEBHOOK_MAX_DELAY_MS: '2000',
    WEBHOOK_DROP_RATE: '0.02',
    PAYMENT_EXPIRY_MS: String((o.holdSeconds + 20) * 1000),
  });
  const jobs = new Proc('jobs', 'jobs/main.js', processEnv({
    HOLD_STRATEGY: o.holdStrategy,
    RELAY_URL: relay.url,
    RECONCILE_AFTER_MS: '5000',
    RECONCILE_INTERVAL_MS: '2000',
  }));
  for (const p of [api, gateway, jobs]) p.start();
  await waitUntil('the API', isUp(`${API_URL}/health`), 30_000);
  await waitUntil('the gateway', isUp(`${GATEWAY_URL}/v1/stats`), 30_000);

  // A stadium: SEATS seats in rows of 50, front sections pricier.
  const rows = Math.ceil(o.seats / 50);
  const res = await fetch(`${API_URL}/v1/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: 'Stress test concert',
      venue: 'Load stadium',
      startsAt: new Date(Date.now() + 86_400_000).toISOString(),
      holdSeconds: o.holdSeconds,
      sections: [
        { name: 'Platinum', rows: Math.max(1, Math.round(rows * 0.2)), seatsPerRow: 50, pricePaise: 1_500_000 },
        { name: 'Gold', rows: Math.max(1, rows - Math.round(rows * 0.2)), seatsPerRow: 50, pricePaise: 500_000 },
      ],
    }),
  });
  const event = (await res.json()) as { id: string; seatCount: number };

  // Timeline: seats taken (held or booked) and booked, every second.
  const timeline: { t: number; taken: number; booked: number }[] = [];
  const started = Date.now();
  const sampler = setInterval(async () => {
    const { rows: r } = await pool.query<{ taken: number; booked: number }>(
      `SELECT count(*) FILTER (WHERE status = 'booked' OR (status = 'held' AND held_until > now()))::int AS taken,
              count(*) FILTER (WHERE status = 'booked')::int AS booked
         FROM seats WHERE event_id = $1`,
      [event.id],
    );
    timeline.push({ t: Math.round((Date.now() - started) / 1000), ...r[0] });
  }, 1000);

  const chaosEvents = scheduleChaos(o.duration, [jobs], api);
  const before = await dbCounters(pool);
  const k6 = await runK6('seated.js', {
    EVENT_ID: event.id,
    RATE: String(o.rate),
    DURATION: o.duration,
    HOLD_SECONDS: String(o.holdSeconds),
  });
  const after = await dbCounters(pool);
  clearInterval(sampler);
  chaosEvents.cancel();

  // Settle: every payment decided, every refund done, every email sent, every webhook in.
  const stats = () => fetch(`${GATEWAY_URL}/v1/stats`).then((r) => r.json() as Promise<GatewayStats>);
  const drainMs = await waitUntil(
    'payments, refunds and emails to settle',
    async () => {
      const { rows: r } = await pool.query<{ open: number }>(
        `SELECT (SELECT count(*) FROM payments WHERE status IN ('pending', 'refund_pending'))
              + (SELECT count(*) FROM outbox WHERE published_at IS NULL) AS open`,
      );
      return Number(r[0].open) === 0 && (await stats()).webhooks.pending === 0;
    },
    240_000,
  );

  const checks = await seatedChecks(pool, event.id, await stats(), relay.keys);
  for (const p of [api, gateway, jobs]) await p.stop();
  relay.close();
  redis.disconnect();
  await pool.end();

  return {
    options: o,
    event,
    k6,
    db: {
      transactions: after.transactions - before.transactions,
      deadlocks: after.deadlocks - before.deadlocks,
      seatUpdates: after.seatUpdates - before.seatUpdates,
    },
    drainMs,
    timeline,
    chaos: chaosEvents.log,
    checks,
  };
}

interface GatewayStats {
  capturedPaise: number;
  refundedPaise: number;
  payments: Record<string, number>;
  webhooks: { events: number; deliveries: number; duplicatesSent: number; dropped: number; failedAttempts: number; pending: number };
}

async function seatedChecks(pool: Pool, eventId: string, gateway: GatewayStats, emailKeys: Map<string, number>) {
  const one = async (sql: string, params: unknown[] = []) => Number((await pool.query(sql, params)).rows[0].n);
  const bookings = await one('SELECT count(*) AS n FROM bookings');
  const bookedSeats = await one(`SELECT count(*) AS n FROM booking_seats`);
  const bookedAmount = await one('SELECT coalesce(sum(amount_paise), 0) AS n FROM bookings');
  const failedNotices = await one(`SELECT count(*) AS n FROM outbox WHERE topic = 'booking.failed'`);
  const emailRequests = [...emailKeys.values()].reduce((a, b) => a + b, 0);

  return {
    seats: await one('SELECT count(*) AS n FROM seats WHERE event_id = $1', [eventId]),
    bookings,
    bookedSeats,
    refunds: await one(`SELECT count(*) AS n FROM payments WHERE status = 'refunded'`),
    lateButBooked: await one(
      `SELECT count(*) AS n FROM bookings b JOIN holds h ON h.id = b.hold_id WHERE b.created_at > h.expires_at`,
    ),
    paymentsFailed: await one(`SELECT count(*) AS n FROM payments WHERE status = 'failed'`),
    webhooks: gateway.webhooks,
    // ---- must all be 0 ----
    seatsSoldTwice: await one(
      'SELECT count(*) AS n FROM (SELECT seat_id FROM booking_seats GROUP BY seat_id HAVING count(*) > 1) x',
    ),
    seatStatusMismatch: await one(
      `SELECT count(*) AS n FROM seats s LEFT JOIN booking_seats bs ON bs.seat_id = s.id
        WHERE s.event_id = $1 AND (s.status = 'booked') <> (bs.seat_id IS NOT NULL)`,
      [eventId],
    ),
    paidWithoutSeatsOrRefund: await one(
      `SELECT count(*) AS n FROM payments p
        WHERE p.status = 'succeeded' AND NOT EXISTS (SELECT 1 FROM bookings b WHERE b.payment_id = p.id)`,
    ),
    paymentsNotSettled: await one(`SELECT count(*) AS n FROM payments WHERE status IN ('pending', 'refund_pending')`),
    holdsWithTwoLivePayments: await one(
      `SELECT count(*) AS n FROM (SELECT hold_id FROM payments WHERE status <> 'failed' GROUP BY hold_id HAVING count(*) > 1) x`,
    ),
    // Money captured minus money refunded must equal what the bookings are worth.
    moneyMismatchPaise: gateway.capturedPaise - gateway.refundedPaise - bookedAmount,
    // One email per booking and per "sorry, refunded" notice (Relay dedupes the retries).
    emailsMissing: bookings + failedNotices - emailKeys.size,
    emailRetriesDeduplicated: emailRequests - emailKeys.size,
  };
}

// ---------- flash ----------

async function runFlash(o: FlashOptions) {
  const pool = createPool(settings.databaseUrl, 5);
  const redis = new Redis(settings.redisUrl);
  await resetState(pool, redis);
  const stamp = Date.now();
  const topic = `flash-orders-load-${stamp}`;

  const kafkaEnv = { FLASH_ORDERS_TOPIC: topic, FLASH_TOPIC_PARTITIONS: '6' };
  const api = new Proc('api', 'api/server.js', processEnv({
    PORT: String(settings.apiPort),
    API_PROCESSES: String(settings.apiProcesses),
    FLASH_STRATEGY: o.flashStrategy,
    ...kafkaEnv,
  }));
  const writer = new Proc('order-writer', 'flash/main.js', processEnv({ ...kafkaEnv, ORDER_WRITER_GROUP: `writer-${stamp}` }));
  const procs = o.flashStrategy === 'redis-kafka' ? [api, writer] : [api];
  for (const p of procs) p.start();
  await waitUntil('the API', isUp(`${API_URL}/health`), 60_000);

  const res = await fetch(`${API_URL}/v1/flash-sales`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'Stress test sneakers', pricePaise: 999_900, stock: o.stock }),
  });
  const sale = (await res.json()) as { id: string };

  const timeline: { t: number; remaining: number; written: number }[] = [];
  const started = Date.now();
  const sampler = setInterval(async () => {
    const written = Number((await pool.query('SELECT count(*) AS n FROM flash_orders')).rows[0].n);
    const inRedis = await redis.get(flashKeys(sale.id).stock);
    const remaining = o.flashStrategy === 'redis-kafka' ? Number(inRedis) : o.stock - written;
    timeline.push({ t: Math.round((Date.now() - started) / 1000), remaining, written });
  }, 1000);

  const chaosEvents = scheduleChaos(o.duration, o.flashStrategy === 'redis-kafka' ? [writer] : [], api);
  const before = await dbCounters(pool);
  const k6 = await runK6('flash.js', { SALE_ID: sale.id, RATE: String(o.rate), DURATION: o.duration });
  clearInterval(sampler);
  chaosEvents.cancel();

  const orders = () => pool.query('SELECT count(*) AS n FROM flash_orders').then((r) => Number(r.rows[0].n));
  let drainMs = 0;
  let republished = 0;
  if (o.flashStrategy === 'redis-kafka') {
    const reservedInRedis = await redis.scard(flashKeys(sale.id).buyers);
    // Wait until the order writer has caught up: consumer lag 0, everything in Kafka written.
    const brokers = settings.kafkaBrokers.split(',');
    drainMs = await waitUntil(
      'the order writer to catch up (consumer lag 0)',
      async () => (await consumerLag(brokers, `writer-${stamp}`, topic, 6)) === 0,
      180_000,
    );
    // Then the reconciler re-publishes the orders that never reached Kafka.
    const producer = new BatchingProducer(createProducer(brokers, 'stress-reconcile'), topic, 5);
    republished = (await reconcileFlashSale(pool, redis, producer, sale.id)).republished;
    await producer.close();
    if (republished > 0) {
      await waitUntil('re-published orders to be written', async () => (await orders()) >= reservedInRedis, 60_000);
    }
  }
  const after = await dbCounters(pool);

  const one = async (sql: string) => Number((await pool.query(sql)).rows[0].n);
  const written = await orders();
  const sold = await one('SELECT sold AS n FROM flash_sales');
  const remainingInRedis = o.flashStrategy === 'redis-kafka' ? Number(await redis.get(flashKeys(sale.id).stock)) : null;
  const checks = {
    stock: o.stock,
    orders: written,
    // ---- must all be 0 ----
    oversold: Math.max(0, written - o.stock),
    soldCounterMismatch: sold - written,
    usersWithTwoOrders: await one(
      'SELECT count(*) AS n FROM (SELECT user_id FROM flash_orders GROUP BY user_id HAVING count(*) > 1) x',
    ),
    // Every buyer told "you got one" (202) has an order. (There can be MORE orders than 202s:
    // a buyer whose request died with a killed API process still gets the order they reserved.)
    toldYesButNoOrder: Math.max(0, k6.reserved - written),
    ordersWithoutAnswer: Math.max(0, written - k6.reserved),
    redisAndPostgresDisagree: remainingInRedis === null ? 0 : o.stock - remainingInRedis - written,
    unsoldWhileDemandExceededStock: k6.soldOut > 0 ? o.stock - written : 0,
    ordersRepublishedByReconciler: republished,
  };

  for (const p of procs) await p.stop();
  redis.disconnect();
  await pool.end();

  return {
    options: o,
    k6,
    db: { transactions: after.transactions - before.transactions, saleRowUpdates: after.saleRowUpdates - before.saleRowUpdates },
    drainMs,
    timeline,
    chaos: chaosEvents.log,
    checks,
  };
}

// ---------- chaos ----------

// With CHAOS=true: kill -9 each background process 40% into the load (restarted 3s later),
// and one API process at 60% (the cluster primary starts a new one).
function scheduleChaos(duration: string, background: Proc[], api: Proc) {
  const log: string[] = [];
  const timers: NodeJS.Timeout[] = [];
  if (settings.chaos) {
    const ms = parseDuration(duration);
    for (const p of background) {
      timers.push(
        setTimeout(() => {
          p.kill9();
          log.push(`${Math.round(ms * 0.4 / 1000)}s: kill -9 ${p.name}, restarted 3s later`);
          timers.push(setTimeout(() => p.start(), 3000));
        }, ms * 0.4),
      );
    }
    timers.push(
      setTimeout(() => {
        const worker = execSync(`pgrep -P ${api.child?.pid} | head -1`).toString().trim();
        if (worker) {
          process.kill(Number(worker), 'SIGKILL');
          log.push(`${Math.round(ms * 0.6 / 1000)}s: kill -9 one API process (pid ${worker})`);
        }
      }, ms * 0.6),
    );
  }
  return { log, cancel: () => timers.forEach(clearTimeout) };
}

function parseDuration(d: string): number {
  const m = /^(\d+)(s|m)$/.exec(d);
  if (!m) throw new Error(`DURATION must look like 60s or 2m, got ${d}`);
  return Number(m[1]) * (m[2] === 'm' ? 60_000 : 1000);
}

// ---------- report ----------

const ms = (v: number) => `${Math.round(v)} ms`;
const n = (v: number) => v.toLocaleString('en-US');

function checkLines(checks: Record<string, unknown>, mustBeZero: string[]) {
  return mustBeZero.map((k) => `| ${k} | ${checks[k]} | ${checks[k] === 0 ? 'PASS' : '**FAIL**'} |`).join('\n');
}

type SeatedResult = Awaited<ReturnType<typeof runSeated>>;
type FlashResult = Awaited<ReturnType<typeof runFlash>>;

const SEATED_MUST_BE_ZERO = [
  'seatsSoldTwice',
  'seatStatusMismatch',
  'paidWithoutSeatsOrRefund',
  'paymentsNotSettled',
  'holdsWithTwoLivePayments',
  'moneyMismatchPaise',
  'emailsMissing',
];
const FLASH_MUST_BE_ZERO = [
  'oversold',
  'soldCounterMismatch',
  'usersWithTwoOrders',
  'toldYesButNoOrder',
  'redisAndPostgresDisagree',
  'unsoldWhileDemandExceededStock',
];

function seatedReport(r: SeatedResult): string {
  const soldOut = r.timeline.find((p) => p.taken >= r.event.seatCount * 0.95);
  const c = r.checks;
  return `## Seated: ${r.options.holdStrategy} strategy

${n(r.options.rate)} new users/s for ${r.options.duration}, ${n(r.event.seatCount)} seats, holds last ${r.options.holdSeconds}s.
Each user looks at the availability list (cached up to 1s), wants 1-4 seats side by side, and 60% aim at the
front 10% of the stadium. Taken seats → they pick again (3 tries). After holding: 82% pay, 5% get declined and
pay on the second try, 10% abandon (the hold expires), 3% pay after their hold expired.

| | |
|---|---|
| Hold requests | ${n(r.k6.holdsCreated + r.k6.holdsConflict + r.k6.holdsError)} (${n(r.k6.holdsCreated)} won, ${n(r.k6.holdsConflict)} seats already taken, ${n(r.k6.holdsError)} errors) |
| Hold latency p50 / p95 / p99 | ${ms(r.k6.holdP50)} / ${ms(r.k6.holdP95)} / ${ms(r.k6.holdP99)} |
| 95% of seats held or booked after | ${soldOut ? `${soldOut.t}s` : 'never'} |
| Bookings | ${n(c.bookings)} (${n(c.bookedSeats)} of ${n(c.seats)} seats) |
| Payments declined, then retried | ${n(r.k6.paymentsDeclined)} |
| Paid after the hold expired | ${n(r.k6.paymentsLate)} (${n(c.lateButBooked)} still booked: nobody had taken their seats; ${n(c.refunds)} refunded) |
| Webhooks | ${n(c.webhooks.events)} events, ${n(c.webhooks.duplicatesSent)} sent twice, ${n(c.webhooks.dropped)} never sent (the reconciler found them) |
| Email requests deduplicated by Relay | ${n(c.emailRetriesDeduplicated)} |
| Users who found no free seats left | ${n(r.k6.sawSoldOut)} |
| Database transactions during the load | ${n(r.db.transactions)} |
| Deadlocks Postgres had to break | ${n(r.db.deadlocks)} |
| Time to settle after the load | ${(r.drainMs / 1000).toFixed(1)}s |
| k6 requests (dropped: k6 couldn't start in time) | ${n(r.k6.requests)} (dropped ${n(r.k6.dropped)}) |
${r.chaos.length ? `\nChaos: ${r.chaos.join('; ')}\n` : ''}
**Checks** (each must be 0):

| Check | Value | |
|---|---|---|
${checkLines(c, SEATED_MUST_BE_ZERO)}
`;
}

function flashReport(r: FlashResult): string {
  const soldOut = r.timeline.find((p) => p.remaining <= 0);
  const c = r.checks;
  return `## Flash sale: ${r.options.flashStrategy} strategy

${n(r.options.rate)} buy attempts/s for ${r.options.duration}, stock ${n(r.options.stock)}, every buyer a new user (5% click twice).

| | |
|---|---|
| Buy requests | ${n(r.k6.requests)} at ${n(Math.round(r.k6.rate))}/s (${n(r.k6.reserved)} got one, ${n(r.k6.replayed)} double clicks answered with the same order, ${n(r.k6.soldOut)} sold out, ${n(r.k6.busy)} busy, ${n(r.k6.errors)} errors) |
| Buy latency p50 / p95 / p99 / max | ${ms(r.k6.p50)} / ${ms(r.k6.p95)} / ${ms(r.k6.p99)} / ${ms(r.k6.max)} |
| Sold out after | ${soldOut ? `${soldOut.t}s` : 'not sold out'} |
| Orders written to Postgres | ${n(c.orders)} |
| Updates of the stock row | ${n(r.db.saleRowUpdates)} |
| Database transactions | ${n(r.db.transactions)} |
| Order writer caught up after the load | ${(r.drainMs / 1000).toFixed(1)}s |
| Orders recovered by the reconciler | ${n(c.ordersRepublishedByReconciler)} (reserved in Redis, never reached Kafka) |
| Orders whose buyer never saw the answer | ${n(c.ordersWithoutAnswer)} (their request died with a killed API process; the order is theirs) |
| k6 dropped iterations | ${n(r.k6.dropped)} |
${r.chaos.length ? `\nChaos: ${r.chaos.join('; ')}\n` : ''}
**Checks** (each must be 0):

| Check | Value | |
|---|---|---|
${checkLines(c, FLASH_MUST_BE_ZERO)}
| ordersRepublishedByReconciler | ${c.ordersRepublishedByReconciler} | not a failure: reservations that never reached Kafka (an API process died), recovered by the reconciler |
`;
}

function failures(checks: Record<string, unknown>, keys: string[]) {
  return keys.filter((k) => checks[k] !== 0);
}

// ---------- main ----------

async function main() {
  const rate = (fallback: number) => Number(env.RATE ?? fallback);
  const seated = (holdStrategy: string): SeatedOptions => ({
    holdStrategy,
    rate: rate(300),
    duration: env.DURATION ?? '60s',
    seats: Number(env.SEATS ?? 5000),
    holdSeconds: Number(env.HOLD_SECONDS ?? 15),
  });
  const flash = (flashStrategy: string): FlashOptions => ({
    flashStrategy,
    rate: rate(3000),
    duration: env.DURATION ?? '30s',
    stock: Number(env.STOCK ?? 5000),
  });

  console.log(`Building...`);
  execSync('npm run build', { cwd: ROOT, stdio: 'ignore' });
  const machine = `${CORES} cores, ${settings.apiProcesses} API processes${settings.chaos ? ', CHAOS on' : ''}`;
  console.log(`Stress test "${scenario}" on ${machine}`);

  const sections: string[] = [];
  const results: unknown[] = [];
  let failed: string[] = [];
  const plans: (() => Promise<void>)[] = [];
  const addSeated = (s: string) =>
    plans.push(async () => {
      console.log(`\n▶ seated, ${s} strategy`);
      const r = await runSeated(seated(s));
      results.push(r);
      sections.push(seatedReport(r));
      failed = failed.concat(failures(r.checks, SEATED_MUST_BE_ZERO).map((f) => `${s}: ${f}`));
      console.log(seatedReport(r));
    });
  const addFlash = (s: string) =>
    plans.push(async () => {
      console.log(`\n▶ flash, ${s} strategy`);
      const r = await runFlash(flash(s));
      results.push(r);
      sections.push(flashReport(r));
      failed = failed.concat(failures(r.checks, FLASH_MUST_BE_ZERO).map((f) => `${s}: ${f}`));
      console.log(flashReport(r));
    });

  if (scenario === 'seated') addSeated(env.HOLD_STRATEGY ?? 'pessimistic');
  else if (scenario === 'flash') addFlash(env.FLASH_STRATEGY ?? 'redis-kafka');
  else if (scenario === 'compare-holds') ['pessimistic', 'optimistic', 'redis'].forEach(addSeated);
  else if (scenario === 'compare-flash') ['postgres', 'redis-kafka'].forEach(addFlash);
  else throw new Error(`unknown scenario ${scenario}`);

  for (const plan of plans) await plan();

  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const dir = path.join(ROOT, 'load-results', `${stamp}-${scenario}`);
  mkdirSync(dir, { recursive: true });
  const report = `# Stress test: ${scenario}\n\nMachine: ${machine}. Everything (k6, API, Postgres, Redis, Kafka) on one machine.\n\n${sections.join('\n')}`;
  writeFileSync(path.join(dir, 'report.md'), report);
  writeFileSync(path.join(dir, 'results.json'), JSON.stringify(results, null, 2));
  console.log(`\nReport: ${path.relative(ROOT, path.join(dir, 'report.md'))}`);
  if (failed.length) {
    console.error(`FAILED CHECKS: ${failed.join(', ')}`);
    process.exit(1);
  }
  console.log('All checks passed.');
  process.exit(0);
}

main().catch(async (err) => {
  console.error(err);
  // Don't leave the API, gateway and workers running after a failed run.
  await Promise.all([...running].map((p) => p.stop()));
  process.exit(1);
});
