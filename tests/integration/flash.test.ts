import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { Pool } from 'pg';
import { flashKeys, orderIdFor, PostgresFlashStrategy, RedisKafkaFlashStrategy } from '../../src/api/flash/flash-strategies';
import { createFlashSale } from '../../src/api/flash/flash.service';
import { OrderConsumer } from '../../src/flash/order-consumer';
import { writeOrders } from '../../src/flash/order-writer';
import { reconcileFlashSale } from '../../src/flash/reconcile-flash';
import { BatchingProducer, createProducer, ensureTopic, FlashOrderMessage } from '../../src/shared/kafka';
import { createTestPool, createTestRedis, resetDatabase, waitFor } from './helpers';
import { testKafkaBrokers } from './test-db';

let pool: Pool;
let redis: Redis;
const brokers = testKafkaBrokers();
// A fresh topic per run, so earlier runs' messages can't leak into these tests.
const topic = `flash-orders-test-${Date.now()}`;
let producer: BatchingProducer;

beforeAll(async () => {
  pool = createTestPool();
  redis = createTestRedis();
  await ensureTopic(brokers, topic, 3);
  producer = new BatchingProducer(createProducer(brokers, 'flash-test'), topic, 5);
});

afterAll(async () => {
  await producer.close();
  await pool.end();
  redis.disconnect();
});

beforeEach(async () => {
  await resetDatabase(pool);
  await redis.flushdb();
});

const newSale = (stock: number) => createFlashSale(pool, redis, { name: 'Limited sneakers', pricePaise: 999_900, stock });

const saleRow = async (id: string) =>
  (await pool.query<{ sold: number; stock: number }>('SELECT sold, stock FROM flash_sales WHERE id = $1', [id])).rows[0];
const orderCount = async (id: string) =>
  (await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM flash_orders WHERE sale_id = $1', [id])).rows[0].n;

function buyers(n: number) {
  return Array.from({ length: n }, (_, i) => `user-${i}`);
}

async function startWriter() {
  const consumer = new OrderConsumer({ pool, brokers, topic, groupId: `writer-${randomUUID()}`, batchSize: 100, flushMs: 20 });
  await consumer.start();
  return consumer;
}

describe('postgres strategy (the hot-row baseline)', () => {
  it('sells exactly the stock to 200 buyers arriving at once', async () => {
    const sale = await newSale(20);
    const strategy = new PostgresFlashStrategy(pool);

    const results = await Promise.all(buyers(200).map((user) => strategy.buy(sale.id, user)));

    expect(results.filter((r) => r.kind === 'reserved')).toHaveLength(20);
    expect(results.filter((r) => r.kind === 'sold_out')).toHaveLength(180);
    expect(await saleRow(sale.id)).toEqual({ sold: 20, stock: 20 });
    expect(await orderCount(sale.id)).toBe(20);
  });

  it('gives one per user, even to a user clicking 10 times at once', async () => {
    const sale = await newSale(5);
    const strategy = new PostgresFlashStrategy(pool);

    const results = await Promise.all(Array.from({ length: 10 }, () => strategy.buy(sale.id, 'alice')));

    expect(results.filter((r) => r.kind === 'reserved')).toHaveLength(1);
    expect(results.filter((r) => r.kind === 'already')).toHaveLength(9);
    expect((await saleRow(sale.id)).sold).toBe(1);
  });
});

describe('redis-kafka strategy', () => {
  it('sells exactly the stock, and the order writer records every order from Kafka', async () => {
    const sale = await newSale(20);
    const strategy = new RedisKafkaFlashStrategy(pool, redis, producer);
    const writer = await startWriter();

    const results = await Promise.all(buyers(200).map((user) => strategy.buy(sale.id, user)));

    const reserved = results.filter((r) => r.kind === 'reserved');
    expect(reserved).toHaveLength(20);
    expect(results.filter((r) => r.kind === 'sold_out')).toHaveLength(180);
    expect(await redis.get(flashKeys(sale.id).stock)).toBe('0');

    await waitFor(async () => (await orderCount(sale.id)) === 20, 15_000);
    expect(await saleRow(sale.id)).toEqual({ sold: 20, stock: 20 });
    await writer.stop();
  });

  it('answers a second buy from the same user with the same order id', async () => {
    const sale = await newSale(5);
    const strategy = new RedisKafkaFlashStrategy(pool, redis, producer);

    const first = await strategy.buy(sale.id, 'alice');
    const second = await strategy.buy(sale.id, 'alice');

    expect(first).toEqual({ kind: 'reserved', orderId: orderIdFor(sale.id, 'alice') });
    expect(second).toEqual({ kind: 'already', orderId: orderIdFor(sale.id, 'alice') });
    expect(await redis.get(flashKeys(sale.id).stock)).toBe('4');
  });

  it('gives the item back when Kafka is unavailable', async () => {
    const sale = await newSale(5);
    const failing = { send: () => Promise.reject(new Error('broker unreachable')) };
    const strategy = new RedisKafkaFlashStrategy(pool, redis, failing);

    expect(await strategy.buy(sale.id, 'alice')).toEqual({ kind: 'unavailable' });

    expect(await redis.get(flashKeys(sale.id).stock)).toBe('5');
    expect(await redis.sismember(flashKeys(sale.id).buyers, 'alice')).toBe(0);
  });

  it('returns not_found for a sale that does not exist', async () => {
    const strategy = new RedisKafkaFlashStrategy(pool, redis, producer);
    expect(await strategy.buy(randomUUID(), 'alice')).toEqual({ kind: 'not_found' });
  });
});

describe('order writer', () => {
  const message = (saleId: string, userId: string): FlashOrderMessage => ({
    orderId: orderIdFor(saleId, userId),
    saleId,
    userId,
    reservedAt: new Date().toISOString(),
  });

  it('ignores a batch delivered twice', async () => {
    const sale = await newSale(10);
    const batch = buyers(5).map((u) => message(sale.id, u));

    expect(await writeOrders(pool, batch)).toEqual({ inserted: 5, duplicates: 0, rejected: 0 });
    expect(await writeOrders(pool, batch)).toEqual({ inserted: 0, duplicates: 5, rejected: 0 });
    expect((await saleRow(sale.id)).sold).toBe(5);
  });

  it('refuses to oversell even if Redis let too many through', async () => {
    const sale = await newSale(3);
    // Redis lost its data and the stock came back as 100: 6 buyers got a yes.
    const batch = buyers(6).map((u) => message(sale.id, u));

    const result = await writeOrders(pool, batch);

    expect(result).toEqual({ inserted: 3, duplicates: 0, rejected: 3 });
    expect(await saleRow(sale.id)).toEqual({ sold: 3, stock: 3 });
  });

  it('picks up where it left off after a restart (offsets committed after writing)', async () => {
    const sale = await newSale(100);
    const groupId = `writer-${randomUUID()}`;
    const start = async () => {
      const c = new OrderConsumer({ pool, brokers, topic, groupId, batchSize: 10, flushMs: 20 });
      await c.start();
      return c;
    };

    const first = await start();
    await Promise.all(buyers(30).map((u) => producer.send(message(sale.id, u))));
    await waitFor(async () => (await orderCount(sale.id)) === 30, 15_000);
    await first.stop();

    await Promise.all(buyers(50).slice(30).map((u) => producer.send(message(sale.id, u))));
    const second = await start();
    await waitFor(async () => (await orderCount(sale.id)) === 50, 15_000);
    await second.stop();
    expect((await saleRow(sale.id)).sold).toBe(50);
  });
});

describe('flash reconciler', () => {
  it('publishes the order of a buyer whose API process died before Kafka got it', async () => {
    const sale = await newSale(5);
    const writer = await startWriter();
    // Redis said yes to Alice, then the process died: no Kafka message, no order.
    await redis.decr(flashKeys(sale.id).stock);
    await redis.sadd(flashKeys(sale.id).buyers, 'alice');

    const result = await reconcileFlashSale(pool, redis, producer, sale.id);

    expect(result).toMatchObject({ reservedInRedis: 1, ordersInPostgres: 0, republished: 1 });
    await waitFor(async () => (await orderCount(sale.id)) === 1, 15_000);
    await writer.stop();
  });
});
