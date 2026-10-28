import { reconcileFlashSale } from '../flash/reconcile-flash';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { BatchingProducer, createProducer } from '../shared/kafka';
import { createRedis, waitForRedis } from '../shared/redis';

// npm run flash:reconcile -- <saleId>
// After a sale: publishes orders for buyers Redis said yes to but Postgres has no order for.
async function main() {
  const saleId = process.argv[2];
  if (!saleId) throw new Error('usage: npm run flash:reconcile -- <saleId>');
  const pool = createPool(config.DATABASE_URL);
  const redis = createRedis(config.REDIS_URL);
  await waitForRedis(redis);
  const producer = new BatchingProducer(createProducer(config.KAFKA_BROKERS, 'seatlock-reconcile'), config.FLASH_ORDERS_TOPIC, 5);
  try {
    console.log(await reconcileFlashSale(pool, redis, producer, saleId));
  } finally {
    await producer.close();
    redis.disconnect();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
