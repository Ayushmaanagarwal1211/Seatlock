import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { ensureTopic } from '../shared/kafka';
import { log } from '../shared/log';
import { OrderConsumer } from './order-consumer';

// The flash-sale order writer process: Kafka → Postgres, in batches.
async function main() {
  const brokers = config.KAFKA_BROKERS;
  await ensureTopic(brokers, config.FLASH_ORDERS_TOPIC, config.FLASH_TOPIC_PARTITIONS);
  const pool = createPool(config.DATABASE_URL, config.DB_POOL_SIZE);
  const consumer = new OrderConsumer({
    pool,
    brokers,
    topic: config.FLASH_ORDERS_TOPIC,
    groupId: config.ORDER_WRITER_GROUP,
    batchSize: config.ORDER_WRITER_BATCH_SIZE,
    flushMs: config.ORDER_WRITER_FLUSH_MS,
  });
  await consumer.start();
  console.log(`[order-writer] reading ${config.FLASH_ORDERS_TOPIC} (batches of up to ${config.ORDER_WRITER_BATCH_SIZE})`);

  const report = setInterval(() => {
    if (consumer.totals.inserted > 0) log.info('[order-writer]', consumer.totals);
  }, 10_000);

  // Graceful shutdown: write what's buffered and commit, so the next writer doesn't redo it.
  const shutdown = async () => {
    clearInterval(report);
    await consumer.stop();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[order-writer] failed to start:', err);
  process.exit(1);
});
