import Redis from 'ioredis';
import { Pool } from 'pg';
import { flashKeys, orderIdFor, OrderPublisher } from '../api/flash/flash-strategies';

export interface FlashReconcileResult {
  reservedInRedis: number;
  ordersInPostgres: number;
  republished: number; // reserved in Redis but no order in Postgres
  unknownToRedis: number; // order in Postgres for a user Redis doesn't know (Redis lost data)
}

/**
 * Compares who Redis said yes to with the orders in Postgres, and re-publishes the missing
 * orders. A buyer can be missing if an API process died after Redis reserved the item but
 * before Kafka acknowledged the order.
 *
 * Re-publishing is always safe: the order id is derived from (sale, user), so an order
 * that was only late (still in Kafka) lands on the same row and is skipped as a duplicate.
 * Run it after the sale, once the order writer has caught up.
 */
export async function reconcileFlashSale(
  pool: Pool,
  redis: Redis,
  publisher: OrderPublisher,
  saleId: string,
): Promise<FlashReconcileResult> {
  const buyers = new Set(await redis.smembers(flashKeys(saleId).buyers));
  const { rows } = await pool.query<{ user_id: string }>('SELECT user_id FROM flash_orders WHERE sale_id = $1', [saleId]);
  const ordered = new Set(rows.map((r) => r.user_id));

  const missing = [...buyers].filter((user) => !ordered.has(user));
  await Promise.all(
    missing.map((userId) =>
      publisher.send({ orderId: orderIdFor(saleId, userId), saleId, userId, reservedAt: new Date().toISOString() }),
    ),
  );
  return {
    reservedInRedis: buyers.size,
    ordersInPostgres: ordered.size,
    republished: missing.length,
    unknownToRedis: [...ordered].filter((user) => !buyers.has(user)).length,
  };
}
