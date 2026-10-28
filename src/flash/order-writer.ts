import { Pool } from 'pg';
import { withTransaction } from '../shared/db';
import { FlashOrderMessage } from '../shared/kafka';
import { log } from '../shared/log';
import { CHECK_VIOLATION, pgErrorCode } from '../shared/pg-errors';

export interface WriteResult {
  inserted: number; // new orders
  duplicates: number; // already written (a redelivered Kafka message)
  rejected: number; // refused because the sale was sold out in Postgres (oversell prevented)
}

/**
 * Writes a batch of orders to Postgres. Called with each batch read from Kafka.
 *
 * Per sale, in one transaction:
 *   - insert every order with ONE statement; ON CONFLICT DO NOTHING skips orders already
 *     written, so a batch delivered twice (the writer crashed before committing its Kafka
 *     offsets) changes nothing the second time;
 *   - add the number of NEW orders to the sale's sold counter with ONE update.
 *
 * That's one update of the flash_sales row per batch of up to 500 orders, instead of one
 * per order: the hot row isn't hot anymore.
 *
 * If the batch would push sold above stock, the CHECK constraint refuses it. That should
 * never happen (Redis counts the stock), unless Redis lost its data and let too many
 * through. Then we fall back to one order at a time, keep the ones that fit, and reject the
 * rest. Postgres, not Redis, has the final say on how many were sold.
 */
export async function writeOrders(pool: Pool, messages: FlashOrderMessage[]): Promise<WriteResult> {
  const result: WriteResult = { inserted: 0, duplicates: 0, rejected: 0 };
  const bySale = new Map<string, FlashOrderMessage[]>();
  for (const m of messages) bySale.set(m.saleId, [...(bySale.get(m.saleId) ?? []), m]);

  for (const [saleId, orders] of bySale) {
    try {
      const inserted = await withTransaction(pool, async (client) => {
        const { rowCount } = await client.query(
          `INSERT INTO flash_orders (id, sale_id, user_id, price_paise, reserved_at)
           SELECT o.id, s.id, o.user_id, s.price_paise, o.reserved_at
             FROM unnest($2::uuid[], $3::text[], $4::timestamptz[]) AS o(id, user_id, reserved_at)
             JOIN flash_sales s ON s.id = $1
           ON CONFLICT DO NOTHING`,
          [saleId, orders.map((o) => o.orderId), orders.map((o) => o.userId), orders.map((o) => o.reservedAt)],
        );
        const count = rowCount ?? 0;
        if (count > 0) await client.query('UPDATE flash_sales SET sold = sold + $2 WHERE id = $1', [saleId, count]);
        return count;
      });
      result.inserted += inserted;
      result.duplicates += orders.length - inserted;
    } catch (err) {
      if (pgErrorCode(err) !== CHECK_VIOLATION) throw err;
      log.error(`[order-writer] sale ${saleId}: batch would oversell, writing orders one by one`);
      const oneByOne = await writeOneByOne(pool, saleId, orders);
      result.inserted += oneByOne.inserted;
      result.duplicates += oneByOne.duplicates;
      result.rejected += oneByOne.rejected;
    }
  }
  return result;
}

async function writeOneByOne(pool: Pool, saleId: string, orders: FlashOrderMessage[]): Promise<WriteResult> {
  const result: WriteResult = { inserted: 0, duplicates: 0, rejected: 0 };
  for (const order of orders) {
    const outcome = await withTransaction(pool, async (client) => {
      const sold = await client.query('UPDATE flash_sales SET sold = sold + 1 WHERE id = $1 AND sold < stock', [saleId]);
      if (sold.rowCount === 0) {
        const existing = await client.query('SELECT 1 FROM flash_orders WHERE id = $1', [order.orderId]);
        return existing.rows.length ? 'duplicate' : 'rejected';
      }
      const inserted = await client.query(
        `INSERT INTO flash_orders (id, sale_id, user_id, price_paise, reserved_at)
         SELECT $1, id, $3, price_paise, $4 FROM flash_sales WHERE id = $2
         ON CONFLICT DO NOTHING`,
        [order.orderId, saleId, order.userId, order.reservedAt],
      );
      if (inserted.rowCount === 0) {
        await client.query('UPDATE flash_sales SET sold = sold - 1 WHERE id = $1', [saleId]);
        return 'duplicate';
      }
      return 'inserted';
    });
    result[outcome === 'inserted' ? 'inserted' : outcome === 'duplicate' ? 'duplicates' : 'rejected']++;
    if (outcome === 'rejected') log.error(`[order-writer] order ${order.orderId} rejected: sale ${saleId} is sold out`);
  }
  return result;
}
