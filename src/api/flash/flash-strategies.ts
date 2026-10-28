import Redis from 'ioredis';
import { Pool } from 'pg';
import { withTransaction } from '../../shared/db';
import { FlashOrderMessage } from '../../shared/kafka';
import { log } from '../../shared/log';
import { uuidFromName } from '../../shared/uuid';

export type BuyResult =
  | { kind: 'reserved'; orderId: string } // you got one
  | { kind: 'already'; orderId: string } // you already have one (a retry, or a second click)
  | { kind: 'sold_out' }
  | { kind: 'not_found' }
  | { kind: 'unavailable' }; // couldn't record the order right now; try again

export interface FlashStrategy {
  readonly name: 'postgres' | 'redis-kafka';
  buy(saleId: string, userId: string): Promise<BuyResult>;
}

class SoldOut extends Error {}

// The order id is derived from (sale, user): the same user always gets the same id, so a
// retried request, a re-sent Kafka message or the reconciler can never create a second order.
export const orderIdFor = (saleId: string, userId: string) => uuidFromName(`flash:${saleId}:${userId}`);

/**
 * BASELINE: Postgres only.
 *
 *   BEGIN;
 *   INSERT INTO flash_orders ...  ON CONFLICT DO NOTHING;            -- one per user
 *   UPDATE flash_sales SET sold = sold + 1 WHERE id = $1 AND sold < stock;  -- the HOT ROW
 *   COMMIT;
 *
 * Correct, but every buyer updates the same flash_sales row. Each UPDATE waits for the row
 * lock held by the transaction before it until that one commits, so buys happen one at a
 * time, however many CPUs or API processes there are. Waiting requests hold a database
 * connection each, and the pool runs out. This is the version to measure against.
 */
export class PostgresFlashStrategy implements FlashStrategy {
  readonly name = 'postgres' as const;

  constructor(private pool: Pool) {}

  async buy(saleId: string, userId: string): Promise<BuyResult> {
    const orderId = orderIdFor(saleId, userId);
    // Cheap check without locks: once sold out, answer from a plain read (MVCC: readers
    // don't wait for writers) instead of queueing for the row lock.
    const sale = await this.pool.query<{ sold_out: boolean }>(
      'SELECT sold >= stock AS sold_out FROM flash_sales WHERE id = $1',
      [saleId],
    );
    if (sale.rows.length === 0) return { kind: 'not_found' };
    if (sale.rows[0].sold_out) return (await this.hasOrder(orderId)) ? { kind: 'already', orderId } : { kind: 'sold_out' };

    try {
      return await withTransaction(this.pool, async (client): Promise<BuyResult> => {
        const inserted = await client.query(
          `INSERT INTO flash_orders (id, sale_id, user_id, price_paise, reserved_at)
           SELECT $1, id, $3, price_paise, now() FROM flash_sales WHERE id = $2
           ON CONFLICT DO NOTHING`,
          [orderId, saleId, userId],
        );
        if (inserted.rowCount === 0) return { kind: 'already', orderId };

        const taken = await client.query('UPDATE flash_sales SET sold = sold + 1 WHERE id = $1 AND sold < stock', [
          saleId,
        ]);
        if (taken.rowCount === 0) throw new SoldOut(); // rolls back the order insert
        return { kind: 'reserved', orderId };
      });
    } catch (err) {
      if (err instanceof SoldOut) return { kind: 'sold_out' };
      throw err;
    }
  }

  private async hasOrder(orderId: string): Promise<boolean> {
    return (await this.pool.query('SELECT 1 FROM flash_orders WHERE id = $1', [orderId])).rows.length > 0;
  }
}

/**
 * The one-per-user check and the stock decrement, as one atomic Redis step.
 * KEYS[1] = stock counter, KEYS[2] = set of buyers, ARGV[1] = user id.
 */
const BUY_LUA = `
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return 'already'
end
local stock = redis.call('GET', KEYS[1])
if not stock then
  return 'unknown'
end
if tonumber(stock) <= 0 then
  return 'sold_out'
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 'ok'
`;

// Undo a reservation whose order couldn't be published: give the item back.
const UNDO_LUA = `
if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
  redis.call('INCR', KEYS[1])
  return 1
end
return 0
`;

type RedisWithFlash = Redis & {
  flashBuy(stockKey: string, buyersKey: string, userId: string): Promise<'already' | 'unknown' | 'sold_out' | 'ok'>;
  flashUndo(stockKey: string, buyersKey: string, userId: string): Promise<number>;
};

export interface OrderPublisher {
  send(message: FlashOrderMessage): Promise<void>;
}

/**
 * REDIS DECIDES, KAFKA CARRIES, POSTGRES RECORDS.
 *
 * 1. A Lua script in Redis checks "already bought?" and "any stock left?" and takes one,
 *    atomically. Redis does tens of thousands of these a second, all in memory, and the
 *    losers (most of the crowd) are answered here without touching Postgres.
 * 2. The winner's order goes to Kafka; once Kafka acknowledges it (acks=all), the order is
 *    durable and we answer 202 "you got one".
 * 3. The order writer (src/flash/order-writer.ts) reads Kafka in batches and writes them
 *    to Postgres, updating the sold counter once per batch instead of once per order.
 *
 * If publishing fails, the Redis reservation is undone and the user is told to retry.
 * If the API process dies between steps 1 and 2, the item stays reserved in Redis with no
 * order behind it: that sells one item too FEW, never one too many. The flash reconciler
 * finds those buyers after the sale and publishes their orders.
 */
export class RedisKafkaFlashStrategy implements FlashStrategy {
  readonly name = 'redis-kafka' as const;
  private redis: RedisWithFlash;

  constructor(
    private pool: Pool,
    redis: Redis,
    private publisher: OrderPublisher,
  ) {
    redis.defineCommand('flashBuy', { numberOfKeys: 2, lua: BUY_LUA });
    redis.defineCommand('flashUndo', { numberOfKeys: 2, lua: UNDO_LUA });
    this.redis = redis as RedisWithFlash;
  }

  async buy(saleId: string, userId: string): Promise<BuyResult> {
    const orderId = orderIdFor(saleId, userId);
    const { stock, buyers } = flashKeys(saleId);

    let decision;
    try {
      decision = await this.redis.flashBuy(stock, buyers, userId);
    } catch (err) {
      log.warn(`[flash] Redis unavailable: ${(err as Error).message}`);
      return { kind: 'unavailable' };
    }
    if (decision === 'already') return { kind: 'already', orderId };
    if (decision === 'sold_out') return { kind: 'sold_out' };
    if (decision === 'unknown') {
      const exists = await this.pool.query('SELECT 1 FROM flash_sales WHERE id = $1', [saleId]);
      return exists.rows.length ? { kind: 'unavailable' } : { kind: 'not_found' };
    }

    try {
      await this.publisher.send({ orderId, saleId, userId, reservedAt: new Date().toISOString() });
    } catch (err) {
      log.warn(`[flash] couldn't publish order ${orderId}, giving the item back: ${(err as Error).message}`);
      await this.redis.flashUndo(stock, buyers, userId).catch(() => {});
      return { kind: 'unavailable' };
    }
    return { kind: 'reserved', orderId };
  }
}

// {saleId} is a Redis Cluster hash tag: both keys of a sale live on the same node, so the
// Lua script can use them together.
export function flashKeys(saleId: string) {
  return { stock: `flash:{${saleId}}:stock`, buyers: `flash:{${saleId}}:buyers` };
}
