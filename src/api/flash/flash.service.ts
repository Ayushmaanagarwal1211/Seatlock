import Redis from 'ioredis';
import { Pool } from 'pg';
import { log } from '../../shared/log';
import { flashKeys, orderIdFor } from './flash-strategies';

export interface FlashSaleRow {
  id: string;
  name: string;
  price_paise: number;
  stock: number;
  sold: number;
  created_at: Date;
}

export async function createFlashSale(
  pool: Pool,
  redis: Redis,
  input: { name: string; pricePaise: number; stock: number },
): Promise<FlashSaleRow> {
  const { rows } = await pool.query<FlashSaleRow>(
    'INSERT INTO flash_sales (name, price_paise, stock) VALUES ($1, $2, $3) RETURNING *',
    [input.name, input.pricePaise, input.stock],
  );
  const sale = rows[0];
  await loadSaleIntoRedis(redis, sale);
  return sale;
}

// Puts the sale's stock in Redis, where the buy script reads it. NX: never overwrite a
// counter that's already counting down.
export async function loadSaleIntoRedis(redis: Redis, sale: FlashSaleRow): Promise<void> {
  try {
    await redis.set(flashKeys(sale.id).stock, sale.stock - sale.sold, 'NX');
  } catch (err) {
    log.warn(`[flash] couldn't load sale ${sale.id} into Redis: ${(err as Error).message}`);
  }
}

export async function getFlashSale(pool: Pool, redis: Redis, saleId: string) {
  const { rows } = await pool.query<FlashSaleRow>('SELECT * FROM flash_sales WHERE id = $1', [saleId]);
  const sale = rows[0];
  if (!sale) return null;
  // Redis knows first (it decides); Postgres catches up as the order writer drains Kafka.
  const inRedis = await redis.get(flashKeys(saleId).stock).catch(() => null);
  return {
    id: sale.id,
    name: sale.name,
    pricePaise: sale.price_paise,
    stock: sale.stock,
    sold: sale.sold,
    remaining: inRedis === null ? sale.stock - sale.sold : Math.max(0, Number(inRedis)),
  };
}

export type MyOrder =
  | { status: 'confirmed'; orderId: string; createdAt: Date }
  | { status: 'processing'; orderId: string } // reserved in Redis, not written to Postgres yet
  | null;

export async function getMyOrder(pool: Pool, redis: Redis, saleId: string, userId: string): Promise<MyOrder> {
  const orderId = orderIdFor(saleId, userId);
  const { rows } = await pool.query<{ created_at: Date }>('SELECT created_at FROM flash_orders WHERE id = $1', [orderId]);
  if (rows[0]) return { status: 'confirmed', orderId, createdAt: rows[0].created_at };
  const reserved = await redis.sismember(flashKeys(saleId).buyers, userId).catch(() => 0);
  return reserved ? { status: 'processing', orderId } : null;
}
