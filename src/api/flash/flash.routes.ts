import { Router } from 'express';
import { z } from 'zod';
import type { AppDeps } from '../app';
import { parseBody, parseId, requireUser } from '../http';
import { createFlashSale, getFlashSale, getMyOrder } from './flash.service';

const createSaleSchema = z.object({
  name: z.string().trim().min(1).max(200),
  pricePaise: z.number().int().positive(),
  stock: z.number().int().positive().max(10_000_000),
});

export function flashRouter({ pool, redis, flashStrategy }: AppDeps): Router {
  const router = Router();

  // POST /v1/flash-sales: create a sale (an admin action; no auth in this project)
  router.post('/', async (req, res) => {
    const input = parseBody(createSaleSchema, req, res);
    if (!input) return;
    const sale = await createFlashSale(pool, redis, input);
    res.status(201).json({ id: sale.id, name: sale.name, pricePaise: sale.price_paise, stock: sale.stock });
  });

  router.get('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Sale');
    if (!id) return;
    const sale = await getFlashSale(pool, redis, id);
    if (!sale) {
      res.status(404).json({ error: 'Sale not found' });
      return;
    }
    res.json(sale);
  });

  // POST /v1/flash-sales/:id/orders  (header: X-User-Id): try to get one.
  // Idempotent per user: buying again returns the same order.
  router.post('/:id/orders', async (req, res) => {
    const id = parseId(req.params.id, res, 'Sale');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;

    const result = await flashStrategy.buy(id, userId);
    switch (result.kind) {
      case 'reserved':
        // 202: the item is yours; the order is being written (redis-kafka) or already is (postgres).
        res.status(202).json({ orderId: result.orderId, status: 'reserved' });
        return;
      case 'already':
        res.set('Idempotent-Replayed', 'true').status(200).json({ orderId: result.orderId, status: 'reserved' });
        return;
      case 'sold_out':
        res.status(409).json({ error: 'Sold out' });
        return;
      case 'not_found':
        res.status(404).json({ error: 'Sale not found' });
        return;
      case 'unavailable':
        res.set('Retry-After', '1').status(503).json({ error: 'Busy. Try again in a moment.' });
        return;
    }
  });

  // GET /v1/flash-sales/:id/orders/me: did I get one?
  router.get('/:id/orders/me', async (req, res) => {
    const id = parseId(req.params.id, res, 'Sale');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;
    const order = await getMyOrder(pool, redis, id, userId);
    if (!order) {
      res.status(404).json({ error: 'No order' });
      return;
    }
    res.json(order);
  });

  return router;
}
