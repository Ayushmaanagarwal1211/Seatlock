import { Router } from 'express';
import { z } from 'zod';
import type { AppDeps } from '../app';
import { parseBody, parseId, requireUser } from '../http';
import { startCheckout } from '../payments/payments.service';
import { getHold, releaseHold, toHoldResponse } from './holds.service';

const checkoutSchema = z.object({ email: z.email() });

export function holdsRouter({ pool, holdStrategy, gateway }: AppDeps): Router {
  const router = Router();

  router.get('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Hold');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;
    const hold = await getHold(pool, id, userId);
    if (!hold) {
      res.status(404).json({ error: 'Hold not found' });
      return;
    }
    res.json({
      ...toHoldResponse(hold),
      payment: hold.payment_id ? { id: hold.payment_id, status: hold.payment_status } : null,
      bookingId: hold.booking_id,
    });
  });

  // POST /v1/holds/:id/checkout  { email }: start paying. Returns the gateway's payment page.
  // Safe to retry: the hold's live payment is reused.
  router.post('/:id/checkout', async (req, res) => {
    const id = parseId(req.params.id, res, 'Hold');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;
    const body = parseBody(checkoutSchema, req, res);
    if (!body) return;

    const result = await startCheckout(pool, gateway, holdStrategy, id, userId, body.email);
    switch (result.kind) {
      case 'ok':
        res.status(201).json({
          paymentId: result.payment.id,
          gatewayPaymentId: result.payment.gateway_payment_id,
          amountPaise: result.payment.amount_paise,
          status: result.payment.status,
          payUrl: result.payUrl,
        });
        return;
      case 'not_found':
        res.status(404).json({ error: 'Hold not found' });
        return;
      case 'not_active':
        res.status(409).json({ error: `The hold is ${result.status}.` });
        return;
      case 'already_paid':
        res.status(409).json({ error: 'This hold is already paid for.' });
        return;
      case 'gateway_error':
        res.status(502).json({ error: 'The payment gateway is unavailable. Try again.' });
        return;
    }
  });

  // DELETE /v1/holds/:id: give the seats back before paying
  router.delete('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Hold');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;

    const result = await releaseHold(pool, holdStrategy, id, userId);
    switch (result.kind) {
      case 'released':
        res.json(toHoldResponse(result.hold));
        return;
      case 'not_found':
        res.status(404).json({ error: 'Hold not found' });
        return;
      case 'not_active':
        res.status(409).json({ error: `The hold is ${result.status}.` });
        return;
      case 'payment_in_progress':
        res.status(409).json({ error: 'A payment for this hold is in progress.' });
        return;
    }
  });

  return router;
}
