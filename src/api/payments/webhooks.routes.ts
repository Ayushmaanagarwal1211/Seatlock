import express, { Router } from 'express';
import { z } from 'zod';
import { GatewayEvent } from '../../shared/gateway-types';
import { SIGNATURE_HEADER, verifySignature } from '../../shared/webhook-signature';
import type { AppDeps } from '../app';
import { handleGatewayEvent } from './payments.service';

const eventSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  createdAt: z.string(),
  data: z.object({ paymentId: z.string().min(1), reference: z.uuid(), amountPaise: z.number().int() }),
});

export function webhooksRouter({ pool, holdStrategy, webhookSecret, webhookToleranceSeconds }: AppDeps): Router {
  const router = Router();

  // POST /v1/webhooks/gateway: payment outcomes from the gateway.
  // express.raw, not express.json: the signature covers the exact bytes that were sent,
  // and re-serializing parsed JSON could change them (key order, spacing).
  router.post('/gateway', express.raw({ type: 'application/json', limit: '100kb' }), async (req, res) => {
    const rawBody = Buffer.isBuffer(req.body) ? req.body.toString('utf8') : '';
    const check = verifySignature(webhookSecret, rawBody, req.get(SIGNATURE_HEADER), webhookToleranceSeconds);
    if (!check.ok) {
      res.status(400).json({ error: `invalid signature (${check.reason})` });
      return;
    }

    let event;
    try {
      event = eventSchema.parse(JSON.parse(rawBody));
    } catch {
      res.status(400).json({ error: 'invalid event' });
      return;
    }

    // Any error here becomes a 500, and the gateway delivers the event again later.
    const result = await handleGatewayEvent(pool, holdStrategy, event as GatewayEvent);
    res.json({ received: true, ...result });
  });

  return router;
}
