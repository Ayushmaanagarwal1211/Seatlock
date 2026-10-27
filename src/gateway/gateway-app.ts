import { randomBytes } from 'node:crypto';
import express, { Request, Response } from 'express';
import { z } from 'zod';
import { GatewayEvent, GatewayEventType, GatewayPayment } from '../shared/gateway-types';
import { WebhookChaos, WebhookSender } from './webhook-sender';

export interface GatewayOptions {
  apiKey: string;
  webhookUrl: string;
  webhookSecret: string;
  chaos: WebhookChaos;
  // A payment nobody completes fails after this long (real payment pages time out too)
  paymentExpiryMs: number;
}

const createPaymentSchema = z.object({ amountPaise: z.number().int().positive(), reference: z.string().min(1) });
const completeSchema = z.object({ outcome: z.enum(['succeeded', 'failed']) });
const refundSchema = z.object({ paymentId: z.string().min(1) });

const newId = (prefix: string) => `${prefix}_${randomBytes(12).toString('hex')}`;

/**
 * A fake payment gateway (like Razorpay or Stripe), for tests and load tests.
 *
 *   POST /v1/payments               merchant creates a payment (idempotent on Idempotency-Key)
 *   POST /v1/payments/:id/complete  the user pays, or their card fails (stands in for the
 *                                   gateway's payment page)
 *   GET  /v1/payments/:id           merchant checks a payment (used to reconcile)
 *   POST /v1/refunds                merchant refunds a payment (idempotent)
 *   GET  /v1/stats                  money captured and refunded, webhook counters
 *
 * Every outcome is reported to the merchant by a signed webhook, delivered at least once.
 * State lives in memory: it's a test double, not a bank.
 */
export function createGateway(options: GatewayOptions) {
  const payments = new Map<string, GatewayPayment>();
  const paymentKeys = new Map<string, string>(); // idempotency key -> payment id
  const refundKeys = new Map<string, string>(); // idempotency key -> payment id
  const expiryTimers = new Map<string, NodeJS.Timeout>();
  const sender = new WebhookSender(options.webhookUrl, options.webhookSecret, options.chaos);
  const totals = { capturedPaise: 0, refundedPaise: 0 };

  function emit(type: GatewayEventType, payment: GatewayPayment) {
    const event: GatewayEvent = {
      id: newId('evt'),
      type,
      createdAt: new Date().toISOString(),
      data: { paymentId: payment.id, reference: payment.reference, amountPaise: payment.amountPaise },
    };
    sender.send(event);
  }

  function finish(payment: GatewayPayment, outcome: 'succeeded' | 'failed') {
    clearTimeout(expiryTimers.get(payment.id));
    expiryTimers.delete(payment.id);
    payment.status = outcome;
    if (outcome === 'succeeded') totals.capturedPaise += payment.amountPaise;
    emit(outcome === 'succeeded' ? 'payment.succeeded' : 'payment.failed', payment);
  }

  const authorized = (req: Request, res: Response): boolean => {
    if (req.get('Authorization') === `Bearer ${options.apiKey}`) return true;
    res.status(401).json({ error: 'invalid API key' });
    return false;
  };

  const app = express();
  app.use(express.json());

  app.post('/v1/payments', (req, res) => {
    if (!authorized(req, res)) return;
    const key = req.get('Idempotency-Key');
    const body = createPaymentSchema.safeParse(req.body);
    if (!key || !body.success) {
      res.status(400).json({ error: 'Idempotency-Key header and { amountPaise, reference } are required' });
      return;
    }
    const existingId = paymentKeys.get(key);
    if (existingId) {
      res.status(201).json(payments.get(existingId));
      return;
    }
    const payment: GatewayPayment = {
      id: newId('pay'),
      reference: body.data.reference,
      amountPaise: body.data.amountPaise,
      status: 'created',
      createdAt: new Date().toISOString(),
    };
    payments.set(payment.id, payment);
    paymentKeys.set(key, payment.id);
    expiryTimers.set(
      payment.id,
      setTimeout(() => {
        if (payment.status === 'created') finish(payment, 'failed');
      }, options.paymentExpiryMs),
    );
    res.status(201).json(payment);
  });

  app.get('/v1/payments/:id', (req, res) => {
    if (!authorized(req, res)) return;
    const payment = payments.get(req.params.id);
    if (!payment) {
      res.status(404).json({ error: 'payment not found' });
      return;
    }
    res.json(payment);
  });

  app.post('/v1/payments/:id/complete', (req, res) => {
    const payment = payments.get(req.params.id);
    const body = completeSchema.safeParse(req.body);
    if (!payment || !body.success) {
      res.status(payment ? 400 : 404).json({ error: payment ? 'outcome must be succeeded or failed' : 'payment not found' });
      return;
    }
    if (payment.status !== 'created') {
      res.status(409).json({ error: `payment is already ${payment.status}` });
      return;
    }
    finish(payment, body.data.outcome);
    res.json(payment);
  });

  app.post('/v1/refunds', (req, res) => {
    if (!authorized(req, res)) return;
    const key = req.get('Idempotency-Key');
    const body = refundSchema.safeParse(req.body);
    if (!key || !body.success) {
      res.status(400).json({ error: 'Idempotency-Key header and { paymentId } are required' });
      return;
    }
    const payment = payments.get(body.data.paymentId);
    if (!payment) {
      res.status(404).json({ error: 'payment not found' });
      return;
    }
    if (refundKeys.has(key)) {
      res.status(201).json(payment);
      return;
    }
    if (payment.status !== 'succeeded') {
      res.status(409).json({ error: `can't refund a payment that is ${payment.status}` });
      return;
    }
    refundKeys.set(key, payment.id);
    payment.status = 'refunded';
    totals.refundedPaise += payment.amountPaise;
    emit('refund.succeeded', payment);
    res.status(201).json(payment);
  });

  app.get('/v1/stats', (_req, res) => {
    const byStatus: Record<string, number> = {};
    for (const p of payments.values()) byStatus[p.status] = (byStatus[p.status] ?? 0) + 1;
    res.json({ ...totals, payments: byStatus, webhooks: { ...sender.stats, pending: sender.pending } });
  });

  return {
    app,
    close() {
      sender.close();
      for (const timer of expiryTimers.values()) clearTimeout(timer);
    },
  };
}
