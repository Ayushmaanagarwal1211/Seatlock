import express from 'express';
import { listen } from './helpers';

export interface RelayRequest {
  idempotencyKey: string | undefined;
  body: { to: string; subject: string; body: string; priority: string };
}

/**
 * Stands in for Relay's POST /v1/emails: answers 202 like Relay does, and records every
 * request so tests can check what was sent and how often. `failNext` makes the next
 * requests fail, to test retries.
 */
export async function startFakeRelay() {
  const requests: RelayRequest[] = [];
  let failures: { count: number; status: number } = { count: 0, status: 500 };

  const app = express();
  app.use(express.json());
  app.post('/v1/emails', (req, res) => {
    if (failures.count > 0) {
      failures.count--;
      res.status(failures.status).json({ error: 'simulated failure' });
      return;
    }
    requests.push({ idempotencyKey: req.get('Idempotency-Key'), body: req.body });
    res.status(202).json({ id: 'email-id', status: 'queued' });
  });
  const server = await listen(app);

  return {
    url: server.url,
    requests,
    failNext(count: number, status = 500) {
      failures = { count, status };
    },
    close: server.close,
  };
}
