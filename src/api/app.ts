import express, { ErrorRequestHandler } from 'express';
import Redis from 'ioredis';
import { Pool } from 'pg';
import { GatewayClient } from '../shared/gateway-client';
import { bookingsRouter } from './bookings.routes';
import { SeatMapCache } from './events/seat-map';
import { eventsRouter } from './events/events.routes';
import { FlashStrategy } from './flash/flash-strategies';
import { flashRouter } from './flash/flash.routes';
import { HoldStrategy } from './holds/hold-strategy';
import { holdsRouter } from './holds/holds.routes';
import { webhooksRouter } from './payments/webhooks.routes';

// Everything the routes need, passed in so tests can build the app around a test database.
export interface AppDeps {
  pool: Pool;
  redis: Redis;
  holdStrategy: HoldStrategy;
  flashStrategy: FlashStrategy;
  seatMap: SeatMapCache;
  gateway: GatewayClient;
  webhookSecret: string;
  webhookToleranceSeconds: number;
}

export function createApp(deps: AppDeps) {
  const app = express();
  // Webhooks first: they need the raw body to check the signature, before JSON parsing.
  app.use('/v1/webhooks', webhooksRouter(deps));
  app.use(express.json({ limit: '100kb' }));

  app.get('/health', async (_req, res) => {
    await deps.pool.query('SELECT 1');
    res.json({ status: 'ok', holdStrategy: deps.holdStrategy.name, flashStrategy: deps.flashStrategy.name });
  });

  app.use('/v1/events', eventsRouter(deps));
  app.use('/v1/holds', holdsRouter(deps));
  app.use('/v1/bookings', bookingsRouter(deps));
  app.use('/v1/flash-sales', flashRouter(deps));

  // Express 5 sends errors thrown in async handlers here automatically.
  const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
    // Body-parser errors (bad JSON, body too large) carry a 4xx status.
    if (typeof err.status === 'number' && err.status < 500) {
      res.status(err.status).json({ error: err.message });
      return;
    }
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  };
  app.use(errorHandler);

  return app;
}
