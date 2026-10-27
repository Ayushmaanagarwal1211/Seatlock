import express, { ErrorRequestHandler } from 'express';
import { Pool } from 'pg';
import { SeatMapCache } from './events/seat-map';
import { eventsRouter } from './events/events.routes';
import { HoldStrategy } from './holds/hold-strategy';
import { holdsRouter } from './holds/holds.routes';

// Everything the routes need, passed in so tests can build the app around a test database.
export interface AppDeps {
  pool: Pool;
  holdStrategy: HoldStrategy;
  seatMap: SeatMapCache;
}

export function createApp(deps: AppDeps) {
  const app = express();
  app.use(express.json({ limit: '100kb' }));

  app.get('/health', async (_req, res) => {
    await deps.pool.query('SELECT 1');
    res.json({ status: 'ok', holdStrategy: deps.holdStrategy.name });
  });

  app.use('/v1/events', eventsRouter(deps));
  app.use('/v1/holds', holdsRouter(deps));

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
