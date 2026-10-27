import cluster from 'node:cluster';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { GatewayClient } from '../shared/gateway-client';
import { createRedis, waitForRedis } from '../shared/redis';
import { createApp } from './app';
import { SeatMapCache } from './events/seat-map';
import { createHoldStrategy } from './holds/strategies';

/**
 * Node runs JavaScript on ONE CPU core per process. With API_PROCESSES > 1 we use Node's
 * cluster module: a primary process forks N API processes that all listen on the same
 * port, and the OS spreads connections between them. They share nothing in memory, which
 * is fine because all state lives in Postgres (and Redis).
 */
if (config.API_PROCESSES > 1 && cluster.isPrimary) {
  runPrimary(config.API_PROCESSES);
} else {
  void runServer();
}

function runPrimary(processes: number) {
  console.log(`API: starting ${processes} processes`);
  for (let i = 0; i < processes; i++) cluster.fork();

  let shuttingDown = false;
  // If an API process crashes, start a new one, like Kubernetes would restart a pod
  cluster.on('exit', (worker, code, signal) => {
    if (shuttingDown) {
      if (Object.keys(cluster.workers ?? {}).length === 0) process.exit(0);
      return;
    }
    console.error(`API process ${worker.process.pid} died (${signal ?? code}), starting a new one`);
    cluster.fork();
  });

  const shutdown = (signal: string) => {
    console.log(`${signal} received, stopping API processes...`);
    shuttingDown = true;
    for (const worker of Object.values(cluster.workers ?? {})) worker?.process.kill('SIGTERM');
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

async function runServer() {
  const pool = createPool(config.DATABASE_URL, config.DB_POOL_SIZE);
  const redis = createRedis(config.REDIS_URL);
  await waitForRedis(redis);

  const app = createApp({
    pool,
    holdStrategy: createHoldStrategy(config.HOLD_STRATEGY, pool, redis),
    seatMap: new SeatMapCache(pool, config.SEAT_MAP_CACHE_MS),
    gateway: new GatewayClient(config.GATEWAY_URL, config.GATEWAY_API_KEY),
    webhookSecret: config.GATEWAY_WEBHOOK_SECRET,
    webhookToleranceSeconds: config.WEBHOOK_TOLERANCE_SECONDS,
  });
  const server = app.listen(config.PORT, () => {
    if (cluster.isPrimary) {
      console.log(`API listening on http://localhost:${config.PORT} (hold strategy: ${config.HOLD_STRATEGY})`);
    }
  });

  // Graceful shutdown: stop accepting requests, let in-flight ones finish, then close connections.
  let closing = false;
  function shutdown(signal: string) {
    if (closing) return;
    closing = true;
    if (cluster.isPrimary) console.log(`${signal} received, shutting down...`);
    server.close(() => {
      redis.disconnect();
      pool.end().then(() => process.exit(0));
    });
  }
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}
