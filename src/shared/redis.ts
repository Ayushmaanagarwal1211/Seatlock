import Redis from 'ioredis';

// A Redis connection that fails fast instead of queueing commands while disconnected.
// Redis is only ever a speed-up in Seatlock (Postgres is the source of truth), so callers
// catch errors and carry on without it rather than hang.
export function createRedis(url: string): Redis {
  const redis = new Redis(url, { enableOfflineQueue: false, maxRetriesPerRequest: 1, commandTimeout: 1000 });
  redis.on('error', () => {
    // Reported by the commands that fail; ioredis keeps reconnecting.
  });
  return redis;
}

// Resolves once connected, or after timeoutMs either way.
export function waitForRedis(redis: Redis, timeoutMs = 5000): Promise<void> {
  if (redis.status === 'ready') return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, timeoutMs);
    redis.once('ready', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}
