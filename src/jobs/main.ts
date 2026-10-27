import { createHoldStrategy } from '../api/holds/strategies';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { GatewayClient } from '../shared/gateway-client';
import { log } from '../shared/log';
import { createRedis, waitForRedis } from '../shared/redis';
import { reconcileOnce } from './reconciler';

/**
 * Background jobs, in one process:
 *   - reconciler: settles payments whose webhook never arrived (see reconciler.ts)
 *
 * Each job is a loop that sleeps between runs. Running several copies of this process is
 * safe: every job only changes rows whose current status allows it.
 */
async function main() {
  const pool = createPool(config.DATABASE_URL, config.DB_POOL_SIZE);
  const redis = createRedis(config.REDIS_URL);
  await waitForRedis(redis);
  const gateway = new GatewayClient(config.GATEWAY_URL, config.GATEWAY_API_KEY);
  const strategy = createHoldStrategy(config.HOLD_STRATEGY, pool, redis);

  let stopping = false;
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  const loops = [
    (async () => {
      while (!stopping) {
        try {
          const result = await reconcileOnce(pool, gateway, strategy, config.RECONCILE_AFTER_MS);
          if (result.checked + result.abandoned > 0) log.info('[reconciler]', result);
        } catch (err) {
          log.error('[reconciler] run failed, will retry:', err);
        }
        await sleep(config.RECONCILE_INTERVAL_MS);
      }
    })(),
  ];
  console.log('[jobs] reconciler running');

  const shutdown = async () => {
    stopping = true;
    await Promise.race([Promise.all(loops), sleep(5000)]);
    redis.disconnect();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[jobs] failed to start:', err);
  process.exit(1);
});
