import { createHoldStrategy } from '../api/holds/strategies';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { GatewayClient } from '../shared/gateway-client';
import { log } from '../shared/log';
import { createRedis, waitForRedis } from '../shared/redis';
import { dispatchOnce } from './dispatcher';
import { LogEmailSender, RelayEmailSender } from './email-sender';
import { reconcileOnce } from './reconciler';

/**
 * Background jobs, in one process:
 *   - dispatcher: delivers outbox rows: confirmation emails through Relay, refunds through
 *     the gateway (see dispatcher.ts)
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
  const email = config.RELAY_URL ? new RelayEmailSender(config.RELAY_URL) : new LogEmailSender();

  let stopping = false;
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  const loops = [
    (async () => {
      while (!stopping) {
        let picked = 0;
        try {
          const result = await dispatchOnce({
            pool,
            email,
            gateway,
            batchSize: config.DISPATCH_BATCH_SIZE,
            concurrency: config.DISPATCH_CONCURRENCY,
          });
          picked = result.picked;
          if (result.retrying + result.parked > 0) log.warn('[dispatcher]', result);
        } catch (err) {
          log.error('[dispatcher] batch failed, will retry:', err);
        }
        // A full batch means more rows are probably waiting: go again straight away.
        if (picked < config.DISPATCH_BATCH_SIZE) await sleep(config.DISPATCH_INTERVAL_MS);
      }
    })(),
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
  console.log(`[jobs] dispatcher (emails ${config.RELAY_URL ? `via Relay at ${config.RELAY_URL}` : 'logged only'}) and reconciler running`);

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
