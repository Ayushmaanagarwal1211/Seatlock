import Redis from 'ioredis';
import { Pool } from 'pg';
import { HoldStrategy } from './hold-strategy';
import { OptimisticStrategy } from './optimistic';
import { PessimisticStrategy } from './pessimistic';
import { RedisGateStrategy } from './redis-gate';

export type HoldStrategyName = HoldStrategy['name'];

export function createHoldStrategy(name: HoldStrategyName, pool: Pool, redis: Redis): HoldStrategy {
  switch (name) {
    case 'pessimistic':
      return new PessimisticStrategy(pool);
    case 'optimistic':
      return new OptimisticStrategy(pool);
    case 'redis':
      return new RedisGateStrategy(pool, redis);
  }
}
