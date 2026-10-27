import Redis from 'ioredis';
import { Pool } from 'pg';
import { log } from '../../shared/log';
import { HoldAttempt, HoldStrategy, StrategyResult } from './hold-strategy';
import { OptimisticStrategy } from './optimistic';

/**
 * Checks every seat key and, only if ALL are free, sets them all with a TTL.
 * Redis runs a Lua script as one atomic step, so two requests can't both see "free".
 *
 * ARGV[1] is the hold id. A key that already holds THIS hold id isn't taken: it was set by
 * an earlier attempt of the same request (a client retry while the first try is still
 * running). Postgres then sorts out which attempt creates the hold.
 *
 * Returns { number of keys that were already ours, 1-based positions of taken keys }.
 */
const TRY_HOLD_LUA = `
local mine = 0
local taken = {}
for i, key in ipairs(KEYS) do
  local owner = redis.call('GET', key)
  if owner == ARGV[1] then
    mine = mine + 1
  elseif owner then
    taken[#taken + 1] = i
  end
end
if #taken > 0 then
  return { mine, taken }
end
for _, key in ipairs(KEYS) do
  redis.call('SET', key, ARGV[1], 'PX', ARGV[2])
end
return { mine, {} }
`;

// Deletes only the keys that still belong to this hold (compare-and-delete), so a late
// release can't free a seat that someone else has since taken.
const RELEASE_LUA = `
local released = 0
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) == ARGV[1] then
    redis.call('DEL', key)
    released = released + 1
  end
end
return released
`;

type RedisWithGate = Redis & {
  seatTryHold(...args: (string | number)[]): Promise<[number, number[]]>;
  seatRelease(...args: (string | number)[]): Promise<number>;
};

/**
 * REDIS GATE, then Postgres.
 *
 * When 10,000 people want the same 100 front-row seats, 9,900 of them will lose. The
 * other strategies make every loser do database work. Here, an atomic Lua script in Redis
 * turns the losers away in about a millisecond, and only the winners reach Postgres.
 *
 * Postgres still has the final say (the optimistic strategy runs for the winners), because
 * a Redis lock alone is not a correctness guarantee: Redis can lose keys in a restart or
 * failover, and a process that paused (garbage collection, a slow network) can act after
 * its key expired. Whatever Redis says, Postgres's version check and UNIQUE constraints
 * decide.
 *
 * If Redis is down, the gate lets everyone through (fail open) and Postgres alone protects
 * the seats. That's safe here because Redis was never the source of truth.
 *
 * Keys look like seat:{eventId}:42. The {eventId} part is a Redis Cluster "hash tag": all
 * seats of one event live on the same node, so one script can touch several of them.
 */
export class RedisGateStrategy implements HoldStrategy {
  readonly name = 'redis' as const;
  private redis: RedisWithGate;
  private postgres: OptimisticStrategy;

  constructor(pool: Pool, redis: Redis) {
    redis.defineCommand('seatTryHold', { lua: TRY_HOLD_LUA });
    redis.defineCommand('seatRelease', { lua: RELEASE_LUA });
    this.redis = redis as RedisWithGate;
    this.postgres = new OptimisticStrategy(pool);
  }

  static seatKey(eventId: string, seatId: number): string {
    return `seat:{${eventId}}:${seatId}`;
  }

  async hold(attempt: HoldAttempt): Promise<StrategyResult> {
    const keys = attempt.seatIds.map((id) => RedisGateStrategy.seatKey(attempt.eventId, id));

    // Whether this attempt set the keys itself, and so must give them back if Postgres says no.
    // A retry that found its own keys already set leaves them to the attempt that set them.
    let gated = false;
    try {
      const [mine, taken] = await this.redis.seatTryHold(keys.length, ...keys, attempt.holdId, attempt.holdSeconds * 1000);
      if (taken.length > 0) return { kind: 'unavailable', seatIds: taken.map((i) => attempt.seatIds[i - 1]) };
      gated = mine === 0;
    } catch (err) {
      log.warn(`[holds] Redis gate unavailable, using Postgres only: ${(err as Error).message}`);
    }

    let result: StrategyResult;
    try {
      result = await this.postgres.hold(attempt);
    } catch (err) {
      if (gated) await this.release(attempt.eventId, attempt.seatIds, attempt.holdId);
      throw err;
    }
    // Redis said yes but Postgres said no (Redis had lost a key, or the seat ids were
    // invalid): give the keys back so the seats aren't blocked until the TTL runs out.
    if (result.kind !== 'held' && gated) await this.release(attempt.eventId, attempt.seatIds, attempt.holdId);
    return result;
  }

  async afterRelease(eventId: string, seatIds: number[], holdId: string): Promise<void> {
    await this.release(eventId, seatIds, holdId);
  }

  // Booked seats are taken for good: keep turning requests for them away in Redis.
  async afterBooking(eventId: string, seatIds: number[]): Promise<void> {
    try {
      const pipeline = this.redis.pipeline();
      for (const id of seatIds) pipeline.set(RedisGateStrategy.seatKey(eventId, id), 'booked');
      await pipeline.exec();
    } catch (err) {
      log.warn(`[holds] couldn't mark booked seats in Redis: ${(err as Error).message}`);
    }
  }

  private async release(eventId: string, seatIds: number[], holdId: string): Promise<void> {
    const keys = seatIds.map((id) => RedisGateStrategy.seatKey(eventId, id));
    try {
      await this.redis.seatRelease(keys.length, ...keys, holdId);
    } catch (err) {
      // The keys expire with the hold anyway; until then, those seats look taken in Redis.
      log.warn(`[holds] couldn't release seats in Redis: ${(err as Error).message}`);
    }
  }
}
