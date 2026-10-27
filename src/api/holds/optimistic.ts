import { Pool } from 'pg';
import { withTransaction } from '../../shared/db';
import { checkSeats, HoldAttempt, HoldStrategy, insertHold, SEAT_IS_FREE_SQL, SeatState, StrategyResult } from './hold-strategy';

class LostRace extends Error {
  constructor(readonly seatIds: number[]) {
    super('seats changed since they were read');
  }
}

/**
 * OPTIMISTIC LOCKING: read without locking, then write only if nothing changed.
 *
 * Every seat row has a version number. We read the seats (no locks), check they're free,
 * then update each one only if its version is still the one we read:
 *
 *   UPDATE seats ... WHERE id = 12 AND version = 7
 *
 * If someone held the seat in between, it's version 8 now, the update matches 0 rows, and
 * we roll back and answer 409. Losing requests fail fast instead of waiting in line for a
 * lock, and no transaction stays open while we think.
 *
 * Cost under heavy contention: many requests do the read, then lose at the write.
 */
export class OptimisticStrategy implements HoldStrategy {
  readonly name: 'optimistic' | 'redis' = 'optimistic';

  constructor(private pool: Pool) {}

  async hold(attempt: HoldAttempt): Promise<StrategyResult> {
    const { rows } = await this.pool.query<SeatState>(
      `SELECT id, event_id, price_paise, version, ${SEAT_IS_FREE_SQL} AS free
         FROM seats
        WHERE id = ANY($1::bigint[])`,
      [attempt.seatIds],
    );
    const check = checkSeats(attempt, rows);
    if (check.kind !== 'ok') return check;

    const versionById = new Map(rows.map((r) => [Number(r.id), r.version]));
    const versions = attempt.seatIds.map((id) => versionById.get(id)!);

    try {
      return await withTransaction(this.pool, async (client) => {
        const hold = await insertHold(client, attempt, check.amountPaise);
        if (!hold) return { kind: 'key_taken' } as const;

        // One statement updates every seat, each guarded by the version we read.
        const { rows: won } = await client.query<{ id: string }>(
          `UPDATE seats AS s
              SET status = 'held', hold_id = $1, held_until = $2, version = s.version + 1
             FROM unnest($3::bigint[], $4::int[]) AS v(id, version)
            WHERE s.id = v.id AND s.version = v.version
            RETURNING s.id`,
          [hold.id, hold.expires_at, attempt.seatIds, versions],
        );
        if (won.length < attempt.seatIds.length) {
          const wonIds = new Set(won.map((r) => Number(r.id)));
          throw new LostRace(attempt.seatIds.filter((id) => !wonIds.has(id))); // rolls back the hold too
        }
        return { kind: 'held', hold } as const;
      });
    } catch (err) {
      if (err instanceof LostRace) return { kind: 'unavailable', seatIds: err.seatIds };
      throw err;
    }
  }

  async afterRelease(): Promise<void> {}
  async afterBooking(): Promise<void> {}
}
