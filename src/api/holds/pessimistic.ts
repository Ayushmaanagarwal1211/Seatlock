import { Pool } from 'pg';
import { withTransaction } from '../../shared/db';
import { checkSeats, HoldAttempt, HoldStrategy, insertHold, SEAT_IS_FREE_SQL, SeatState, StrategyResult } from './hold-strategy';

/**
 * PESSIMISTIC LOCKING: lock the seats first, then check them.
 *
 * SELECT ... FOR UPDATE locks the seat rows until the transaction ends. A second request
 * for the same seat WAITS at that line until the first commits, then reads the new state
 * ("held") and gets a 409. Nobody can check-then-act at the same time as anyone else.
 *
 * ORDER BY id: every request locks its seats in the same order. Without it, Alice could
 * lock seat 1 and wait for seat 2 while Bob locks seat 2 and waits for seat 1: a deadlock.
 * With a fixed order, whoever gets seat 1 first also gets seat 2 first.
 *
 * Cost under heavy contention: waiting requests hold a database connection while they wait.
 */
export class PessimisticStrategy implements HoldStrategy {
  readonly name = 'pessimistic' as const;

  constructor(private pool: Pool) {}

  hold(attempt: HoldAttempt): Promise<StrategyResult> {
    return withTransaction(this.pool, async (client) => {
      const { rows } = await client.query<SeatState>(
        `SELECT id, event_id, price_paise, version, ${SEAT_IS_FREE_SQL} AS free
           FROM seats
          WHERE id = ANY($1::bigint[])
          ORDER BY id
          FOR UPDATE`,
        [attempt.seatIds],
      );
      const check = checkSeats(attempt, rows);
      if (check.kind !== 'ok') return check; // nothing written; COMMIT just releases the locks

      const hold = await insertHold(client, attempt, check.amountPaise);
      if (!hold) return { kind: 'key_taken' };

      await client.query(
        `UPDATE seats SET status = 'held', hold_id = $2, held_until = $3, version = version + 1
          WHERE id = ANY($1::bigint[])`,
        [attempt.seatIds, hold.id, hold.expires_at],
      );
      return { kind: 'held', hold };
    });
  }

  async afterRelease(): Promise<void> {}
  async afterBooking(): Promise<void> {}
}
