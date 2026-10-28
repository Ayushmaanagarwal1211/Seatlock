import { Pool } from 'pg';
import { SeatMapCache } from '../../src/api/events/seat-map';

// A fake pool that counts queries and answers slowly, like a busy database.
function fakePool() {
  let queries = 0;
  const pool = {
    async query(sql: string) {
      queries++;
      await new Promise((r) => setTimeout(r, 20));
      return sql.includes('FROM events') ? { rows: [{}] } : { rows: [{ id: '1' }, { id: '2' }] };
    },
  };
  return { pool: pool as unknown as Pool, queries: () => queries };
}

describe('SeatMapCache', () => {
  it('makes 500 simultaneous requests share one database load (single-flight)', async () => {
    const { pool, queries } = fakePool();
    const cache = new SeatMapCache(pool, 1000);

    const bodies = await Promise.all(Array.from({ length: 500 }, () => cache.getAvailability('event-1')));

    expect(new Set(bodies).size).toBe(1);
    expect(queries()).toBe(2); // one "event exists?" + one "available seats", not 1,000
  });

  it('serves from the cache until it expires', async () => {
    const { pool, queries } = fakePool();
    const cache = new SeatMapCache(pool, 50);

    await cache.getAvailability('event-1');
    await cache.getAvailability('event-1');
    expect(queries()).toBe(2);

    await new Promise((r) => setTimeout(r, 60));
    await cache.getAvailability('event-1');
    expect(queries()).toBe(4);
  });
});
