import { Pool } from 'pg';

/**
 * The seat map: every seat of an event and whether it's available.
 *
 * When 50,000 people wait for a sale, they all refresh this page. Each API process
 * keeps the finished JSON for `ttlMs` (1s by default), so the database sees at most one
 * seat map query per second per process, however many users ask.
 *
 * SINGLE-FLIGHT: when the cached copy expires, 500 requests may arrive in the same
 * millisecond. Without care, all 500 would query Postgres at once (a "cache stampede").
 * Instead, the first request starts the query and the other 499 wait for that same promise.
 *
 * The map can be up to a second old. That's fine: holding a seat is checked again,
 * properly, by the hold strategies. The map is a hint; the hold is the truth.
 */
export class SeatMapCache {
  private cached = new Map<string, { body: string; expiresAt: number }>();
  private inFlight = new Map<string, Promise<string | null>>();

  constructor(
    private pool: Pool,
    private ttlMs: number,
  ) {}

  // Returns the seat map as a JSON string (serialized once, not once per request), or null
  // if the event doesn't exist.
  async get(eventId: string): Promise<string | null> {
    const hit = this.cached.get(eventId);
    if (hit && hit.expiresAt > Date.now()) return hit.body;

    const pending = this.inFlight.get(eventId);
    if (pending) return pending;

    const load = this.load(eventId).finally(() => this.inFlight.delete(eventId));
    this.inFlight.set(eventId, load);
    return load;
  }

  private async load(eventId: string): Promise<string | null> {
    const event = await this.pool.query('SELECT 1 FROM events WHERE id = $1', [eventId]);
    if (event.rows.length === 0) return null;

    // A seat held by an expired hold counts as available (holds expire lazily).
    const { rows } = await this.pool.query<{
      id: string;
      label: string;
      section: string;
      price_paise: number;
      available: boolean;
    }>(
      `SELECT id, label, section, price_paise,
              (status = 'available' OR (status = 'held' AND held_until <= now())) AS available
         FROM seats
        WHERE event_id = $1
        ORDER BY id`,
      [eventId],
    );
    const body = JSON.stringify({
      eventId,
      generatedAt: new Date().toISOString(),
      seats: rows.map((s) => ({
        id: Number(s.id),
        label: s.label,
        section: s.section,
        pricePaise: s.price_paise,
        available: s.available,
      })),
    });
    if (this.ttlMs > 0) this.cached.set(eventId, { body, expiresAt: Date.now() + this.ttlMs });
    return body;
  }
}
