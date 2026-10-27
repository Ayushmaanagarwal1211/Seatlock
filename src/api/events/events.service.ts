import { Pool } from 'pg';
import { withTransaction } from '../../shared/db';
import type { CreateEventInput } from '../schemas';

export interface EventRow {
  id: string;
  name: string;
  venue: string;
  starts_at: Date;
  hold_seconds: number;
  max_seats_per_hold: number;
  created_at: Date;
}

// Row labels like a cinema: A..Z, then AA, AB, ...
export function rowLabel(index: number): string {
  let label = '';
  let n = index;
  do {
    label = String.fromCharCode(65 + (n % 26)) + label;
    n = Math.floor(n / 26) - 1;
  } while (n >= 0);
  return label;
}

/**
 * Creates an event and all its seats. Seats are inserted with ONE statement (unnest of
 * arrays), not one INSERT per seat: a 50,000-seat stadium is one round trip, not 50,000.
 */
export async function createEvent(pool: Pool, input: CreateEventInput): Promise<{ event: EventRow; seatCount: number }> {
  const labels: string[] = [];
  const sections: string[] = [];
  const prices: number[] = [];
  for (const section of input.sections) {
    for (let r = 0; r < section.rows; r++) {
      for (let s = 1; s <= section.seatsPerRow; s++) {
        labels.push(`${section.name} ${rowLabel(r)}${s}`);
        sections.push(section.name);
        prices.push(section.pricePaise);
      }
    }
  }

  return withTransaction(pool, async (client) => {
    const { rows } = await client.query<EventRow>(
      `INSERT INTO events (name, venue, starts_at, hold_seconds, max_seats_per_hold)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [input.name, input.venue, input.startsAt, input.holdSeconds, input.maxSeatsPerHold],
    );
    const event = rows[0];
    await client.query(
      `INSERT INTO seats (event_id, label, section, price_paise)
       SELECT $1, label, section, price FROM unnest($2::text[], $3::text[], $4::int[]) AS t(label, section, price)`,
      [event.id, labels, sections, prices],
    );
    return { event, seatCount: labels.length };
  });
}

export async function getEvent(pool: Pool, id: string): Promise<EventRow | null> {
  const { rows } = await pool.query<EventRow>('SELECT * FROM events WHERE id = $1', [id]);
  return rows[0] ?? null;
}

export function toEventResponse(event: EventRow) {
  return {
    id: event.id,
    name: event.name,
    venue: event.venue,
    startsAt: event.starts_at,
    holdSeconds: event.hold_seconds,
    maxSeatsPerHold: event.max_seats_per_hold,
  };
}
