import { Router } from 'express';
import type { AppDeps } from './app';
import { parseId, requireUser } from './http';

export function bookingsRouter({ pool }: AppDeps): Router {
  const router = Router();

  // GET /v1/bookings/:id: a confirmed booking and its seats (owner only)
  router.get('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Booking');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;

    const { rows } = await pool.query<{
      id: string;
      event_id: string;
      amount_paise: number;
      created_at: Date;
      seats: { id: number; label: string }[];
    }>(
      `SELECT b.id, b.event_id, b.amount_paise, b.created_at,
              json_agg(json_build_object('id', s.id, 'label', s.label) ORDER BY s.id) AS seats
         FROM bookings b
         JOIN booking_seats bs ON bs.booking_id = b.id
         JOIN seats s ON s.id = bs.seat_id
        WHERE b.id = $1 AND b.user_id = $2
        GROUP BY b.id`,
      [id, userId],
    );
    const booking = rows[0];
    if (!booking) {
      res.status(404).json({ error: 'Booking not found' });
      return;
    }
    res.json({
      id: booking.id,
      eventId: booking.event_id,
      amountPaise: booking.amount_paise,
      seats: booking.seats,
      createdAt: booking.created_at,
    });
  });

  return router;
}
