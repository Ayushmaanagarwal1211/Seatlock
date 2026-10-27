import { Router } from 'express';
import { createHold, toHoldResponse } from '../holds/holds.service';
import { parseBody, parseId, requireIdempotencyKey, requireUser } from '../http';
import { createEventSchema, createHoldSchema } from '../schemas';
import type { AppDeps } from '../app';
import { createEvent, getEvent, toEventResponse } from './events.service';

export function eventsRouter({ pool, holdStrategy, seatMap }: AppDeps): Router {
  const router = Router();

  // POST /v1/events: create an event and its seats (an admin action; no auth in this project)
  router.post('/', async (req, res) => {
    const input = parseBody(createEventSchema, req, res);
    if (!input) return;
    const { event, seatCount } = await createEvent(pool, input);
    res.status(201).json({ ...toEventResponse(event), seatCount });
  });

  router.get('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Event');
    if (!id) return;
    const event = await getEvent(pool, id);
    if (!event) {
      res.status(404).json({ error: 'Event not found' });
      return;
    }
    res.json(toEventResponse(event));
  });

  // GET /v1/events/:id/seats: the seat map, cached for about a second (see seat-map.ts)
  router.get('/:id/seats', async (req, res) => {
    const id = parseId(req.params.id, res, 'Event');
    if (!id) return;
    const body = await seatMap.get(id);
    if (!body) {
      res.status(404).json({ error: 'Event not found' });
      return;
    }
    res.type('application/json').send(body);
  });

  // POST /v1/events/:id/holds  (headers: X-User-Id, Idempotency-Key)
  // Holds 1 to maxSeatsPerHold seats, all or nothing, until the hold expires.
  router.post('/:id/holds', async (req, res) => {
    const eventId = parseId(req.params.id, res, 'Event');
    if (!eventId) return;
    const userId = requireUser(req, res);
    if (!userId) return;
    const key = requireIdempotencyKey(req, res);
    if (!key) return;
    const input = parseBody(createHoldSchema, req, res);
    if (!input) return;

    const result = await createHold(pool, holdStrategy, { eventId, userId, idempotencyKey: key, seatIds: input.seatIds });
    switch (result.kind) {
      case 'created':
        res.status(201).json(toHoldResponse(result.hold));
        return;
      case 'replayed':
        res.set('Idempotent-Replayed', 'true').status(201).json(toHoldResponse(result.hold));
        return;
      case 'key_reused':
        res.status(422).json({ error: 'This Idempotency-Key was already used with a different request.' });
        return;
      case 'event_not_found':
        res.status(404).json({ error: 'Event not found' });
        return;
      case 'too_many_seats':
        res.status(400).json({ error: `At most ${result.max} seats per hold.` });
        return;
      case 'invalid_seats':
        res.status(400).json({ error: 'Some seats are not part of this event.', seatIds: result.seatIds });
        return;
      case 'unavailable':
        res.status(409).json({ error: 'Some seats are no longer available.', unavailableSeatIds: result.seatIds });
        return;
      case 'contention':
        res.status(409).json({ error: 'Someone else was booking these seats at the same moment. Try again.' });
        return;
    }
  });

  return router;
}
