import Redis from 'ioredis';
import { Pool } from 'pg';
import request from 'supertest';
import { createApp } from '../../src/api/app';
import { SeatMapCache } from '../../src/api/events/seat-map';
import { createHoldStrategy } from '../../src/api/holds/strategies';
import { createTestPool, createTestRedis, resetDatabase } from './helpers';

let pool: Pool;
let redis: Redis;
let app: ReturnType<typeof createApp>;

beforeAll(() => {
  pool = createTestPool();
  redis = createTestRedis();
  // Cache off (0ms), so each test sees seat changes immediately.
  app = createApp({ pool, holdStrategy: createHoldStrategy('pessimistic', pool, redis), seatMap: new SeatMapCache(pool, 0) });
});

afterAll(async () => {
  await pool.end();
  redis.disconnect();
});

beforeEach(async () => {
  await resetDatabase(pool);
});

const EVENT = {
  name: 'Coldplay: Music of the Spheres',
  venue: 'DY Patil Stadium',
  startsAt: '2027-01-18T19:00:00+05:30',
  sections: [
    { name: 'Platinum', rows: 2, seatsPerRow: 5, pricePaise: 1_250_000 },
    { name: 'Gold', rows: 3, seatsPerRow: 10, pricePaise: 450_000 },
  ],
};

async function createEvent() {
  const res = await request(app).post('/v1/events').send(EVENT).expect(201);
  const map = await request(app).get(`/v1/events/${res.body.id}/seats`).expect(200);
  return { eventId: res.body.id as string, seats: map.body.seats as { id: number; label: string; available: boolean }[] };
}

function postHold(eventId: string, seatIds: number[], { user = 'alice', key = 'key-1' } = {}) {
  return request(app)
    .post(`/v1/events/${eventId}/holds`)
    .set('X-User-Id', user)
    .set('Idempotency-Key', key)
    .send({ seatIds });
}

describe('events and seat map', () => {
  it('creates an event with every seat of every section', async () => {
    const res = await request(app).post('/v1/events').send(EVENT).expect(201);

    expect(res.body).toMatchObject({ name: EVENT.name, seatCount: 40, holdSeconds: 600, maxSeatsPerHold: 6 });
  });

  it('returns the seat map with labels and availability', async () => {
    const { seats } = await createEvent();

    expect(seats).toHaveLength(40);
    expect(seats[0]).toMatchObject({ label: 'Platinum A1', available: true, pricePaise: 1_250_000 });
    expect(seats[39].label).toBe('Gold C10');
  });

  it('shows held seats as unavailable', async () => {
    const { eventId, seats } = await createEvent();
    await postHold(eventId, [seats[0].id]).expect(201);

    const map = await request(app).get(`/v1/events/${eventId}/seats`).expect(200);

    expect(map.body.seats[0].available).toBe(false);
    expect(map.body.seats[1].available).toBe(true);
  });

  it('returns 404 for an unknown event', async () => {
    await request(app).get('/v1/events/00000000-0000-0000-0000-000000000000/seats').expect(404);
    await request(app).get('/v1/events/not-a-uuid').expect(404);
  });

  it('validates the event body', async () => {
    await request(app).post('/v1/events').send({ ...EVENT, sections: [] }).expect(400);
  });
});

describe('POST /v1/events/:id/holds', () => {
  it('holds seats: 201 with the total price and expiry', async () => {
    const { eventId, seats } = await createEvent();

    const res = await postHold(eventId, [seats[0].id, seats[1].id]).expect(201);

    expect(res.body).toMatchObject({ eventId, seatIds: [seats[0].id, seats[1].id], amountPaise: 2_500_000, status: 'active' });
    expect(new Date(res.body.expiresAt).getTime()).toBeGreaterThan(Date.now() + 590_000);
  });

  it('replays a retried request with the same key', async () => {
    const { eventId, seats } = await createEvent();
    const first = await postHold(eventId, [seats[0].id]).expect(201);

    const retry = await postHold(eventId, [seats[0].id]).expect(201);

    expect(retry.headers['idempotent-replayed']).toBe('true');
    expect(retry.body.id).toBe(first.body.id);
  });

  it('returns 409 with the taken seats', async () => {
    const { eventId, seats } = await createEvent();
    await postHold(eventId, [seats[0].id]).expect(201);

    const res = await postHold(eventId, [seats[0].id, seats[1].id], { user: 'bob' }).expect(409);

    expect(res.body.unavailableSeatIds).toEqual([seats[0].id]);
  });

  it('returns 422 when a key is reused for other seats', async () => {
    const { eventId, seats } = await createEvent();
    await postHold(eventId, [seats[0].id]).expect(201);

    await postHold(eventId, [seats[1].id]).expect(422);
  });

  it('requires a user and an idempotency key', async () => {
    const { eventId, seats } = await createEvent();

    await request(app).post(`/v1/events/${eventId}/holds`).set('Idempotency-Key', 'k').send({ seatIds: [seats[0].id] }).expect(401);
    await request(app).post(`/v1/events/${eventId}/holds`).set('X-User-Id', 'alice').send({ seatIds: [seats[0].id] }).expect(400);
  });

  it('rejects too many seats, duplicates and unknown seats', async () => {
    const { eventId, seats } = await createEvent();

    await postHold(eventId, seats.slice(0, 7).map((s) => s.id), { key: 'a' }).expect(400);
    await postHold(eventId, [seats[0].id, seats[0].id], { key: 'b' }).expect(400);
    await postHold(eventId, [999_999_999], { key: 'c' }).expect(400);
  });
});

describe('/v1/holds/:id', () => {
  it('shows a hold to its owner only', async () => {
    const { eventId, seats } = await createEvent();
    const { body } = await postHold(eventId, [seats[0].id]).expect(201);

    await request(app).get(`/v1/holds/${body.id}`).set('X-User-Id', 'alice').expect(200);
    await request(app).get(`/v1/holds/${body.id}`).set('X-User-Id', 'bob').expect(404);
  });

  it('releases a hold, and the seats go back on sale', async () => {
    const { eventId, seats } = await createEvent();
    const { body } = await postHold(eventId, [seats[0].id]).expect(201);

    const released = await request(app).delete(`/v1/holds/${body.id}`).set('X-User-Id', 'alice').expect(200);
    expect(released.body.status).toBe('released');
    await request(app).delete(`/v1/holds/${body.id}`).set('X-User-Id', 'alice').expect(409);

    await postHold(eventId, [seats[0].id], { user: 'bob' }).expect(201);
  });
});
