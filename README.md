# Seatlock: ticket booking and flash sales under heavy contention

Thousands of people try to book the same seats at the same moment. Seatlock makes sure every seat is sold once, stock is never oversold, and every user who pays either gets their seats or a refund.

- **Seated booking** (like BookMyShow): hold seats for 10 minutes, pay, get a booking and a confirmation email. Holds expire on their own if the user doesn't pay.
- **Flash sale**: limited stock, one per user. Redis decides who gets one, Kafka carries the orders, Postgres records them in batches.

Design and trade-offs: [docs/design.md](docs/design.md).

## Architecture

```
                 ┌──────────────────────────── Seatlock ────────────────────────────┐
 Client ──HTTP──►│ API (Node cluster)                                                │
                 │  ├─ seat map / availability (cached 1s per process, single-flight)│
                 │  ├─ holds ──► [Redis gate] ──► Postgres (the source of truth)     │
                 │  ├─ checkout ──────────────────────────► payment gateway (mock)   │
                 │  ├─ webhooks (signed, deduplicated) ◄─── payment gateway          │
                 │  └─ flash-sale buy ──► Redis Lua (stock) ──► Kafka                │
                 │                                                │                  │
                 │ Order writer (consumer group) ◄────────────────┘                  │
                 │   └─ batch insert, one stock update per batch ──► Postgres        │
                 │ Jobs: outbox dispatcher ──► Relay (emails), gateway (refunds)     │
                 │       reconciler ──► asks the gateway about lost webhooks         │
                 └───────────────────────────────────────────────────────────────────┘
```

Confirmation emails go through [Relay](https://github.com/Ayushmaanagarwal1211/notificatiion_service), my email delivery service, with an idempotency key per booking.

## What it guarantees, and how

| Guarantee | How |
|---|---|
| No seat sold twice | Holds are atomic (three interchangeable strategies, below); `UNIQUE (seat_id)` on booked seats as the last line of defence |
| Abandoned seats go back on sale | Holds expire lazily: a hold past `held_until` counts as free in every check, no cron job needed |
| No user charged twice | One live payment per hold (partial unique index); idempotency keys on every gateway call |
| Pay too late → seats or a refund | If nobody took the seats, book them anyway; if someone did, book none and refund automatically |
| Webhooks: fake, replayed, duplicated, out of order, lost | HMAC signature + 5-minute timestamp window; event-id dedupe; forward-only status; a reconciler |
| Booking and email/refund never out of sync | Transactional outbox, at-least-once dispatcher, idempotency keys downstream |
| Flash sale never oversells | Atomic Lua check-and-decrement in Redis; `CHECK (sold <= stock)` in Postgres |
| Flash orders survive crashes | Kafka with `acks=all` and an idempotent producer; offsets committed only after the batch is in Postgres; deterministic order ids |

### Three ways to hold seats (`HOLD_STRATEGY`)

| Strategy | How | Good when |
|---|---|---|
| `pessimistic` | `SELECT ... FOR UPDATE`, locks taken in id order (no deadlocks) | Contention is high and requests should queue fairly |
| `optimistic` | Read versions, then `UPDATE ... WHERE version = $read` | Most requests don't collide |
| `redis` | Atomic Lua script turns losers away in Redis; winners run the optimistic path, so Postgres still decides | Most requests lose (a big on-sale moment) |

## Stress test results

Each run starts the whole system, sends load with k6, waits until every payment, refund, email and order has settled, and then **checks the result with SQL**: no seat sold twice, money captured minus refunded equals the bookings, every payment settled, one email per booking, no item oversold, every "you got one" answer has an order. The mock gateway misbehaves on purpose: 10% of webhooks are delivered twice, up to 2s late, and 2% never.

Machine: Apple M4 laptop (10 cores, 16 GB), with k6, the API (5 processes), Postgres, Redis and Kafka all running on it. Full reports: [load-results/](load-results).

**Flash sale: 3,000 buy attempts/s for 30s, 5,000 items** ([report](load-results/2026-10-28T06-07-16-compare-flash/report.md))

| | Postgres only (one hot row) | Redis gate + Kafka + batched writer |
|---|---|---|
| Throughput | 94 buys/s (k6 couldn't start 82,011 attempts) | **3,008 buys/s** (none dropped) |
| Latency p50 / p99 | 15.8 s / 50.6 s | **0 ms / 6 ms** |
| Sold out after | 43 s | **2 s** |
| Updates of the stock row | 5,000 | **10** |
| Oversold / users with two orders | 0 / 0 | 0 / 0 |

In the baseline every buyer updates the same `flash_sales` row, so buys go one at a time, each waiting for the previous commit. With Redis in front, Postgres sees one update per batch of up to 500.

**Flash sale with failures: 60,000 items, `kill -9` the order writer and an API process mid-sale** ([report](load-results/2026-10-28T06-03-57-flash/report.md)): 93,090 requests at 3,103/s, p99 8 ms, 60,000 orders written, 0 oversold, 0 double orders, every buyer told "you got one" has an order.

**Seated booking: 1,000 new users/s for 30s, 10,000 seats, all three strategies** ([report](load-results/2026-10-28T05-55-39-compare-holds/report.md)). Users look at the availability list, want 1-4 adjacent seats, and 60% aim at the front 10% of the stadium; 10% abandon, 5% get declined and retry, 3% pay after their hold expired.

| | pessimistic | optimistic | redis |
|---|---|---|---|
| Hold requests (won) | 49,688 (6,774) | 52,930 (6,771) | 52,847 (6,837) |
| Database transactions | 126,662 | 140,324 | **94,127** |
| 95% of seats taken after | 17 s | 15 s | **13 s** |
| Seats sold twice, money mismatch, unsettled payments | 0 | 0 | 0 |

At this rate the laptop itself is the limit (k6 dropped 6-10K iterations in each run), so latency numbers are noisy. The clear difference is database work: the Redis gate turns losing requests away before they reach Postgres.

**Seated booking with failures: 300 users/s for 60s, 5,000 seats, `kill -9` the background jobs and an API process** ([report](load-results/2026-10-28T06-05-09-seated/report.md)): all 5,000 seats booked, 105 late payers refunded automatically, 71 lost webhooks recovered by the reconciler, every check 0.

The stress tests found two bugs, both fixed: refunds stuck in `refund_pending` when the refund webhook was lost (the reconciler now covers refunds too), and an up-front idempotency lookup that roughly doubled the database work of each hold request.

## Run it

**GitHub Codespaces:** click **Code → Codespaces → Create codespace**, then:

```bash
npm run infra:up && npm run db:migrate && npm run dev
```

**On your own machine:** requires Node.js 22+ and Docker.

```bash
npm install
cp .env.example .env
npm run infra:up      # Postgres (5433), Redis (6380), Kafka (9092)
npm run db:migrate
npm run dev           # API (4000), mock gateway (4100), background jobs, flash order writer
```

To send real confirmation emails, run [Relay](https://github.com/Ayushmaanagarwal1211/notificatiion_service) and set `RELAY_URL=http://localhost:3000`; otherwise emails are logged.

Try it:

```bash
# Create an event: 2 sections, 100 seats
curl -s -X POST localhost:4000/v1/events -H 'Content-Type: application/json' -d '{
  "name": "Coldplay", "venue": "DY Patil Stadium", "startsAt": "2027-01-18T19:00:00+05:30",
  "sections": [{"name": "Platinum", "rows": 2, "seatsPerRow": 10, "pricePaise": 1250000},
               {"name": "Gold", "rows": 4, "seatsPerRow": 20, "pricePaise": 450000}]}'

# Hold two seats (use the event id from above)
curl -s -X POST localhost:4000/v1/events/<eventId>/holds -H 'Content-Type: application/json' \
  -H 'X-User-Id: alice' -H 'Idempotency-Key: hold-1' -d '{"seatIds": [1, 2]}'

# Pay: checkout returns the gateway's payment page; "complete" stands in for the user paying
curl -s -X POST localhost:4000/v1/holds/<holdId>/checkout -H 'Content-Type: application/json' \
  -H 'X-User-Id: alice' -d '{"email": "alice@example.com"}'
curl -s -X POST <payUrl> -H 'Content-Type: application/json' -d '{"outcome": "succeeded"}'
curl -s localhost:4000/v1/holds/<holdId> -H 'X-User-Id: alice'   # status: booked, bookingId
```

## API

| Method | Path | |
|---|---|---|
| `POST` | `/v1/events` | Create an event and its seats |
| `GET` | `/v1/events/:id/seats` | Seat map (cached ~1s) |
| `GET` | `/v1/events/:id/availability` | Ids of free seats only (cached ~1s) |
| `POST` | `/v1/events/:id/holds` | Hold seats, all or nothing. Headers: `X-User-Id`, `Idempotency-Key`. 201, or 409 with the taken seats |
| `GET` / `DELETE` | `/v1/holds/:id` | A hold with its payment and booking / release it |
| `POST` | `/v1/holds/:id/checkout` | Start paying; returns the gateway's payment page |
| `POST` | `/v1/webhooks/gateway` | Payment outcomes from the gateway (signed) |
| `GET` | `/v1/bookings/:id` | A booking and its seats |
| `POST` | `/v1/flash-sales` | Create a flash sale |
| `POST` | `/v1/flash-sales/:id/orders` | Try to buy one. 202 reserved, 200 already yours, 409 sold out |
| `GET` | `/v1/flash-sales/:id/orders/me` | Did I get one? (`processing` until the order writer stores it) |

The user id comes from `X-User-Id`; in production an API gateway would set it after verifying the user's login.

## Tests

```bash
npm test                  # unit tests
npm run test:integration  # needs Postgres, Redis and Kafka (npm run infra:up)
npm run stress -- compare-holds   # or: seated, flash, compare-flash; CHAOS=true to kill processes mid-load
```

The integration tests race many requests against each other: 50 users clicking the same seat, 60 overlapping multi-seat holds, the same webhook 5 times at once, 200 buyers for 20 items. Every hold test runs against all three strategies. GitHub Actions runs them on each push.

## Project layout

```
src/
  api/        HTTP API: events, holds (3 strategies), payments, webhooks, flash sales
  gateway/    mock payment gateway (its own process)
  jobs/       outbox dispatcher (Relay emails, refunds) and payment reconciler
  flash/      Kafka order writer and the flash-sale reconciler
  shared/     config, Postgres, Redis, Kafka, webhook signatures
  scripts/    migrate, stress tests
migrations/   SQL schema
load/         k6 scripts
tests/        unit and integration tests
docs/         design
```
