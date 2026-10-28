# Seatlock design

Seatlock sells tickets when demand is far higher than supply: a big concert where 50,000 people want the same 5,000 seats, or a flash sale where 200,000 people want 5,000 items. It has two modes:

- **Seated booking** (like BookMyShow): pick specific seats, hold them for 10 minutes, pay, get a booking.
- **Flash sale**: limited stock, no seat choice, one per user, as fast as possible.

## Requirements

**Must never happen**
1. A seat sold to two people (double booking).
2. More items sold than exist (overselling).
3. A user charged without getting their seats, or a refund.
4. A user charged twice for the same seats.

**Should hold under load**
- Losing users get a fast, clear "sold out" or "seat taken" answer, not a timeout.
- Seats from abandoned checkouts go back on sale.
- One slow or failing part (payment gateway, email service) doesn't stop bookings.

**Out of scope:** user accounts and login (the user id comes from a header that an API gateway would set after checking a token), cancellations, seat map UI.

## Architecture

```
                      ┌──────────────────────── Seatlock ─────────────────────────┐
 Client ──HTTP──►     │  API (Node cluster)                                       │
                      │   ├─ seat map (cached 1s per process, single-flight)       │
                      │   ├─ holds ──► Postgres (truth)  [+ Redis gate, strategy 3]│
                      │   ├─ checkout ──► mock payment gateway                     │
                      │   ├─ webhooks ◄── mock payment gateway (signed)            │
                      │   └─ flash-sale buy ──► Redis (stock gate) ──► Kafka       │
                      │                                                ▼           │
                      │  Order writer (Kafka consumer) ──batch insert──► Postgres  │
                      │  Dispatcher (outbox) ──► Relay (emails), gateway (refunds) │
                      │  Reconciler ──► asks the gateway about stuck payments      │
                      └────────────────────────────────────────────────────────────┘
```

Postgres is the source of truth for every seat, hold, payment and order. Redis is used only to make things fast; if Redis loses its data, Postgres still prevents double booking and overselling.

## Seated booking flow

1. `GET /v1/events/:id/seats`: the seat map (labels, sections, prices, availability). `GET /v1/events/:id/availability` returns only the ids of free seats, a few KB, for clients that already have the layout and just poll what's free. Each API process keeps the finished JSON for 1 second, and when it expires, concurrent requests share one query instead of all querying at once (single-flight, which prevents a cache stampede). So 50,000 users refreshing it cost about one query per second per API process. The map can be up to a second out of date. That's fine because step 2 is the real check.
2. `POST /v1/events/:id/holds` with `Idempotency-Key`: hold 1 to 6 seats, all or nothing, for 10 minutes. `201` with the hold, or `409` with the seats that were taken.
3. `POST /v1/holds/:id/checkout`: creates a payment at the gateway and returns its id.
4. The user pays at the gateway. The gateway calls `POST /v1/webhooks/gateway`.
5. On `payment.succeeded`, in one transaction: mark the seats booked, create the booking, and add a "send confirmation" row to the outbox.
6. The dispatcher sends the confirmation email through Relay.

### Seat states

```
available ──hold──► held ──payment succeeded──► booked
                     │
                     └── held_until passes ──► counts as available again
```

**Holds expire lazily.** A `held` seat whose `held_until` has passed is treated as available by every query that looks for free seats. No background job has to release it on time, so there's no window where a seat is free but still shown as held because a job is late. (A sweeper can tidy up statuses later for reporting; correctness doesn't depend on it.)

### Preventing double booking: three strategies

`HOLD_STRATEGY` picks one, so they can be load-tested against each other on the same machine.

**1. Pessimistic locking (`SELECT ... FOR UPDATE`)**
```sql
BEGIN;
SELECT id, status, held_until FROM seats
 WHERE event_id = $1 AND id = ANY($2)
 ORDER BY id            -- always lock in the same order: no deadlocks
 FOR UPDATE;
-- every seat free (available, or held but expired)? then:
UPDATE seats SET status = 'held', hold_id = $3, held_until = now() + interval '10 min', version = version + 1
 WHERE id = ANY($2);
INSERT INTO holds ...;
COMMIT;
```
Competing requests wait in line for the row locks. Simple and correct. Under heavy contention, requests pile up waiting and hold database connections while they wait.

**2. Optimistic locking (version column)**
Read the seats and their versions without locking. Then:
```sql
UPDATE seats SET status = 'held', ..., version = version + 1
 WHERE id = $1 AND version = $2     -- only if nobody changed it since I read it
```
If any seat's update matches 0 rows, someone else got there first: roll back and answer 409. No waiting for locks. Works well when conflicts are rare; when everyone wants the same seats, most requests do the work and then fail.

**3. Redis gate, then Postgres**
A Lua script checks that every requested seat key is free and sets them all with a 10-minute TTL, in one atomic step. Most losing requests stop here, in about a millisecond, without touching Postgres. Winners then run strategy 2 in Postgres, which stays the source of truth: a Redis lock alone isn't enough, because Redis can lose keys (a restart, a failover) and a paused process can still act after its lock expired. If the Postgres step fails, the Redis keys are released.

The hold id is derived from the user and the Idempotency-Key, and is the value stored in each seat key. A client retry that arrives while the first attempt is still running recognises its own keys instead of getting a 409 for its own seats.

All seat keys for one event share a hash tag (`seat:{eventId}:...`) so the script still works on a Redis Cluster.

**Whatever the strategy**, `booking_seats` has `UNIQUE (seat_id)`. A bug in any strategy would produce a database error, not a double booking.

### Payments

The mock gateway behaves like Razorpay or Stripe, including the inconvenient parts:
- Webhooks are delivered **at least once**: sometimes twice, sometimes late, sometimes out of order.
- Webhooks are **signed**: header `t=<timestamp>,v1=<HMAC-SHA256 of "timestamp.body">`. Seatlock checks the signature with a constant-time comparison and rejects timestamps older than 5 minutes (so a captured webhook can't be replayed later).
- A configurable share of payments fail.

Handling:
- **Duplicates**: each event id is inserted into `webhook_events`; a second copy finds it already there and does nothing.
- **Out of order**: payment status only moves forward (`pending → succeeded | failed`, then `succeeded → refund_pending → refunded`). A `failed` that arrives after `succeeded` is ignored.
- **Lost webhooks**: the reconciler asks the gateway about payments still `pending` after 2 minutes, and applies the answer through the same code path as a webhook. Checkout does the same for the hold's pending payment, so a user whose card was declined can pay again even if the "failed" webhook hasn't arrived.
- **Charged twice**: a partial unique index allows one live payment per hold.

### Late payment: the hold expired, then the payment succeeded

The user took 12 minutes. Their hold expired at 10, and maybe someone else booked one of the seats at 11. Then the payment succeeds. In one transaction:
- If every seat is still held by this hold (nobody took it), book them anyway: the user paid.
- If any seat now belongs to someone else, book none of them (the user asked for seats together), release the rest, mark the payment `refund_pending`, and add a refund and a "sorry" email to the outbox.

### Side effects through the outbox

Confirmation emails (through Relay) and refunds (through the gateway) are written to the `outbox` table in the same transaction as the booking or payment change. The dispatcher delivers them afterwards (`FOR UPDATE SKIP LOCKED`, so several dispatchers can run at once). A failed row is retried with a growing delay (5s, 10s, 20s, ... up to 10 minutes) without holding up the rows behind it; a row that can never succeed (e.g. Relay rejects it as invalid) is parked with its error for a human to look at. Each call carries an idempotency key (`booking-confirmed:<bookingId>`, `refund:<paymentId>`), so a retried call can't send two emails or two refunds. Relay already deduplicates on its `Idempotency-Key`.

## Flash sale

Stock of N items, one per user, a huge crowd in the first seconds.

### The problem: one hot row

The obvious version:
```sql
UPDATE flash_sales SET sold = sold + 1 WHERE id = $1 AND sold < stock;
INSERT INTO flash_orders ...;
```
It's correct, but every buyer updates the **same row**, so every transaction waits for the one before it to commit. Throughput is capped at roughly 1 / (time the row stays locked), whatever the hardware, and waiting requests hold database connections until the pool runs out. This is the baseline to measure against.

### The design: Redis decides, Kafka carries, Postgres records

1. **Redis gate.** One Lua script per buy: is this user already a buyer? Is stock above zero? If not sold out: decrement stock, add the user to the buyers set. Redis runs each script atomically, and a single Redis can run tens of thousands of these per second. Losing buyers get "sold out" here and never reach Postgres.
2. **Kafka.** Winners' orders are published to the `flash-orders` topic (`acks=all`, idempotent producer), and the API answers `202 Accepted` with the order id. Kafka absorbs the burst: the API doesn't wait for Postgres.
3. **Order writer.** A consumer group reads `flash-orders` in batches, inserts each batch in one statement (`ON CONFLICT (sale_id, user_id) DO NOTHING`, so a redelivered batch is harmless), adds the batch size to `sold` once, and only then commits the Kafka offsets. One update of the counter row per 500 orders instead of per order: the hot row isn't hot anymore.

**Safety nets in Postgres:** `UNIQUE (sale_id, user_id)` (one per user) and `CHECK (sold <= stock)` (no overselling, even if Redis lost its data and reset the stock).

**Order ids are deterministic** (a UUID derived from sale id + user id). If the API crashes after the Redis decrement but before Kafka confirms the publish, the item is reserved but no order exists: that's underselling, never overselling. After the sale, a reconciler compares the Redis buyers set with Postgres and publishes the missing orders. Because the id is derived from the user, a re-published order can't become a second order.

**Why Kafka here, and RabbitMQ in Relay?** Relay sends tasks: each email is handled once and then deleted, with per-message retries and delays. Flash-sale orders are a stream of facts: written once, read in order and in batches, kept for replay if the order writer had a bug, and readable by other consumers later (analytics, notifications) without changing the producer.

## Correctness checks after every load test

The load test doesn't just measure speed. After each run it checks:

- No seat appears in two bookings.
- Every seat marked `booked` belongs to exactly one booking, and every booked seat in a booking is marked `booked`.
- Every succeeded payment has a booking or a refund. No hold has two live payments.
- Flash sale: orders = stock (when demand exceeded stock), no user has two orders, Redis stock is 0, Kafka consumer lag is 0.
- Every confirmed booking got exactly one confirmation email (counted at Relay's mock provider).

## Plan

1. Design and database schema
2. Seat map, holds, and the three locking strategies
3. Checkout, mock payment gateway, signed webhooks, refunds for late payments, reconciler
4. Confirmation emails through Relay (outbox + dispatcher)
5. Flash sale: hot-row baseline, Redis gate, Kafka, order writer
6. Load and stress tests: strategies compared, correctness checks, chaos
7. README with results
