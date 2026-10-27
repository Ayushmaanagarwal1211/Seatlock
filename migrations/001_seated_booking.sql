-- Seated booking: events, seats, holds, payments, bookings.
-- Money is stored in paise (integer), never as a float: 0.1 + 0.2 != 0.3 in floating point.

CREATE TABLE events (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name                text NOT NULL,
  venue               text NOT NULL,
  starts_at           timestamptz NOT NULL,
  -- How long a hold keeps seats before they go back on sale
  hold_seconds        integer NOT NULL DEFAULT 600 CHECK (hold_seconds > 0),
  max_seats_per_hold  integer NOT NULL DEFAULT 6 CHECK (max_seats_per_hold BETWEEN 1 AND 10),
  created_at          timestamptz NOT NULL DEFAULT now()
);

-- A user's temporary claim on some seats while they pay.
CREATE TABLE holds (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id         uuid NOT NULL REFERENCES events (id),
  user_id          text NOT NULL,

  -- Idempotency, scoped per user: a retried "hold these seats" request returns the same hold.
  idempotency_key  text NOT NULL,
  request_hash     text NOT NULL,

  -- The seats this hold asked for. Kept even after the hold expires, so a late payment
  -- can tell which seats it paid for (and whether someone else has them now).
  seat_ids         bigint[] NOT NULL CHECK (cardinality(seat_ids) BETWEEN 1 AND 10),
  amount_paise     integer NOT NULL CHECK (amount_paise > 0),

  -- 'active' past expires_at counts as expired; nothing has to flip it on time.
  status           text NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'booked', 'released', 'expired')),
  expires_at       timestamptz NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),

  UNIQUE (user_id, idempotency_key)
);

-- One row per seat per event. This table is where contention happens: thousands of users
-- try to change the same rows at the same moment.
CREATE TABLE seats (
  -- Locks are always taken in id order, so two holds that want overlapping seats
  -- can't deadlock (each would otherwise wait for a lock the other holds).
  id           bigserial PRIMARY KEY,
  event_id     uuid NOT NULL REFERENCES events (id),
  label        text NOT NULL,      -- what the user sees, e.g. 'A-12'
  section      text NOT NULL,      -- e.g. 'Gold'
  price_paise  integer NOT NULL CHECK (price_paise > 0),

  -- available → held → booked. A 'held' seat whose held_until has passed is available
  -- again: expiry is checked when someone asks for the seat, not by a background job.
  status       text NOT NULL DEFAULT 'available' CHECK (status IN ('available', 'held', 'booked')),
  hold_id      uuid REFERENCES holds (id),
  held_until   timestamptz,
  -- Bumped on every change. Used by the optimistic hold strategy ("change it only if
  -- nobody else changed it since I read it").
  version      integer NOT NULL DEFAULT 0,

  UNIQUE (event_id, label),
  -- The three states can't be mixed up, whatever the application code does.
  CHECK (
    (status = 'available' AND hold_id IS NULL     AND held_until IS NULL) OR
    (status = 'held'      AND hold_id IS NOT NULL AND held_until IS NOT NULL) OR
    (status = 'booked'    AND hold_id IS NOT NULL AND held_until IS NULL)
  )
);

CREATE INDEX seats_hold_idx ON seats (hold_id) WHERE hold_id IS NOT NULL;

CREATE TABLE payments (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  hold_id             uuid NOT NULL REFERENCES holds (id),
  amount_paise        integer NOT NULL CHECK (amount_paise > 0),
  -- pending → succeeded | failed;  succeeded → refund_pending → refunded
  status              text NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'succeeded', 'failed', 'refund_pending', 'refunded')),
  gateway_payment_id  text UNIQUE,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);

-- At most one live payment per hold, so a user can't be charged twice for the same seats.
-- A failed payment doesn't count: the user may try again with another card.
CREATE UNIQUE INDEX payments_one_live_per_hold ON payments (hold_id) WHERE status <> 'failed';

-- The reconciler looks for payments stuck in 'pending' (their webhook never arrived).
CREATE INDEX payments_pending_idx ON payments (created_at) WHERE status = 'pending';

-- Every webhook event already processed. Gateways deliver webhooks at least once,
-- so the same event can arrive twice: the primary key makes the second one a no-op.
CREATE TABLE webhook_events (
  id           text PRIMARY KEY,   -- the gateway's event id
  type         text NOT NULL,
  payload      jsonb NOT NULL,
  received_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE bookings (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id      uuid NOT NULL REFERENCES events (id),
  user_id       text NOT NULL,
  hold_id       uuid NOT NULL UNIQUE REFERENCES holds (id),
  payment_id    uuid NOT NULL UNIQUE REFERENCES payments (id),
  amount_paise  integer NOT NULL CHECK (amount_paise > 0),
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE booking_seats (
  booking_id  uuid NOT NULL REFERENCES bookings (id),
  seat_id     bigint NOT NULL REFERENCES seats (id),
  PRIMARY KEY (booking_id, seat_id),
  -- The last line of defence against double booking: a seat can be in only one booking.
  -- Even if the hold logic had a bug, Postgres would reject the second booking.
  UNIQUE (seat_id)
);

-- Transactional outbox: side effects on other systems (confirmation emails through Relay,
-- refunds through the payment gateway), written in the same transaction as the change
-- that causes them. A dispatcher process delivers them afterwards.
CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,
  topic         text NOT NULL,    -- e.g. 'booking.confirmed', 'payment.refund'
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
