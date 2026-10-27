-- Step 3: checkout asks for a contact email, used for the confirmation (or refund) email.
ALTER TABLE payments ADD COLUMN contact_email text;

-- Step 4: the dispatcher retries failed side effects with growing delays, per row, so one
-- failing call (Relay down, gateway slow) doesn't block the rest or get hammered.
ALTER TABLE outbox ADD COLUMN attempts integer NOT NULL DEFAULT 0;
ALTER TABLE outbox ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE outbox ADD COLUMN last_error text;

DROP INDEX outbox_unpublished_idx;
CREATE INDEX outbox_due_idx ON outbox (next_attempt_at, id) WHERE published_at IS NULL;
