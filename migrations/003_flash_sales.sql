-- Flash sales: limited stock, one item per user, a huge crowd in the first seconds.

CREATE TABLE flash_sales (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name         text NOT NULL,
  price_paise  integer NOT NULL CHECK (price_paise > 0),
  stock        integer NOT NULL CHECK (stock > 0),
  sold         integer NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now(),
  -- The last line of defence against overselling. Redis decides who gets an item, but if
  -- Redis ever let too many through (it lost its data and reset the stock), Postgres
  -- refuses to record more sales than there is stock.
  CHECK (sold >= 0 AND sold <= stock)
);

CREATE TABLE flash_orders (
  -- Derived from (sale, user), not random: a re-sent order message lands on the same row.
  id           uuid PRIMARY KEY,
  sale_id      uuid NOT NULL REFERENCES flash_sales (id),
  user_id      text NOT NULL,
  price_paise  integer NOT NULL CHECK (price_paise > 0),
  reserved_at  timestamptz NOT NULL,          -- when the user was told "you got one"
  created_at   timestamptz NOT NULL DEFAULT now(), -- when the order was written here
  -- One per user, whatever happens upstream.
  UNIQUE (sale_id, user_id)
);
