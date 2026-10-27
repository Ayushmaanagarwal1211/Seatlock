# Seatlock: ticket booking and flash sales under heavy contention

Thousands of people try to book the same seats at the same moment. Seatlock makes sure every seat is sold once, stock is never oversold, and every user who pays either gets their seats or a refund.

- **Seated booking** (like BookMyShow): hold seats for 10 minutes, pay, get a booking. Holds expire on their own if the user doesn't pay.
- **Flash sale**: limited stock, one per user. Redis decides who gets one, Kafka carries the orders, Postgres records them in batches.

Design and trade-offs: [docs/design.md](docs/design.md).

## Progress

- [x] Step 1: Design and database schema
- [x] Step 2: Seat map, holds, and three locking strategies (pessimistic, optimistic, Redis gate)
- [x] Step 3: Checkout, mock payment gateway, signed idempotent webhooks, refunds for late payments
- [ ] Step 4: Confirmation emails through [Relay](https://github.com/Ayushmaanagarwal1211/notificatiion_service)
- [ ] Step 5: Flash sale: hot-row baseline, Redis gate, Kafka, batched order writer
- [ ] Step 6: Load and stress tests with correctness checks
- [ ] Step 7: Final README and results

## Run it

**GitHub Codespaces:** click **Code → Codespaces → Create codespace**, then:

```bash
npm run infra:up && npm run db:migrate
```

**On your own machine:** requires Node.js 22+ and Docker.

```bash
npm install
cp .env.example .env
npm run infra:up      # Postgres (port 5433) and Redis (port 6380)
npm run db:migrate
```

## Tests

```bash
npm test                  # unit tests, no Docker needed
npm run test:integration  # needs Postgres; uses the separate seatlock_test database
```

## Project layout

```
src/
  shared/     config, database pool, migrations
  scripts/    one-off commands: migrate
migrations/   SQL schema, applied in filename order
tests/        unit and integration tests
docs/         design
```
