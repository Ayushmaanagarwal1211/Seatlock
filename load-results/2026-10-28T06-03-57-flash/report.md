# Stress test: flash

Machine: 10 cores, 5 API processes, CHAOS on. Everything (k6, API, Postgres, Redis, Kafka) on one machine.

## Flash sale: redis-kafka strategy

3,000 buy attempts/s for 30s, stock 60,000, every buyer a new user (5% click twice).

| | |
|---|---|
| Buy requests | 93,090 at 3,103/s (59,994 got one, 3,089 double clicks answered with the same order, 29,999 sold out, 0 busy, 8 errors) |
| Buy latency p50 / p95 / p99 / max | 4 ms / 6 ms / 8 ms / 51 ms |
| Sold out after | 21s |
| Orders written to Postgres | 60,000 |
| Updates of the stock row | 199 |
| Database transactions | 249 |
| Order writer caught up after the load | 0.0s |
| Orders recovered by the reconciler | 0 (reserved in Redis, never reached Kafka) |
| Orders whose buyer never saw the answer | 6 (their request died with a killed API process; the order is theirs) |
| k6 dropped iterations | 0 |

Chaos: 12s: kill -9 order-writer, restarted 3s later; 18s: kill -9 one API process (pid 30294)

**Checks** (each must be 0):

| Check | Value | |
|---|---|---|
| oversold | 0 | PASS |
| soldCounterMismatch | 0 | PASS |
| usersWithTwoOrders | 0 | PASS |
| toldYesButNoOrder | 0 | PASS |
| redisAndPostgresDisagree | 0 | PASS |
| unsoldWhileDemandExceededStock | 0 | PASS |
| ordersRepublishedByReconciler | 0 | not a failure: reservations that never reached Kafka (an API process died), recovered by the reconciler |
