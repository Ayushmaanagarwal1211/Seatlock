# Stress test: compare-flash

Machine: 10 cores, 5 API processes. Everything (k6, API, Postgres, Redis, Kafka) on one machine.

## Flash sale: postgres strategy

3,000 buy attempts/s for 30s, stock 5,000, every buyer a new user (5% click twice).

| | |
|---|---|
| Buy requests | 5,631 at 94/s (5,000 got one, 69 double clicks answered with the same order, 562 sold out, 0 busy, 0 errors) |
| Buy latency p50 / p95 / p99 / max | 15780 ms / 44815 ms / 50601 ms / 53545 ms |
| Sold out after | 43s |
| Orders written to Postgres | 5,000 |
| Updates of the stock row | 5,000 |
| Database transactions | 13,437 |
| Order writer caught up after the load | 0.0s |
| Orders recovered by the reconciler | 0 (reserved in Redis, never reached Kafka) |
| Orders whose buyer never saw the answer | 0 (their request died with a killed API process; the order is theirs) |
| k6 dropped iterations | 82,011 |

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

## Flash sale: redis-kafka strategy

3,000 buy attempts/s for 30s, stock 5,000, every buyer a new user (5% click twice).

| | |
|---|---|
| Buy requests | 90,242 at 3,008/s (5,000 got one, 241 double clicks answered with the same order, 85,001 sold out, 0 busy, 0 errors) |
| Buy latency p50 / p95 / p99 / max | 0 ms / 4 ms / 6 ms / 21 ms |
| Sold out after | 2s |
| Orders written to Postgres | 5,000 |
| Updates of the stock row | 10 |
| Database transactions | 49 |
| Order writer caught up after the load | 0.0s |
| Orders recovered by the reconciler | 0 (reserved in Redis, never reached Kafka) |
| Orders whose buyer never saw the answer | 0 (their request died with a killed API process; the order is theirs) |
| k6 dropped iterations | 0 |

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
