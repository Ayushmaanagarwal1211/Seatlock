# Stress test: seated

Machine: 10 cores, 5 API processes, CHAOS on. Everything (k6, API, Postgres, Redis, Kafka) on one machine.

## Seated: redis strategy

300 new users/s for 60s, 5,000 seats, holds last 15s.
Each user looks at the availability list (cached up to 1s), wants 1-4 seats side by side, and 60% aim at the
front 10% of the stadium. Taken seats → they pick again (3 tries). After holding: 82% pay, 5% get declined and
pay on the second try, 10% abandon (the hold expires), 3% pay after their hold expired.

| | |
|---|---|
| Hold requests | 21,079 (3,440 won, 17,639 seats already taken, 0 errors) |
| Hold latency p50 / p95 / p99 | 0 ms / 2 ms / 28 ms |
| 95% of seats held or booked after | 10s |
| Bookings | 3,006 (5,000 of 5,000 seats) |
| Payments declined, then retried | 159 |
| Paid after the hold expired | 106 (1 still booked: nobody had taken their seats; 105 refunded) |
| Webhooks | 3,375 events, 375 sent twice, 71 never sent (the reconciler found them) |
| Email requests deduplicated by Relay | 0 |
| Users who found no free seats left | 9,094 |
| Database transactions during the load | 42,764 |
| Deadlocks Postgres had to break | 0 |
| Time to settle after the load | 0.5s |
| k6 requests (dropped: k6 couldn't start in time) | 57,793 (dropped 0) |

Chaos: 24s: kill -9 jobs, restarted 3s later; 36s: kill -9 one API process (pid 30474)

**Checks** (each must be 0):

| Check | Value | |
|---|---|---|
| seatsSoldTwice | 0 | PASS |
| seatStatusMismatch | 0 | PASS |
| paidWithoutSeatsOrRefund | 0 | PASS |
| paymentsNotSettled | 0 | PASS |
| holdsWithTwoLivePayments | 0 | PASS |
| moneyMismatchPaise | 0 | PASS |
| emailsMissing | 0 | PASS |
