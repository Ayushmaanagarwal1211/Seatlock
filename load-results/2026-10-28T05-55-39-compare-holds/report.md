# Stress test: compare-holds

Machine: 10 cores, 5 API processes. Everything (k6, API, Postgres, Redis, Kafka) on one machine.

## Seated: pessimistic strategy

1,000 new users/s for 30s, 10,000 seats, holds last 15s.
Each user looks at the availability list (cached up to 1s), wants 1-4 seats side by side, and 60% aim at the
front 10% of the stadium. Taken seats → they pick again (3 tries). After holding: 82% pay, 5% get declined and
pay on the second try, 10% abandon (the hold expires), 3% pay after their hold expired.

| | |
|---|---|
| Hold requests | 49,688 (6,774 won, 42,914 seats already taken, 0 errors) |
| Hold latency p50 / p95 / p99 | 0 ms / 345 ms / 786 ms |
| 95% of seats held or booked after | 17s |
| Bookings | 5,903 (9,796 of 10,000 seats) |
| Payments declined, then retried | 342 |
| Paid after the hold expired | 192 (48 still booked: nobody had taken their seats; 149 refunded) |
| Webhooks | 6,551 events, 672 sent twice, 142 never sent (the reconciler found them) |
| Email requests deduplicated by Relay | 0 |
| Users who found no free seats left | 0 |
| Database transactions during the load | 126,662 |
| Deadlocks Postgres had to break | 0 |
| Time to settle after the load | 4.3s |
| k6 requests (dropped: k6 couldn't start in time) | 112,181 (dropped 9,929) |

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

## Seated: optimistic strategy

1,000 new users/s for 30s, 10,000 seats, holds last 15s.
Each user looks at the availability list (cached up to 1s), wants 1-4 seats side by side, and 60% aim at the
front 10% of the stadium. Taken seats → they pick again (3 tries). After holding: 82% pay, 5% get declined and
pay on the second try, 10% abandon (the hold expires), 3% pay after their hold expired.

| | |
|---|---|
| Hold requests | 52,930 (6,771 won, 46,159 seats already taken, 0 errors) |
| Hold latency p50 / p95 / p99 | 0 ms / 183 ms / 673 ms |
| 95% of seats held or booked after | 15s |
| Bookings | 5,902 (9,814 of 10,000 seats) |
| Payments declined, then retried | 343 |
| Paid after the hold expired | 196 (38 still booked: nobody had taken their seats; 170 refunded) |
| Webhooks | 6,592 events, 661 sent twice, 136 never sent (the reconciler found them) |
| Email requests deduplicated by Relay | 0 |
| Users who found no free seats left | 415 |
| Database transactions during the load | 140,324 |
| Deadlocks Postgres had to break | 0 |
| Time to settle after the load | 0.3s |
| k6 requests (dropped: k6 couldn't start in time) | 119,122 (dropped 8,434) |

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

## Seated: redis strategy

1,000 new users/s for 30s, 10,000 seats, holds last 15s.
Each user looks at the availability list (cached up to 1s), wants 1-4 seats side by side, and 60% aim at the
front 10% of the stadium. Taken seats → they pick again (3 tries). After holding: 82% pay, 5% get declined and
pay on the second try, 10% abandon (the hold expires), 3% pay after their hold expired.

| | |
|---|---|
| Hold requests | 52,847 (6,837 won, 46,010 seats already taken, 0 errors) |
| Hold latency p50 / p95 / p99 | 0 ms / 205 ms / 618 ms |
| 95% of seats held or booked after | 13s |
| Bookings | 5,946 (9,880 of 10,000 seats) |
| Payments declined, then retried | 330 |
| Paid after the hold expired | 226 (45 still booked: nobody had taken their seats; 214 refunded) |
| Webhooks | 6,709 events, 640 sent twice, 140 never sent (the reconciler found them) |
| Email requests deduplicated by Relay | 0 |
| Users who found no free seats left | 2,735 |
| Database transactions during the load | 94,127 |
| Deadlocks Postgres had to break | 0 |
| Time to settle after the load | 1.0s |
| k6 requests (dropped: k6 couldn't start in time) | 121,420 (dropped 6,206) |

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
